// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"
	"log"

	pb "github.com/ls-2018/etcd_cn/raft/raftpb"
)

// 快照 + storage + unstable
//
type raftLog struct {
	// storage 存储已经持久化到 WAL 中的日志条目,unstable 存储未持久化的条目和快照,一旦持久化会及时删除日志条目,因此不存在过多内存占用的问题.
	storage   Storage  // 最后存储数据	// 这里还是一个内存存储,用于保存自从最后一次snapshot之后提交的数据
	unstable  unstable // 快照之后的数据	// 用于存储未写入Storage的快照数据及Entry记录
	committed uint64   // 己提交的位置,即己提交的Entry记录中最大的索引值.
	// 而applied保存的是传入状态机中的最高index
	// 即一条日志首先要提交成功(即committed),才能被applied到状态机中;因此以下不等式一直成立:applied <= committed
	applied         uint64
	logger          Logger
	maxNextEntsSize uint64 // 调用 nextEnts 时,返回的日志项集合的最大的大小 返回应用程序已经可以应用到状态机的日志项集合
}

// 追加日志.
func (l *raftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	// 日志与部分提交日志重叠,这种属于不能接受的情况
	if after := ents[0].Index - 1; after < l.committed {
		l.logger.Panicf("after(%d) 超出范围[committed(%d)]", after, l.committed)
	}
	l.unstable.truncateAndAppend(ents)
	return l.lastIndex()
}

// findConflict 对每一个日志查找冲突
func (l *raftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.lastIndex() {
				l.logger.Infof("发现索引冲突[任期不一致] %d [existing term: %d, conflicting term: %d]", ne.Index, l.zeroTermOnErrCompacted(l.term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

// 从此索引开始,查找第一个任期小于LogTerm的日志索引
func (l *raftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	// case 1  follower   	index:6 本地存储的      term:9leader认为的				返回 6
	//   idx        1 2 3 4 5 6 7 8 9 10 11 12
	//              -------------------------
	//   term (L)   1 3 3 3 5 5 5 5 5
	//   term (F)   1 1 1 1 2 2
	// case 2  follower   	index:12 本地存储的     term:9leader认为的				返回 12
	//   idx        1 2 3 4 5 6 7 8 9 10 11 12
	//              -------------------------
	//   term (L)   1 3 3 3 5 5 5 5 5
	//   term (F)   1 1 1 1 2 2 2 2 2  2  2  2
	// case 3  leader   	index:6 本地存储的      term:2follower的				返回 1
	//   idx        1 2 3 4 5 6 7 8 9 10 11 12
	//              -------------------------
	//   term (L)   1 3 3 3 5 5 5 5 5
	//   term (F)   1 1 1 1 2 2
	if li := l.lastIndex(); index > li {
		l.logger.Warningf("index(%d) 超出范围 [0, lastIndex(%d)] in findConflictByTerm", index, li)
		return index
	}
	for {
		logTerm, err := l.term(index) // 2
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	return index
}

// 获取不可靠日志,就是把unstable的所有日志输出,这个函数用于输出给使用者持久化
func (l *raftLog) unstableEntries() []pb.Entry {
	if len(l.unstable.entries) == 0 {
		return nil
	}
	return l.unstable.entries
}

// nextEnts 获取[applied+1: committed+1] 的所有日志
func (l *raftLog) nextEnts() (ents []pb.Entry) {
	off := max(l.applied+1, l.firstIndex())
	if l.committed+1 > off {
		ents, err := l.slice(off, l.committed+1, l.maxNextEntsSize)
		if err != nil {
			l.logger.Panicf("在获取未应用的条目时出现意外错误 (%v)", err)
		}
		return ents
	}
	return nil
}

// hasNextEnts 判断是否有可应用的日志
func (l *raftLog) hasNextEnts() bool {
	off := max(l.applied+1, l.firstIndex())
	return l.committed+1 > off
}

// hasPendingSnapshot 判断是不是正在处理快照
func (l *raftLog) hasPendingSnapshot() bool {
	return l.unstable.snapshot != nil && !IsEmptySnap(*l.unstable.snapshot)
}

// 获取快照
func (l *raftLog) snapshot() (pb.Snapshot, error) {
	if l.unstable.snapshot != nil {
		return *l.unstable.snapshot, nil
	}
	return l.storage.Snapshot()
}

// 获取快照之后的第一个索引
func (l *raftLog) firstIndex() uint64 {
	if i, ok := l.unstable.maybeFirstIndex(); ok {
		// 有快照
		return i
	}
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return index
}

// 获取最新的日志索引
func (l *raftLog) lastIndex() uint64 {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		l.logger.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

// 更新提交索引
func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

// 简单的l.term封装,0或正常的任期
func (l *raftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	l.logger.Panicf("未知的 error (%v)", err)
	return 0
}

// 构建新的日志条目
// 刚刚创建的时候unstable里的snapshot和ents为空
func newLogWithSize(storage Storage, logger Logger, maxNextEntsSize uint64) *raftLog {
	if storage == nil {
		log.Panic("存储不能为空")
	}
	log := &raftLog{ // struct
		storage:         storage, // memory
		logger:          logger,
		maxNextEntsSize: maxNextEntsSize, // 消息最大大小
		unstable:        unstable{},
	}
	// 使用者启动需要把持久化的快照以及日志存储在storage中,前面已经提到了,这个
	// storage类似于使用者持久化存储的cache.

	firstIndex, err := storage.FirstIndex() // 返回第一条数据的索引
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex() // 返回最后一条数据的索引
	if err != nil {
		panic(err)
	}
	// 这个代码印证了前面提到了,当unstable没有不可靠日志的时候,unstable.offset的值就是
	// 未来的第一个不可靠日志的索引.
	log.unstable.offset = lastIndex + 1 // 保存了尚未持久化的日志条目或快照
	log.unstable.logger = logger
	//   -------------------------------------
	//    commit|apply      storage
	log.committed = firstIndex - 1 // 已经提交的了
	log.applied = firstIndex - 1

	return log
}

// OK
func (l *raftLog) String() string {
	return fmt.Sprintf("----> 【committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d】", l.committed, l.applied, l.unstable.offset, len(l.unstable.entries))
}

// slice 获取lo到hi-1的所有日志,但总量限制在maxsize
func (l *raftLog) slice(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	var ents []pb.Entry
	// 日志有一部分落在storage中
	if lo < l.unstable.offset {
		storedEnts, err := l.storage.Entries(lo, min(hi, l.unstable.offset), maxSize)
		if err == ErrCompacted { // 压缩了
			return nil, err
		} else if err == ErrUnavailable {
			l.logger.Panicf("日志[%d:%d] 索引中的请求条目不可用", lo, min(hi, l.unstable.offset))
		} else if err != nil {
			panic(err) // TODO(bdarnell)
		}

		// 检查ents是否达到大小限制
		// 如果从storage获取的日志数量比预期少;说明没那么多日志存在storage中;那也就没必要再找unstable了.
		if uint64(len(storedEnts)) < min(hi, l.unstable.offset)-lo {
			return storedEnts, nil
		}
		ents = storedEnts
	}

	// 日志有一部分在unstable中.
	if hi > l.unstable.offset {
		unstable := l.unstable.slice(max(lo, l.unstable.offset), hi)
		if len(ents) > 0 {
			combined := make([]pb.Entry, len(ents)+len(unstable))
			n := copy(combined, ents)
			copy(combined[n:], unstable)
			ents = combined
		} else {
			ents = unstable
		}
	}
	return limitSize(ents, maxSize), nil
}

// 范围检查 l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Panicf("无效的索引 %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}

	length := l.lastIndex() + 1 - fi
	if hi > fi+length {
		l.logger.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.lastIndex())
	}
	return nil
}

// 查看索引消息对应的任期
func (l *raftLog) term(i uint64) (uint64, error) {
	// 如果索引在raftLog记录的所有日志之外,那么只能返回0代表没找到
	// [快照之后的第一个索引, unstable最新索引]
	dummyIndex := l.firstIndex() - 1
	if i < dummyIndex || i > l.lastIndex() {
		return 0, nil
	}
	// 在unstable中找一下
	if t, ok := l.unstable.maybeTerm(i); ok {
		return t, nil
	}
	// unstable中没有那就在storage找
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	// 如果storage和unstable都没有,那也算是没找到.这是因为storage可能会压缩,比如把应用
	// 索引以前的日志删除,因为他们已经没用了,这样可以节省内存空间.
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)
}

// isUpToDate Follower节点在接收到Candidate节点的选举请求之后,会通过比较Candidate节点的本地日志与自身本地日志的新旧程度,从而决定是否投票.
// raftLog提供了isUpToDat巳()方法用于比较日志的新旧程度.
func (l *raftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

// 检测MsgApp消息的Index 字段及LogTerm字段是否合法
func (l *raftLog) matchTerm(i, term uint64) bool {
	t, err := l.term(i) // 查看索引消息对应的任期
	if err != nil {
		return false
	}
	return t == term
}

// 将日志commit到tocommit
func (l *raftLog) commitTo(tocommit uint64) {
	if l.committed < tocommit {
		if l.lastIndex() < tocommit {
			l.logger.Panicf("tocommit(%d)超过了 [lastIndex(%d)] raft log是否被损坏、截断或丢失?.?", tocommit, l.lastIndex())
		}
		l.committed = tocommit
	}
}

func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		l.logger.Panicf("applied(%d) 不再范围内[prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

// 获取从索引值为i之后的所有日志,但是日志总量限制在maxsize
func (l *raftLog) entries(i, maxsize uint64) ([]pb.Entry, error) {
	if i > l.lastIndex() {
		return nil, nil
	}
	return l.slice(i, l.lastIndex()+1, maxsize)
}

// ------------------------------------------	entrance	----------------------------------------

// maybeAppend 当Follower节点或Candidate节点需要向raftLog 中追加Entry记录时,会通过raft.handleAppendEntriesO方法调用raftLog.maybeAppend
// m.Index: ents[0].Index, m.LogTerm: ents[0].Term, m.Commit:leader记录的本机点已经commit的日志索引
// m.Entries... 真正的日志数据
func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) { // 查看 index 的 term 与 logTerm 是否匹配·
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents) // 查找ents中,index与term 冲突的位置.
		switch {
		case ci == 0: // 没有冲突
		case ci <= l.committed: // 如果冲突的位置在已提交的位置之前,有问题
			l.logger.Panicf("日志 %d 与已承诺的条目冲突  [committed(%d)]", ci, l.committed)
		default: // 如果冲突位置是未提交的部分
			// [1,2] ----> [1,3,4]
			// 本节点存在一些无效的数据,比leader多
			offset := index + 1
			// 则将ents中未发生冲突的部分追加到raftLog中
			// etcd 深入解析 图1-11 f
			l.append(ents[ci-offset:]...) // 追加到unstable
		}
		l.commitTo(min(committed, lastnewi)) // committed:leader端发送过来的,认为本节点已committed的索引
		return lastnewi, true
	}
	return 0, false
}

// 收到快照
func (l *raftLog) restore(s pb.Snapshot) {
	l.logger.Infof("log [%s] 开始恢复快照 [index: %d, term: %d]", l, s.Metadata.Index, s.Metadata.Term)
	l.committed = s.Metadata.Index
	l.unstable.restore(s)
}
