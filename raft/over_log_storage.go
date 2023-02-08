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
	"errors"
	"sync"

	pb "github.com/ls-2018/etcd_cn/raft/raftpb"
)

var ErrCompacted = errors.New("由于压缩,请求的索引无法到达")

var ErrSnapOutOfDate = errors.New("请求的索引比现有快照的老")

var ErrUnavailable = errors.New("索引中的请求条目不可用")

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

// Storage raft状态机
type Storage interface {
	// InitialState 使用者在构造raft时,需要传入初始状态,这些状态存储在可靠存储中,使用者需要通过Storage
	// 告知raft.关于状态的定义不在本文导论范围,笔者会在其他文章中详细说明.
	InitialState() (pb.HardState, pb.ConfState, error)
	// Entries 获取索引在[lo,hi)之间的日志,日志总量限制在maxSize
	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)
	// Term 获取日志索引为i的届.找不到的情况下error返回值不为空,其中当返回ErrCompacted表示传入的索引数据已经找不到,
	// 说明已经被压缩成快照数据了;返回ErrUnavailable:表示传入的索引值大于当前的最大索引.
	Term(index uint64) (uint64, error)
	LastIndex() (uint64, error)     // 返回最后一条日志的索引
	FirstIndex() (uint64, error)    // 返回第一条日志的索引
	Snapshot() (pb.Snapshot, error) // 反回最近的快照数据
}

// MemoryStorage 大部分操作都需要加锁
type MemoryStorage struct {
	sync.Mutex
	hardState pb.HardState // 状态信息(当前任期,当前节点投票给了谁,已提交的entry记录的位置)
	snapshot  pb.Snapshot  // 当前内存里的快照信息
	// wal日志 内存存储
	ents []pb.Entry // snapshot之后的日志条目,第一条日志条目的index为snapshot.Metadata.Index 已经apply的日志项
}

// NewMemoryStorage 创建内存存储
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		// 当从头开始时,用一个假的条目来填充列表中的第零项.
		ents: make([]pb.Entry, 1),
	}
}

// Entries 获取一定范围内的日志项
func (ms *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 {
		getLogger().Panicf("日志 hi(%d)超出范围的最后一个索引(%d)", hi, ms.lastIndex())
	}
	// only contains dummy entries.
	if len(ms.ents) == 1 {
		return nil, ErrUnavailable
	}
	// 获取下标的数据
	ents := ms.ents[lo-offset : hi-offset]
	// limitsize把超过大小的数据剔除
	return limitSize(ents, maxSize), nil
}

// Term 获取指定索引日志的任期
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// ApplySnapshot 更新快照数据,将snapshot实例保存到memorystorage中
func (ms *MemoryStorage) ApplySnapshot(snap pb.Snapshot) error {
	ms.Lock()
	defer ms.Unlock()

	msIndex := ms.snapshot.Metadata.Index // 内存的
	snapIndex := snap.Metadata.Index      // 文件系统的
	if msIndex >= snapIndex {
		return ErrSnapOutOfDate
	}

	ms.snapshot = snap
	ms.ents = []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

// CreateSnapshot 创建新的快照 i是新建Snapshot包含的最大的索引值,cs是当前集群的状态,data是状态机里的快照数据
// 更新快照信息
func (ms *MemoryStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	// ents  [a,b,c,d,e,f,g,h,i,j,k]
	//                  i
	// 前提条件: i 必须>= a日志的索引
	ms.Lock()
	defer ms.Unlock()
	// 新建立的快照是旧数据
	if i <= ms.snapshot.Metadata.Index {
		return pb.Snapshot{}, ErrSnapOutOfDate
	}

	offset := ms.ents[0].Index // snapshot之后的日志条目, apply之后的
	if i > ms.lastIndex() {    // k 位置的索引
		getLogger().Panicf("快照 %d 超过了最新的日志(%d)", i, ms.lastIndex())
	}

	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term // 获取数组指定偏移位置的索引
	if cs != nil {
		ms.snapshot.Metadata.ConfState = *cs
	}
	ms.snapshot.Data = data
	return ms.snapshot, nil
}

// InitialState OK
func (ms *MemoryStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return ms.hardState, ms.snapshot.Metadata.ConfState, nil
}

// SetHardState OK
func (ms *MemoryStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Snapshot OK
func (ms *MemoryStorage) Snapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

// LastIndex todo 返回ents最新entry的索引
func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

// todo 返回ents最新entry的索引
func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

// FirstIndex todo
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

// 第一条日志索引,默认ents里有一个索引为0的EntryNormal
func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

// Compact  新建Snapshot之后,一般会调用MemoryStorage.Compact()方法将MemoryStorage.ents中指定索引之前的Entry记录全部抛弃,
// 从而实现压缩MemoryStorage.ents 的目的,具体实现如下:    [GC]
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index // ents记录中第一条日志的索引
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() { // ents记录中最新日志的索引
		getLogger().Panicf("压缩 %d 超出范围 lastindex(%d)", compactIndex, ms.lastIndex())
	}

	i := compactIndex - offset
	// 创建新的切片,用来存储compactIndex之后的Entry
	ents := make([]pb.Entry, 1, 1+uint64(len(ms.ents))-i)
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	// 将compactlndex之后的Entry拷贝到ents中,并更新MemoryStorage.ents 字段
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	// 那么随着写请求增多,内存中保留的 Raft 日志条目会越来越多,如何防止 etcd 出现 OOM 呢？etcd 提供了快照和压缩功能来解决这个问题.
	// 首先你可以通过调整 --snapshot-count 参数来控制生成快照的频率,其值默认是 100000（etcd v3.4.9,早期 etcd 版本是 10000）,
	// 也就是每 10 万个写请求触发一次快照生成操作.快照生成完之后,etcd 会通过压缩来删除旧的日志条目.那么是全部删除日志条目还是保留一小部分呢？
	// 答案是保留一小部分 Raft 日志条目.数量由 DefaultSnapshotCatchUpEntries 参数控制,默认 5000,目前不支持自定义配置.
	// 保留一小部分日志条目其实是为了帮助慢的 Follower 以较低的开销向 Leader 获取 Raft 日志条目,以尽快追上 Leader 进度.
	// 若 raftLog 中不保留任何日志条目,就只能发送快照给慢的 Follower,这开销就非常大了.
	return nil
}

// Append 向快照添加数据
// 确保日志项是 连续的且entries[0].Index > ms.entries[0].Index
func (ms *MemoryStorage) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	first := ms.firstIndex()                            // 当前第一个
	last := entries[0].Index + uint64(len(entries)) - 1 // 最后一个

	if last < first {
		return nil // entries切片中所有的Entry都已经过时,无须添加任何Entry
	}
	// first之前的Entry已经记入Snapshot中,不应该再记录到ents中,所以将这部分Entry截掉
	// entries[0]    first   entries[-1]
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	// 计算entries切片中第一条可用的Entry与first之间的差距
	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		// 保留MemoryStorage.ents中first～offset的部分,offset之后的部分被抛弃
		// 然后将待追加的Entry追加到MemoryStorage.ents中
		ms.ents = append([]pb.Entry{}, ms.ents[:offset]...)
		ms.ents = append(ms.ents, entries...)
	case uint64(len(ms.ents)) == offset:
		// 直接将待追加的日志记录(entries)追加到MemoryStorage中
		ms.ents = append(ms.ents, entries...)
	default:
		getLogger().Panicf("丢失日志项 [last: %d, append at: %d]", ms.lastIndex(), entries[0].Index)
	}
	return nil
}
