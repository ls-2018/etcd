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
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ls-2018/etcd_cn/raft/quorum"

	"github.com/ls-2018/etcd_cn/raft/confchange"
	pb "github.com/ls-2018/etcd_cn/raft/raftpb"
	"github.com/ls-2018/etcd_cn/raft/tracker"
)

// None 是一个占位的节点ID,在没有leader时使用.
const None uint64 = 0
const noLimit = math.MaxUint64

// 状态类型
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	StatePreCandidate
)

type ReadOnlyOption int

const (
	ReadOnlySafe ReadOnlyOption = iota
	ReadOnlyLeaseBased
	// 1、 ReadOnlySafe
	//	该线性读模式,每次 Follower 进行读请求时,需要和 Leader 同步日志提交位点信息,而 Leader需要向过半的 Follower 发起证明自己是 Leader 的轻量的 RPC 请求,
	//	相当于一个 Follower 读,至少需要 1 +(n/2)+ 1 次的 RPC 请求.
	// 2、ReadOnlyLeaseBased
	// 该线性读模式,每次 Follower 进行读请求时, Leader 只需要判断自己的 Leader 租约是否过期了,如果没有过期,直接可以回复 Follower 自己是 Leader
	// 但是该机制对于机器时钟要求很严格,如果有做时钟同步的话,可以考虑使用该线性读模式.
	// 如果说对于配置的发布、修改操作比较频繁,可以将 Raft 快照的时间适当的进行调整,避免新节点加入或者节点重启时,由于 Raft 日志回放操作数太多导致节点可开始对外服务的时间过长.
)

const (
	campaignPreElection CampaignType = "CampaignPreElection" // 竞选类型: pre-vote模式
	campaignElection    CampaignType = "CampaignElection"    // 竞选类型:vote模式
	campaignTransfer    CampaignType = "CampaignTransfer"    // 竞选类型:leader开始转移
)

// ErrProposalDropped 在某些情况下提案被忽略时返回,以便提案者可以得到通知并快速失败.
var ErrProposalDropped = errors.New("撤销raft提案")

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// CampaignType 竞选类型
type CampaignType string

// 封装了当前节点所有的核心数据.
type raft struct {
	id                        uint64                  // 是本节点raft的身份
	Term                      uint64                  // 任期.如果Message的Term字段为0,则表示该消息是本地消息,例如,MsgHup、 MsgProp、 MsgReadlndex 等消息,都属于本地消息.
	Vote                      uint64                  // 当前任期中当前节点将选票投给了哪个节点
	raftLog                   *raftLog                // 当前节点的log状态信息
	maxMsgSize                uint64                  // 每次发送消息的最大大小[多条日志]
	maxUncommittedSize        uint64                  // 每条日志最大消息体
	prstrack                  tracker.ProgressTracker // 跟踪Follower节点的状态,比如日志复制的matchIndex
	state                     StateType               // 当前节点的状态可选值分为StateFollower、StateCandidate、 StateLeader和StatePreCandidat巳四种状态.
	isLearner                 bool                    // 本节点是不是learner角色
	msgs                      []pb.Message            // 缓存了当前节点等待发送的消息.
	lead                      uint64                  // 当前leaderID
	leadTransferee            uint64                  // leader转移到的节点ID
	pendingConfIndex          uint64                  // 记录配置变更日志的索引,可能该索引已经被apply
	uncommittedSize           uint64                  // 还未提交的日志s的大小,非准确值
	checkQuorum               bool                    // 检查需要维持的选票数,一旦小于,就会丢失leader
	preVote                   bool                    // PreVote 是否启用PreVote
	electionElapsed           int                     // 选举计时器的指针,其单位是逻辑时钟的刻度,逻辑时钟每推进一次,该字段值就会增加1.
	heartbeatElapsed          int                     // 心跳计时器的指针,其单位也是逻辑时钟的刻度,逻辑时钟每推进一次,该字段值就会增加1 .
	heartbeatTimeout          int                     // 心跳间隔   上限     heartbeatTimeout是当前距离上次心跳的时间
	electionTimeout           int                     // 选举超时时间,当electionE!apsed 宇段值到达该值时,就会触发新一轮的选举.
	randomizedElectionTimeout int                     // 随机选举超时
	disableProposalForwarding bool                    // 禁止将请求转发到leader,默认FALSE
	tick                      func()                  // 逻辑计数器推进函数, 由 r.ticker = time.NewTicker(r.heartbeat) ;触发该函数的执行  r.start
	step                      stepFunc                // 阶段函数、在那个角色就执行那个角色的函数、处理接收到的消息
	logger                    Logger
	// pendingReadIndexMessages is used to store messages of type MsgReadIndex
	// that can't be answered as new leader didn't committed any log in
	// current term. Those will be handled as fast as first log is committed in
	// current term.
	pendingReadIndexMessages []pb.Message
	readStates               []ReadState // leader会直接往这里存储； follower 转发MsgReadIndex至leader 的响应
	readOnly                 *readOnly
}

// 通知RawNode 应用程序已经应用并保存了最后一个Ready结果的进度.
func (r *raft) advance(rd Ready) {
	// 此时这些数据,应用到了wal,与应用程序状态机
	r.reduceUncommittedSize(rd.CommittedEntries) // 日志committed以后应该从这里扣除

	// 如果应用了条目(或快照),则将游标更新为下一个Ready.请注意,如果当前的HardState包含一个新的Commit索引,
	// 这并不意味着我们也应用了所有由于按大小提交分页而产生的新条目.
	if newApplied := rd.appliedCursor(); newApplied > 0 {
		oldApplied := r.raftLog.applied
		r.raftLog.appliedTo(newApplied)

		if r.prstrack.Config.AutoLeave && oldApplied <= r.pendingConfIndex && newApplied >= r.pendingConfIndex && r.state == StateLeader {
			// 如果当前配置(和最近的配置,至少在这个领导人的任期内)应该是自动离开的,现在启动它.我们使用一个空的数据,
			// 它分解成一个空的ConfChangeV2,并且有一个好处,即appendEntry永远不会根据它的大小(寄存器为零)拒绝它.
			ent := pb.Entry{
				Type: pb.EntryConfChangeV2,
				Data: nil,
			}
			// 这个建议是不可能被拒绝的.
			if !r.appendEntry(ent) {
				panic("拒绝不可拒绝的 auto-leaving ConfChangeV2")
			}
			r.pendingConfIndex = r.raftLog.lastIndex()
			r.logger.Infof("启动自动过渡,脱离joint配置 %s", r.prstrack.Config)
		}
	}
	// 让unstable 更新数据
	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		r.raftLog.unstable.stableTo(e.Index, e.Term)
	}
	// 更新快照数据
	if !IsEmptySnap(rd.Snapshot) {
		r.raftLog.unstable.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
}

// 检测是否有未应用的EntryConfChange记录
func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].Type == pb.EntryConfChange || ents[i].Type == pb.EntryConfChangeV2 {
			n++
		}
	}
	return n
}

// 判断本节点是不是重新选举,因为丢失了leader
func (r *raft) pastElectionTimeout() bool {
	// 选举过期计数(electionElapsed):主要用于follower来判断leader是不是正常工作,
	// 当follower接受到leader的心跳的时候会把electionElapsed的时候就会置为0,electionElapsed的相加是通过外部调用实现的,
	// node对外提供一个tick的接口,需要外部定时去调用,调用的周期由外部决定,每次调用就++,
	// 然后检查是否会超时,上方的tickElection就是为follower状态的定时调用函数,leader状态的定时调用函数就是向follower发送心跳.
	// 计时次数 超过了 限定的 选举次数,   规定:在randomizedElectionTimeout次数内必须收到来自leader的消息
	return r.electionElapsed >= r.randomizedElectionTimeout
}

// 设置随机选举超时
func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout) // 随机选举超时
}

// OK
func (r *raft) softState() *SoftState { return &SoftState{Lead: r.lead, RaftState: r.state} }

// OK
func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote, // 给谁投了票
		Commit: r.raftLog.committed,
	}
}

// send 将状态持久化到一个稳定的存储中,之后再发送消息
func (r *raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	// 数据校验,选举类消息必须带term属性
	// 竞选投票相关的消息类型,必须设置term
	if m.Type == pb.MsgVote || m.Type == pb.MsgVoteResp || m.Type == pb.MsgPreVote || m.Type == pb.MsgPreVoteResp {
		if m.Term == 0 {
			panic(fmt.Sprintf("任期应该被设置%s", m.Type))
		}
	} else {
		// 其它类消息不能带term属性
		if m.Term != 0 {
			panic(fmt.Sprintf("任期不能被设置,当 %s (was %d)", m.Type, m.Term))
		}
		// 除了MsgProp和MsgReadIndex消息外,设置term为raft当前周期
		if m.Type != pb.MsgProp && m.Type != pb.MsgReadIndex {
			m.Term = r.Term
		}
	}

	r.msgs = append(r.msgs, m) // 将消息放入队列 写
}

// StateType 节点在集群中的状态
type StateType uint64

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// Config 启动raft的配置参数
type Config struct {
	ID                        uint64         // ID 是本节点raft的身份.ID不能为0.
	ElectionTick              int            // 返回选举权检查对应多少次tick触发次数
	HeartbeatTick             int            // 返回心跳检查对应多少次tick触发次数
	Storage                   Storage        // Storage 存储 日志项、状态
	Applied                   uint64         // 提交到用户状态机的索引,起始为0
	MaxSizePerMsg             uint64         // 每条消息的最大大小:math.MaxUint64表示无限制,0表示每条消息最多一个条目.  1M
	MaxCommittedSizePerReady  uint64         // 限制  commited --> apply 之间的数量 MaxSizePerMsg 它们之前是同一个参数
	MaxUncommittedEntriesSize uint64         // 未提交的日志项上限
	MaxInflightMsgs           int            // 最大的处理中的消息数量
	CheckQuorum               bool           // CheckQuorum 检查需要维持的选票数,一旦小于,就会丢失leader
	PreVote                   bool           // PreVote 防止分区服务器[term会很大]重新加入集群时出现中断   是否启用PreVote
	ReadOnlyOption            ReadOnlyOption // 必须是enabled if ReadOnlyOption is ReadOnlyLeaseBased.
	DisableProposalForwarding bool           // 禁止将请求转发到leader,默认FALSE
	Logger                    Logger
}

// OK
func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("补鞥呢使用None作为ID")
	}
	// 返回心跳检查对应多少次tick触发次数
	if c.HeartbeatTick <= 0 { //
		return errors.New("心跳间隔必须是>0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("选举超时必须是大于心跳间隔")
	}

	if c.Storage == nil {
		return errors.New("不能没有存储")
	}

	if c.MaxUncommittedEntriesSize == 0 {
		c.MaxUncommittedEntriesSize = noLimit
	}

	//  它们之前是同一个参数.
	if c.MaxCommittedSizePerReady == 0 {
		c.MaxCommittedSizePerReady = c.MaxSizePerMsg
	}

	if c.MaxInflightMsgs <= 0 {
		return errors.New("max inflight messages必须是>0")
	}

	if c.Logger == nil {
		c.Logger = getLogger()
	}
	// 作为leader时的检查
	if c.ReadOnlyOption == ReadOnlyLeaseBased && !c.CheckQuorum {
		return errors.New("如果ReadOnlyOption 是 ReadOnlyLeaseBased 的时候必须开启CheckQuorum")
	}

	return nil
}

// ok
func newRaft(c *Config) *raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftlog := newLogWithSize(c.Storage, c.Logger, c.MaxCommittedSizePerReady) // ✅
	// 搜 s = raft.NewMemoryStorage()
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	r := &raft{
		id:                        c.ID,                        // 是本节点raft的身份
		lead:                      None,                        // 当前leaderID
		isLearner:                 false,                       // 本节点是不是learner角色
		raftLog:                   raftlog,                     // 当前节点的log状态信息
		maxMsgSize:                c.MaxSizePerMsg,             // 每条消息的最大大小
		maxUncommittedSize:        c.MaxUncommittedEntriesSize, // 每条日志最大消息体
		prstrack:                  tracker.MakeProgressTracker(c.MaxInflightMsgs),
		electionTimeout:           c.ElectionTick,  // 返回选举权检查对应多少次tick触发次数
		heartbeatTimeout:          c.HeartbeatTick, // 返回心跳检查对应多少次tick触发次数
		logger:                    c.Logger,
		checkQuorum:               c.CheckQuorum,                 // 检查需要维持的选票数,一旦小于,就会丢失leader
		preVote:                   c.PreVote,                     // PreVote 是否启用PreVote
		readOnly:                  newReadOnly(c.ReadOnlyOption), // etcd/etcdserver/over_raft.go:469    默认值0 ReadOnlySafe
		disableProposalForwarding: c.DisableProposalForwarding,   // 禁止将请求转发到leader,默认FALSE
	}
	// todo 没看懂
	// -----------------------
	cfg, prs, err := confchange.Restore(confchange.Changer{
		Tracker:   r.prstrack,
		LastIndex: raftlog.lastIndex(),
	}, cs)
	if err != nil {
		panic(err)
	}
	assertConfStatesEquivalent(r.logger, cs, r.switchToConfig(cfg, prs)) // 判断相不相等
	// -----------------------
	// 根据从Storage中获取的HardState,初始化raftLog.committed字段,以及raft.Term和Vote字段
	if !IsEmptyHardState(hs) { // 判断初始状态是不是空的
		r.loadState(hs) // 更新状态索引信息
	}
	// 如采Config中己置了Applied,则将raftLog.applied字段重直为指定的Applied值上层模块自己的控制正确的己应用位置时使用该配置
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied) // ✅
	}
	r.becomeFollower(r.Term, None) // ✅ start

	var nodesStrs []string
	for _, n := range r.prstrack.VoterNodes() { // 一开始没有
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	r.logger.Infof("【newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]】",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	return r
}

func (r *raft) hasLeader() bool { return r.lead != None }

// 向指定的节点发送信息
func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	commit := min(r.prstrack.Progress[to].Match, r.raftLog.committed)
	m := pb.Message{
		To:      to,
		Type:    pb.MsgHeartbeat,
		Commit:  commit, // leader会为每个Follower都维护一个leaderCommit,表示leader认为Follower已经提交的日志条目索引值
		Context: ctx,
	}
	r.send(m)
}

// bcastAppend 向集群中其他节点广播MsgApp消息
func (r *raft) bcastAppend() {
	// 遍历所有节点,给除自己外的节点发送日志Append消息
	r.prstrack.Visit(func(id uint64, _ *tracker.Progress) {
		if id == r.id {
			return
		}
		r.sendAppend(id)
	})
}

// OK 向集群中特定节点发送MsgApp消息
func (r *raft) sendAppend(to uint64) {
	r.maybeSendAppend(to, true)
}

// maybeSendAppend 向给定的peer发送一个带有新条目的追加RPC.如果有消息被发送,返回true.
// sendIfEmpty参数控制是否发送没有条目的消息("空 "消息对于传达更新的Commit索引很有用,但当我们批量发送多条消息时就不可取).
func (r *raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	// 在消息发送之前会检测当前节点的状态,然后查找待发迭的Entry记录并封装成MsgApp消息,
	// 之后根据对应节点的Progress.State值决定发送消息之后的操作

	// 1. 获取对端节点当前同步进度
	pr := r.prstrack.Progress[to]
	if pr.IsPaused() {
		return false
	}
	m := pb.Message{}
	m.To = to
	// 2. 注意这里带的term是本次发送给follower的第一条日志条目的term
	term, errt := r.raftLog.term(pr.Next - 1)              // leader认为 follower所在的任期
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize) // 要发给follower的日志
	if len(ents) == 0 && !sendIfEmpty {
		// 这种情况就不发了
		return false
	}

	if errt != nil || erre != nil {
		// 3. 如果获取term或日志失败,说明follower落后太多,raftLog内存中日志已经做过快照后被删除了
		// 根据日志进度去取日志条目的时候发现,follower日志落后太多,这通常出现在新节点刚加入或者网络连接出现故障的情况下.
		// 那么在这种情况下,leader改为发送最近一次快照给Follower,从而提高同步效率

		if !pr.RecentActive {
			r.logger.Debugf("忽略向%x发送快照,因为它最近没有活动.", to)
			return false
		}
		// 4. 改为发送Snapshot消息
		m.Type = pb.MsgSnap
		snapshot, err := r.raftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x 由于快照暂时不可用,未能向%x发送快照.", r.id, to)
				return false
			}
			panic(err)
		}
		if IsEmptySnap(snapshot) {
			panic("需要一个非空快照")
		}
		m.Snapshot = snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		r.logger.Debugf("%x [firstindex: %d, commit: %d] 发送快照[index: %d, term: %d] to %x [%s]", r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
		pr.BecomeSnapshot(sindex) // 变成发送快照的状态
		r.logger.Debugf("%x 暂停发送复制信息到 %x [%s]", r.id, to, pr)
	} else {
		// 5. 发送Append消息
		m.Type = pb.MsgApp             // 设置消息类型
		m.Index = pr.Next - 1          // 设置MsgApp消息的Index字段
		m.LogTerm = term               // 设置MsgApp消息的LogTerm字段
		m.Entries = ents               // 设置消息携带的Entry记录集合
		m.Commit = r.raftLog.committed // 设置消息的Commit字段,即当前节点的raftLog中最后一条已提交的记录索引值
		// 6. 每次发送日志或心跳都会带上最新的commitIndex
		m.Commit = r.raftLog.committed
		if n := len(m.Entries); n != 0 {
			switch pr.State {
			// 在StateReplicate中,乐观地增加
			case tracker.StateReplicate:
				last := m.Entries[n-1].Index
				pr.OptimisticUpdate(last) // 新目标节点对应的Next值(这里不会更新Match)
				pr.Inflights.Add(last)    // 记录已发送但是未收到响应的消息
			case tracker.StateProbe:
				// 消息发送后,就将Progress.Paused字段设置成true,暂停后续消息的发送
				pr.ProbeSent = true
			default:
				r.logger.Panicf("%x 在未知的状态下发送%s", r.id, pr.State)
			}
		}
	}
	// 7. 发送消息
	r.send(m)
	return true
}

// 取消 先前的领导权移交
func (r *raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

// tickHeartbeat 只在leader执行,在r.heartbeatTimeout之后发送一个MsgBeat.
func (r *raft) tickHeartbeat() {
	// 每次tick计时器触发,会调用这个函数
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout { // 如果选举计时超时
		r.electionElapsed = 0 // 重置计时器
		if r.checkQuorum {    // 给自己发送一条 MsgCheckQuorum 消息,检测是否出现网络隔离
			r.Step(pb.Message{From: r.id, Type: pb.MsgCheckQuorum})
		}
		// leader状态下; 重置leadTransferee
		if r.state == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}

	if r.state != StateLeader {
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: r.id, Type: pb.MsgBeat})
	}
}

//   所提交的state 必须在 [r.raftLog.committed,r.raftLog.lastIndex()]之间
func (r *raft) loadState(state pb.HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		r.logger.Panicf("%x state.commit %d 不再指定范围内 [%d, %d]", r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote // 当前节点的选票投给了谁做我Leader
}

// maybeCommit 尝试更新本机committed索引
func (r *raft) maybeCommit() bool {
	// 在所有的follower中 获取最大的公共的超过半数确认的index
	mci := r.prstrack.Committed()
	// 更新本机commitIndex
	return r.raftLog.maybeCommit(mci, r.Term)
}

// 角色变更时,都会调用此函数
func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None // 当前任期中当前节点将选票投给了哪个节点
	}
	r.lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout() // 设置随机选举超时
	r.abortLeaderTransfer()            // 置空 leader转移目标

	r.prstrack.ResetVotes() // 将每个节点的记录信息  重置
	// 重直prs, 其中每个Progress中的Next设置为raftLog.lastindex
	r.prstrack.Visit(func(id uint64, pr *tracker.Progress) {
		*pr = tracker.Progress{
			Match:     0,
			Next:      r.raftLog.lastIndex() + 1,
			Inflights: tracker.NewInflights(r.prstrack.MaxInflight),
			IsLearner: pr.IsLearner,
		}
		if id == r.id {
			pr.Match = r.raftLog.lastIndex() // 对应Follower节点当前己经成功复制的Entry记录的索引值,不知有没有同步大多数节点
		}
	})

	r.pendingConfIndex = 0
	r.uncommittedSize = 0
	r.readOnly = newReadOnly(r.readOnly.option) // 只读请求的相关摄者
}

// 通过减少记录未提交的条目大小   来处理新提交的条目.
func (r *raft) reduceUncommittedSize(ents []pb.Entry) {
	// 日志committed以后应该从这里扣除
	if r.uncommittedSize == 0 {
		// follower的快速路径,他们不跟踪或执行限制.
		return
	}

	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(e))
	}
	if s > r.uncommittedSize {
		// uncommittedSize可能会低估未提交的Raft日志的大小,但永远不会高估它.
		r.uncommittedSize = 0
	} else {
		r.uncommittedSize -= s
	}
}

// 判断未提交的日志条目是不是超过限制,是的话拒绝并返回失败
func (r *raft) increaseUncommittedSize(ents []pb.Entry) bool {
	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(e))
	}

	if r.uncommittedSize > 0 && s > 0 && r.uncommittedSize+s > r.maxUncommittedSize {
		return false
	}
	r.uncommittedSize += s
	return true
}

// 日志新增, 加日志放入unstable 没有落盘
func (r *raft) appendEntry(es ...pb.Entry) (accepted bool) {
	// 1. 获取raft节点当前最后一条日志条目的index
	li := r.raftLog.lastIndex()
	// 2. 给新的日志条目设置term和index
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	// 3. 判断未提交的日志条目是不是超过限制,是的话拒绝并返回失败
	// etcd限制了leader上最多有多少未提交的条目,防止因为leader和follower之间出现网络问题时,导致条目一直累积.
	if !r.increaseUncommittedSize(es) {
		r.logger.Debugf("%x 在日志中添加新条目将超过未提交的条目大小限制;下降的建议", r.id)
		return false
	}
	// 4. 将日志条目追加到raftLog中
	// 将日志条目追加到raftLog内存队列中,并且返回最大一条日志的index,对于leader追加日志的情况,这里返回的li肯定等于方法第1行中获取的li
	li = r.raftLog.append(es...)
	// 5. 检查并更新日志进度
	// raft的leader节点保存了所有节点的日志同步进度,这里面也包括它自己
	r.prstrack.Progress[r.id].MaybeUpdate(li)
	// 6. 判断是否做一次commit
	r.maybeCommit()
	return true
}

// 开启竞选条件判断
func (r *raft) hup(t CampaignType) {
	if r.state == StateLeader {
		r.logger.Debugf("%x忽略MsgHup消息,因为已经是leader了", r.id)
		return
	}

	if !r.roleUp() {
		r.logger.Warningf("%x角色不可以提升,不能参与竞选", r.id)
		return
	}

	// 获取raftLog中已提交但未apply( lip applied～committed) 的Entry记录
	ents, err := r.raftLog.slice(r.raftLog.applied+1, r.raftLog.committed+1, noLimit)
	if err != nil {
		r.logger.Panicf("获取没有apply日志时出现错误(%v)", err)
	}

	// 检测是否有未应用的EntryConfChange记录,如果有就放弃发起选举的机会
	if n := numOfPendingConf(ents); n != 0 && r.raftLog.committed > r.raftLog.applied {
		r.logger.Warningf("%x不能参与竞选在任期 %d 因为还有 %d 应用配置要更改 ", r.id, r.Term, n)
		return
	}
	// 核对完成,开始选举
	r.logger.Infof("%x开启新的任期在任期%d", r.id, r.Term)
	r.campaign(t)
}

// campaign 开始竞选
func (r *raft) campaign(t CampaignType) { // CampaignTransfer
	if !r.roleUp() {
		r.logger.Warningf("%x is 无法推动;不应该调用 campaign()", r.id)
	}
	var term uint64
	var voteMsg pb.MessageType
	if t == campaignPreElection { // pre-vote模式
		r.becomePreCandidate() // 变成预竞选者角色,更新状态、step、 但不增加任期
		voteMsg = pb.MsgPreVote
		// 在增加r.Term之前,将本节点打算增加到的任期数通过rpc发送出去
		term = r.Term + 1
	} else {
		r.becomeCandidate() // 变成竞选者角色,更新状态、step、任期加1
		voteMsg = pb.MsgVote
		term = r.Term
	}
	// 自己给自己投票
	// pre-vote  那么Votes会置空
	//		单机 : 那么此时给自己投一票,res是VoteWon
	// 		多机:此时是VotePending
	// vote	直接给自己投票
	//		单机 : 那么此时给自己投一票,res是VoteWon
	// 		多机:此时是VotePending
	//  如果是leader转移的话,不会发送vote消息
	_, _, res := r.poll(r.id, voteRespMsgType(voteMsg), true) // 获取自己的竞选结果
	if res == quorum.VoteWon {
		// 我们在为自己投票后赢得了选举(这肯定意味着 这是一个单一的本地节点集群).推进到下一个状态.
		if t == campaignPreElection {
			r.campaign(campaignElection)
		} else {
			r.becomeLeader()
		}
		return
	}
	// 	VotePending VoteLost 两种情况
	//	VoteLost
	var ids []uint64
	// 给节点排序
	{
		idMap := r.prstrack.Voters.IDs()
		ids = make([]uint64, 0, len(idMap))
		for id := range idMap {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}

	for _, id := range ids {
		if id == r.id {
			// 不给自己投票
			continue
		}
		r.logger.Infof("%x [logterm: %d, index: %d] 发送 %s 请求到 %x在任期 %d", r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)

		var ctx []byte
		if t == campaignTransfer { // leader开始转移
			ctx = []byte(t)
		}
		// leader转移 follower 发送申请投票消息,但是任期不会增加, context 是CampaignTransfer
		// 给所有节点发消息
		r.send(pb.Message{Term: term, To: id, Type: voteMsg, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm(), Context: ctx})
		_ = r.Step
	}
}

// 返回  "节点ID" 的投票响应类型,true
func (r *raft) poll(id uint64, t pb.MessageType, v bool) (granted int, rejected int, result quorum.VoteResult) {
	if v {
		r.logger.Infof("%x 收到 %s 从 %x 在任期 %d", r.id, t, id, r.Term)
	} else {
		r.logger.Infof("%x 收到 %s 拒绝消息从 %x 在任期 %d", r.id, t, id, r.Term)
	}
	r.prstrack.RecordVote(id, v)   // 记录投票结果
	return r.prstrack.TallyVotes() // 竞选情况
}

// 处理leader发送来的心跳信息   【follower、Candidate】
func (r *raft) handleHeartbeat(m pb.Message) {
	// 把msg中的commit提交,commit是只增不减的
	r.raftLog.commitTo(m.Commit) // leader commit 了,follower再commit
	// 发送Response给Leader   按照raft协议的要求带上自己日志的进度.
	r.send(pb.Message{To: m.From, Type: pb.MsgHeartbeatResp, Context: m.Context})
}

// 非leader角色的 tick函数, 每次逻辑计时器触发就会调用
func (r *raft) tickElection() {
	r.electionElapsed++ // 收到MsgBeat消息时会重置其选举计时器,从而防止节点发起新一轮选举.
	// roleUp返回是否可以被提升为leader
	// pastElectionTimeout检测当前的候选超时间是否过期
	if r.roleUp() && r.pastElectionTimeout() {
		// 自己可以被promote & election timeout 超时了,规定时间没有听到心跳发起选举;发送MsgHup// 选举超时
		r.electionElapsed = 0                           // 避免两次计时器触发,仍然走这里
		r.Step(pb.Message{From: r.id, Type: pb.MsgHup}) // 让自己选举
	}
}

// 处理日志
func (r *raft) handleAppendEntries(m pb.Message) {
	// 在leader在发消息时,也会将消息写入本地日志文件中,不会等待follower确认
	// 判断是否是过时的消息; 日志索引 小于本地已经commit的消息
	if m.Index < r.raftLog.committed {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
		return
	}
	// 会进行一致性检查;尝试将消息携带的Entry记录追加到raftLog中
	// m.Index:携带的日志的最小日志索引, m.LogTerm:携带的第一条日志任期, m.Commit:leader记录的本机点已经commit的日志索引
	// m.Entries... 真正的日志数据
	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		// 返回收到的最后一条日志的索引,这样Leader节点就可以根据此值更新其对应的Next和Match值
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: mlastIndex})
	} else {
		// 收到的日志索引任期不满足以下条件:任期一样,日志索引比lastIndex大1

		// 上面的maybeAppend() 方法只会将日志存储到RaftLog维护的内存队列中,
		// 日志的持久化是异步进行的,这个和Leader节点的存储WAL逻辑基本相同.
		// 有一点区别就是follower节点正式发送MsgAppResp消息会在wal保存成功后
		// 而leader节点是先发送消息,后保存的wal.

		//   idx        1 2 3 4 5 6 7 8 9
		//              -----------------
		//   term (L)   1 3 3 3 5 5 5 5 5
		//   term (F)   1 1 1 1 2 2
		// extern 当flower多一些未commit数据时, Leader是如何精准地找到每个Follower 与其日志条目首个不一致的那个槽位的呢
		// Follower 将之后的删除,重新同步leader之后的数据
		// 如采追加记录失败,则将失/败信息返回给Leader节点(即MsgAppResp 消息的Reject字段为true),
		// 同时返回的还有一些提示信息(RejectHint字段保存了当前节点raftLog中最后一条记录的索引)

		index, err := r.raftLog.term(m.Index) // 判断leader传过来的index在本地是否有存储
		r.logger.Debugf("%x [logterm: %d, index: %d]拒绝消息MsgApp [logterm: %d, index: %d] from %x",
			r.id, r.raftLog.zeroTermOnErrCompacted(index, err), m.Index, m.LogTerm, m.Index, m.From)
		// 向leader返回一个关于两个日志可能出现分歧关于 index 和 term 的提示.
		// if m.LogTerm >= term &&  m.Index >= index 可以跳过一些follower拥有的未提交数据
		hintIndex := min(m.Index, r.raftLog.lastIndex())               // 发来的消息最小索引与当前最新消息, 一般来说后者会比较小,6
		hintIndex = r.raftLog.findConflictByTerm(hintIndex, m.LogTerm) // 核心逻辑
		hintTerm, err := r.raftLog.term(hintIndex)
		if err != nil {
			panic(fmt.Sprintf("term(%d)必须是valid, but got %v", hintIndex, err))
		}
		r.send(pb.Message{
			To:         m.From,
			Type:       pb.MsgAppResp,
			Index:      m.Index,
			Reject:     true,
			RejectHint: hintIndex,
			LogTerm:    hintTerm,
		})
	}
}

// 判断提交的日志任期与当前任期 是否一致
func (r *raft) committedEntryInCurrentTerm() bool {
	// 获取最新提交的日志所对应的任期 == 当前的任期
	return r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) == r.Term
}

// 发送超时消息
func (r *raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, Type: pb.MsgTimeoutNow}) // 给指定节点发消息
}
