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
	"context"
	"errors"

	pb "github.com/ls-2018/etcd_cn/raft/raftpb"
)

type SnapshotStatus int

const (
	SnapshotFinish  SnapshotStatus = 1
	SnapshotFailure SnapshotStatus = 2
)

var (
	emptyState = pb.HardState{}
	ErrStopped = errors.New("raft: 已停止")
)

func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// IsEmptyHardState 判断是不是空的
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, emptyState)
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp pb.Snapshot) bool {
	return sp.Metadata.Index == 0
}

type Peer struct {
	ID      uint64 // 成员ID
	Context []byte // 成员信息序列化后的数据
}

// StartNode 它为每个给定的peer在初始日志中添加一个ConfChangeAddNode条目.
// Peer封装了节点的ID, peers记录了当前集群中全部节点的ID
func StartNode(c *Config, peers []Peer) RaftNodeInterFace { // ✅✈️ 🚗🚴🏻😁
	if len(peers) == 0 {
		panic("没有给定peers;使用RestartNode代替.")
	}
	rn, err := NewRawNode(c) // ✅
	if err != nil {
		panic(err)
	}
	rn.Bootstrap(peers) // [{"id":10276657743932975437,"peerURLs":["http://localhost:2380"],"name":"default"}]

	n := newLocalNode(rn) // 本机,用于接收发消息
	go n.run()            // ok

	return &n
}

// RestartNode 集群的当前成员将从Storage中恢复.
// 如果调用者有一个现有的状态机,请传入最后应用于它的日志索引;否则使用0.
func RestartNode(c *Config) RaftNodeInterFace {
	rn, err := NewRawNode(c)
	if err != nil {
		panic(err)
	}
	n := newLocalNode(rn)
	go n.run()
	return &n
}

// Ready数据通过上一次的软、硬状态,计算这两个状态的变化,其他
// 的数据都是来源于raft.
func newReady(r *raft, prevSoftSt *SoftState, prevHardSt pb.HardState) Ready {
	rd := Ready{
		Entries:          r.raftLog.unstableEntries(), // 还没有落盘的,需要调用方落盘
		CommittedEntries: r.raftLog.nextEnts(),        // 已经commit待apply的日志,交给上层应用
		Messages:         r.msgs,                      // 封装好的需要通过网络发送都其他节点的消息
		SoftState:        nil,
		HardState:        pb.HardState{},
		Snapshot:         pb.Snapshot{},
		ReadStates:       []ReadState{},
		MustSync:         false,
	}
	// 判断softState有没有变化,有则赋值
	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		rd.SoftState = softSt
	}
	// 判断hardState有没有变化,有则赋值
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, prevHardSt) {
		rd.HardState = hardSt
	}
	// 判断是不是收到snapshot
	if r.raftLog.unstable.snapshot != nil {
		rd.Snapshot = *r.raftLog.unstable.snapshot
	}
	if len(r.readStates) != 0 {
		rd.ReadStates = r.readStates
	}
	// 处理该Ready后是否需要做fsync,将数据强制刷盘
	rd.MustSync = MustSync(r.hardState(), prevHardSt, len(rd.Entries))
	return rd
}

// 包含在raftNode中,是Node接口的实现.里面包含一个协程和多个队列,是状态机消息处理的入口.
type localNode struct {
	rn         *RawNode
	propc      chan msgWithResult   // Propose队列,调用raftNode的Propose即把Propose消息塞到这个队列里
	recvc      chan pb.Message      // Message队列,除Propose消息以外其他消息塞到这个队列里
	confc      chan pb.ConfChangeV2 // 接受配置变更的管道
	confstatec chan pb.ConfState    // 将配置变更后的管道
	readyc     chan Ready           // 已经准备好apply的信息队列,通知使用者
	advancec   chan struct{}        // 每次apply好了以后往这个队列里塞个空对象.通知raft可以继续准备Ready消息.
	tickc      chan struct{}        // tick信息队列,用于调用心跳
	done       chan struct{}        //
	stop       chan struct{}        // 为Stop接口实现的,应该还好理解
	status     chan chan Status     //
	logger     Logger               // 用来写运行日志的
}

func newLocalNode(rn *RawNode) localNode {
	return localNode{
		confc:      make(chan pb.ConfChangeV2), // 接收EntryConfChange类型消息比如动态添加节点
		rn:         rn,
		propc:      make(chan msgWithResult), // 接收网络层MsgProp类型消息
		recvc:      make(chan pb.Message),    // 接收网络层除MsgProp类型以外的消息
		confstatec: make(chan pb.ConfState),
		readyc:     make(chan Ready),         // 向上层返回 ready
		advancec:   make(chan struct{}),      // 上层处理往ready后返回给raft的消息
		tickc:      make(chan struct{}, 128), // 管理超时的管道,繁忙时可以处理之前的事件
		done:       make(chan struct{}),
		stop:       make(chan struct{}),
		status:     make(chan chan Status),
	}
}

func (n *localNode) Stop() {
	select {
	case n.stop <- struct{}{}:
		// Not already stopped, so trigger it
	case <-n.done:
		// RaftNodeInterFace has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by run()
	<-n.done
}

func (n *localNode) run() {
	var propc chan msgWithResult // 提议
	var readyc chan Ready        // 接收已committed的消息
	var advancec chan struct{}   // 通知raft 继续的channel
	var rd Ready

	r := n.rn.raft
	// 初始状态不知道谁是leader,需要通过Ready获取
	lead := None
	for {
		// 这一段 主要是为了 只有只有客户端通知了了,才能继续往readyc放新的
		if advancec != nil { // 开始时是nil
			readyc = nil
		} else if n.rn.HasReady() { // 判断是否有Ready数据:待发送的数据
			rd = n.rn.readyWithoutAccept() // 计算软硬状态变化;返回ready结构体
			readyc = n.readyc              // 下边有放入数据的
		}
		// 初始时都是0,   lead发生变化
		if lead != r.lead {
			if r.hasLeader() {
				if lead == None {
					r.logger.Infof("raft.localNode: %x 成为了leader %x 在任期 %d", r.id, r.lead, r.Term)
				} else {
					r.logger.Infof("raft.localNode: %x leader变成了 %x to %x 在任期 %d", r.id, lead, r.lead, r.Term)
				}
				propc = n.propc // 从里边消费消息
			} else {
				r.logger.Infof("raft.localNode: %x 丢失leader %x 在任期 %d", r.id, lead, r.Term)
				propc = nil
			}
			lead = r.lead
		}

		select {
		// TODO: maybe buffer the config propose if there exists one (the way
		// described in raft dissertation)
		// Currently it is dropped in Step silently.

		case pm := <-propc: // 接收到提议消息;提议消息是本节点生成的;	版本变更消息处理完后,会关闭propc
			_ = msgWithResult{}
			m := pm.m
			m.From = r.id
			err := r.Step(m) // 因为是异步发送到每个节点的,因此这里不是发送的结果
			if pm.result != nil {
				pm.result <- err
				close(pm.result)
			}
		case m := <-n.recvc: // Message队列, 除Propose消息以外其他消息塞到这个队列里
			// 必须是已知节点、或者是非响应类信息
			// MsgReadIndex类型
			if pr := r.prstrack.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) {
				r.Step(m)
			}
		case cc := <-n.confc: // 配置的变更信息 [新增、删除、更新节点]
			// 如果NodeID是None,就变成了获取节点信息的操作
			_, okBefore := r.prstrack.Progress[r.id] // 获取本节点的信息
			cs := r.applyConfChange(cc)
			// 如果localNode被移除,则阻止传入的变化.请注意,我们只在localNode之前在配置中时才这样做.
			// 节点可能在不知道这一点的情况下成为组的成员(当他们在追赶日志时,没有最新的配置),在这种情况下,我们不希望阻止提案通道.
			// NB:当领导者发生变化时,propc会被重置,如果我们了解到这一点,就有点暗示我们被读取了,也许?这并不 这不是很合理,而且很可能有bug.
			if _, okAfter := r.prstrack.Progress[r.id]; okBefore && !okAfter {
				// 变更前有自己,变更后没有自己
				var found bool
				_ = r.prstrack.ConfState
			outer:
				for _, sl := range [][]uint64{cs.Voters, cs.VotersOutgoing} {
					// 在所有的投票者中,寻找自己
					for _, id := range sl {
						if id == r.id {
							found = true
							break outer
						}
					}
				}
				if !found {
					propc = nil
				}
			}
			select {
			case n.confstatec <- cs:
			case <-n.done:
			}
		case <-n.tickc: // 超时时间到,包括心跳超时和选举超时等
			n.rn.Tick()
		case readyc <- rd: // 数据放入ready channel中,等待上层应用处理
			n.rn.acceptReady(rd)  // 告诉raft,ready数据已被接收
			advancec = n.advancec // 赋值Advance channel等待Ready处理完成的消息
		case <-advancec: // 使用者处理完Ready数据后,调用了Advance()
			n.rn.Advance(rd) // 通知RawNode  应用程序已经应用并保存了最后一个Ready结果的进度.
			rd = Ready{}     // 重置数据
			advancec = nil
		case c := <-n.status: // 收取了获取节点状态的信号
			c <- getStatus(r)
		case <-n.stop: // 收到停止信号
			close(n.done)
			return
		}
	}
}

func (n *localNode) ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState {
	var cs pb.ConfState
	select {
	case n.confc <- cc.AsV2(): // 把配置调整发送到confc
	case <-n.done:
	}
	select {
	case cs = <-n.confstatec: // 再通过confstatec把调整后的结果读出来
	case <-n.done:
	}
	return &cs
}

// ------------------------------------------	over --------------------------------------------------------------

// TransferLeadership leader 由 lead转移给transferee
func (n *localNode) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	select {
	case n.recvc <- pb.Message{Type: pb.MsgTransferLeader, From: transferee, To: lead}:
	case <-n.done:
	case <-ctx.Done():
	}
}

// ReadIndex etcdctl get 会走这里,rctx 是一个生成的索引
func (n *localNode) ReadIndex(ctx context.Context, rctx []byte) error {
	// rctx 后随着响应返回来
	return n.step(ctx, pb.Message{Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: rctx}}}) // ok  data是requestIndex[生成的]
}

func (n *localNode) Ready() <-chan Ready {
	// Ready 如果raft状态机有变化,会通过channel返回一个Ready的数据结构,里面包含变化信息,比如日志变化、心跳发送等.
	return n.readyc
}

func (n *localNode) Advance() {
	select {
	case n.advancec <- struct{}{}:
	case <-n.done:
	}
}

func (n *localNode) Status() Status {
	c := make(chan Status)
	select {
	case n.status <- c: // 通过status把c送给node,让node通过c把Status输出 		  chan chan Status
		_ = getStatus // 就是它的返回结果
		return <-c    // 此时再从c中把Status读出来
	case <-n.done:
		return Status{}
	}
}

func (n *localNode) ReportUnreachable(id uint64) {
	select {
	case n.recvc <- pb.Message{Type: pb.MsgUnreachable, From: id}:
	case <-n.done:
	}
}

func (n *localNode) ReportSnapshot(id uint64, status SnapshotStatus) {
	rej := status == SnapshotFailure

	select {
	case n.recvc <- pb.Message{Type: pb.MsgSnapStatus, From: id, Reject: rej}:
	case <-n.done:
	}
}

func (n *localNode) Propose(ctx context.Context, data []byte) error {
	// 发起提议,要等到得到大多数响应
	return n.stepWait(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}}) // ok
}

func (n *localNode) Step(ctx context.Context, m pb.Message) error {
	// 忽略通过网络接收的非本地信息
	if IsLocalMsg(m.Type) {
		return nil
	}
	return n.step(ctx, m)
}

func (n *localNode) step(ctx context.Context, m pb.Message) error {
	return n.stepWithWaitOption(ctx, m, false)
}

func (n *localNode) stepWait(ctx context.Context, m pb.Message) error {
	return n.stepWithWaitOption(ctx, m, true)
}

func (n *localNode) stepWithWaitOption(ctx context.Context, m pb.Message, wait bool) error {
	if m.Type != pb.MsgProp {
		// 所有的非pb.MsgProp消息通过recvc送给node处理,此时是否wait根本不关心,因为通过recvc
		// 提交给node处理的消息可以理解为没有返回值的调用.
		select {
		case n.recvc <- m: // 非提议信息,放进去就完事了
			return nil // 一般都会走这里
		case <-ctx.Done():
			return ctx.Err()
		case <-n.done:
			return ErrStopped
		}
	}

	// 处理提议消息,等待响应
	ch := n.propc // 生产消息
	pm := msgWithResult{m: m}
	if wait {
		pm.result = make(chan error, 1)
	}

	select {
	case ch <- pm:
		if !wait {
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}

	select {
	case err := <-pm.result:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	return nil
}

func (n *localNode) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	default:
		n.rn.raft.logger.Warningf("%x 错过了开火的时间.RaftNodeInterFace 阻塞时间过长! ", n.rn.raft.id)
	}
}

func (n *localNode) Campaign(ctx context.Context) error {
	// 封装成pb.MsgHup消息然后再处理,step()后面会详细说明
	// 主动触发一次选举
	return n.step(ctx, pb.Message{Type: pb.MsgHup})
}

// MustSync 设置是否必须同步
func MustSync(st, prevst pb.HardState, entsnum int) bool {
	// 有不可靠日志、leader更换以及换届选举都需要设置同步标记,也就是说当有不可靠日志或者
	// 新一轮选举发生时必须等到这些数据同步到可靠存储后才能继续执行,这还算是比较好理解,毕竟
	// 这些状态是全局性的,需要leader统计超过半数可靠可靠以后确认为可靠的数据.如果此时采用
	// 异步实现,就会出现不一致的可能性.
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}

// ProposeConfChange 集群配置变更,[新增、删除节点]
func (n *localNode) ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error {
	msg, err := confChangeToMsg(cc)
	if err != nil {
		return err
	}
	return n.Step(ctx, msg)
}

// 消息、结果包装
type msgWithResult struct {
	m      pb.Message // 发送出去的信息
	result chan error // 返回的结果
}

// 将raftpb.ConfChangeV1变成用于用于发送的pb.Message
func confChangeToMsg(c pb.ConfChangeI) (pb.Message, error) {
	typ, data, err := pb.MarshalConfChange(c)
	if err != nil {
		return pb.Message{}, err
	}
	return pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: typ, Data: data}}}, nil // ok
}
