package raft

import (
	"context"

	pb "github.com/ls-2018/etcd_cn/raft/raftpb"
)

// RaftNodeInterFace raft 节点
type RaftNodeInterFace interface {
	Tick()                                                           // 触发一次Tick,会触发Node心跳或者选举
	Campaign(ctx context.Context) error                              // 主动触发一次选举
	Propose(ctx context.Context, data []byte) error                  // 使用者的data通过日志广播到所有节点,返回nil,不代表成功,因为是异步的
	ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error  // 集群配置变更,[新增、删除节点]
	Step(ctx context.Context, msg pb.Message) error                  // 处理msg的函数
	Ready() <-chan Ready                                             // 已经commit,准备apply的数据通过这里通知
	Advance()                                                        // ready消息处理完后,发送一个通知消息;当处理完这些消息后,必须调用,不然raft会堵塞在这里
	ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState                 // 应用集群变化到状态机
	TransferLeadership(ctx context.Context, lead, transferee uint64) // 将Leader转给transferee.
	Status() Status                                                  // 返回 raft state machine当前状态.
	ReportSnapshot(id uint64, status SnapshotStatus)                 // 告诉leader状态机该id节点发送snapshot的最终处理状态
	ReadIndex(ctx context.Context, rctx []byte) error                // follower当请求的索引不存在时,记录,当applied >= 该索引时,通过Ready()返回给使用者
	ReportUnreachable(id uint64)                                     // 报告raft指定的节点上次发送没有成功
	Stop()                                                           // 关闭节点
}

// Ready raft 准备好需要使用者处理的数据
type Ready struct {
	// Ready引入的第一个概念就是"软状态",主要是节点运行状态,包括Leader是谁,自己是什么角色.该参数为nil代表软状态没有变化
	*SoftState

	// Ready引入的第二个概念局势"硬状态",主要是集群运行状态,包括任期、提交索引和Leader.如何
	// 更好理解软、硬状态?一句话,硬状态需要使用者持久化,而软状态不需要,就好像一个是内存
	// 值一个是硬盘值,这样就比较好理解了.
	pb.HardState

	ReadStates []ReadState // 这个参数就是Node.ReadIndex()的结果回调.  携带了当前leader已经committed的索引,用于表示raft还活着

	// 需要存入可靠存储的日志,还记得log那片文章里面提到的unstable么,这些日志就是从哪里获取的.
	Entries []pb.Entry

	// 需要存入可靠存储的快照,它也是来自unstable
	Snapshot pb.Snapshot

	// 已经提交的日志,用于使用者应用这些日志,需要注意的是,CommittedEntries可能与Entries有
	// 重叠的日志,这是因为Leader确认一半以上的节点接收就可以提交.而节点接收到新的提交索引的消息
	// 的时候,一些日志可能还存储在unstable中.
	CommittedEntries []pb.Entry

	// Messages 日志被提交到稳定的存储.如果它包含一个MsgSnap消息,应用程序必须在收到快照或调用ReportSnapshot失败时向raft报告.
	// Messages是需要发送给其他节点的消息,raft负责封装消息但不负责发送消息,消息发送需要使用者来实现
	Messages []pb.Message // 就是raft.msgs

	/// MustSync指示了硬状态和不可靠日志是否必须同步的写入磁盘还是异步写入,也就是使用者必须把
	// 数据同步到磁盘后才能调用Advance()
	MustSync bool
}

// appliedCursor 从Ready中提取客户端apply的最高索引 (一旦通过Advance确认Ready) .
func (rd Ready) appliedCursor() uint64 {
	if n := len(rd.CommittedEntries); n > 0 {
		return rd.CommittedEntries[n-1].Index
	}
	if index := rd.Snapshot.Metadata.Index; index > 0 {
		return index
	}
	return 0
}
