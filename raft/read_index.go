package raft

import pb "github.com/ls-2018/etcd_cn/raft/raftpb"

func releasePendingReadIndexMessages(r *raft) {
	if !r.committedEntryInCurrentTerm() {
		r.logger.Error("pending MsgReadIndex should be released only after first commit in current term")
		return
	}

	msgs := r.pendingReadIndexMessages
	r.pendingReadIndexMessages = nil

	for _, m := range msgs {
		sendMsgReadIndexResponse(r, m)
	}
}

// raft结构体中的readOnly作用是批量处理只读请求,只读请求有两种模式,分别是ReadOnlySafe和ReadOnlyLeaseBased
// ReadOnlySafe是ETCD作者推荐的模式,因为这种模式不受节点之间时钟差异和网络分区的影响
// 线性一致性读用的就是ReadOnlySafe
func sendMsgReadIndexResponse(r *raft, m pb.Message) {
	switch r.readOnly.option {
	// 如果需要更多的地方投票,进行全面的广播.
	case ReadOnlySafe:
		//	该线性读模式,每次 Follower 进行读请求时,需要和 Leader 同步日志提交位点信息,而 Leader需要向过半的 Follower 发起证明自己是 Leader 的轻量的 RPC 请求,
		//	相当于一个 Follower 读,至少需要 1 +(n/2)+ 1 次的 RPC 请求.

		// 清空readOnly中指定消息ID及之前的所有记录
		r.readOnly.addRequest(r.raftLog.committed, m) // 记录当前节点的raftLog.committed字段值,即已提交位置

		// recvAck通知只读结构raft状态机已收到对附加只读请求上下文的心跳信号的确认.
		r.readOnly.recvAck(r.id, m.Entries[0].Data) // 本机确认此消息
		// leader 节点向其他节点发起广播
		r.bcastHeartbeatWithCtx(m.Entries[0].Data)

	case ReadOnlyLeaseBased:
		if resp := r.responseToReadIndexReq(m, r.raftLog.committed); resp.To != None {
			r.send(resp)
		}
	}
}

// responseToReadIndexReq 为' req '构造响应.如果' req '来自对等体本身,则返回一个空值.
func (r *raft) responseToReadIndexReq(req pb.Message, readIndex uint64) pb.Message {
	// 通过from来判断该消息是否是follower节点转发到leader中的
	// 如果是客户端直接发到leader节点的消息,将MsgReadIndex消息中的已提交位置和消息id封装成ReadState实例,添加到readStates
	// raft 模块也有一个 for-loop 的 goroutine,来读取该数组,并对MsgReadIndex进行响应
	if req.From == None || req.From == r.id {
		r.readStates = append(r.readStates, ReadState{
			Index:      readIndex,
			RequestCtx: req.Entries[0].Data,
		})
		return pb.Message{}
	}
	// 转发自follower
	return pb.Message{
		Type:    pb.MsgReadIndexResp,
		To:      req.From,
		Index:   readIndex,
		Entries: req.Entries,
	}
}
