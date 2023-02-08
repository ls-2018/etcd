package raft

import (
	"bytes"
	"fmt"

	"github.com/ls-2018/etcd_cn/raft/quorum"
	pb "github.com/ls-2018/etcd_cn/raft/raftpb"
	"github.com/ls-2018/etcd_cn/raft/tracker"
)

// Step 分流各种消息
func (r *raft) Step(m pb.Message) error {
	// 处理消息的期限,这可能会导致我们成为一名follower.
	switch {
	case m.Term == 0:
		// 本地消息 MsgHup、MsgProp、MsgReadindex
	case m.Term > r.Term: // 投票或预投票请求
		// 消息的Term大于节点当前的Term
		if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {
			// 如果收到的是投票类消息
			// 当context为campaignTransfer时表示强制要求进行竞选
			force := bytes.Equal(m.Context, []byte(campaignTransfer))
			// 是否在租约期以内
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
			if !force && inLease {
				// 如果非强制,而且又在租约期以内,就不做任何处理
				// 非强制又在租约期内可以忽略选举消息,见论文的4.2.3,这是为了阻止已经离开集群的节点再次发起投票请求
				r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] 忽略消息 %s from %x [logterm: %d, index: %d] at term %d: 租约没有过期 (remaining ticks: %d)",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
				return nil
			}
		}
		switch {
		case m.Type == pb.MsgPreVote:
			// 在应答一个prevote消息时不对任期term做修改
		case m.Type == pb.MsgPreVoteResp && !m.Reject:
			// We send pre-vote requests with a term in our future. If the
			// pre-vote is granted, we will increment our term when we get a
			// quorum. If it is not, the term comes from the localNode that
			// rejected our vote so we should become a follower at the new
			// term.
		default:
			r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
			if m.Type == pb.MsgApp || m.Type == pb.MsgHeartbeat || m.Type == pb.MsgSnap {
				r.becomeFollower(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, None)
			}
		}

	case m.Term < r.Term:
		if (r.checkQuorum || r.preVote) && (m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp) {
			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this localNode has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MsgVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MsgVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed localNode will send MsgVotes (or MsgPreVotes) which will be ignored,
			// but it will not receive MsgApp or MsgHeartbeat, so it will not create
			// disruptive term increases, by notifying leader of this localNode's activeness.
			// The above comments also true for Pre-Vote
			//
			// When follower gets isolated, it soon starts an election ending
			// up with a higher term than leader, although it won't receive enough
			// votes to win the election. When it regains connectivity, this response
			// with "pb.MsgAppResp" of higher term would force leader to step down.
			// However, this disruption is inevitable to free this stuck localNode with
			// fresh election. This can be prevented with Pre-Vote phase.
			r.send(pb.Message{To: m.From, Type: pb.MsgAppResp})
		} else if m.Type == pb.MsgPreVote {
			// Before Pre-Vote enable, there may have candidate with higher term,
			// but less log. After update to Pre-Vote, the cluster may deadlock if
			// we drop messages with a lower term.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: pb.MsgPreVoteResp, Reject: true})
		} else {
			// ignore other cases
			r.logger.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
		}
		return nil
	}

	switch m.Type {
	case pb.MsgHup: // 没有leader时会触发, 开始选举
		if r.preVote { // PreVote 是否启用PreVote
			r.hup(campaignPreElection)
		} else {
			r.hup(campaignElection)
		}

	case pb.MsgVote, pb.MsgPreVote:
		// 当前节点在参与投票时,会综合下面几个条件决定是否投票(在Raft协议的介绍中也捉到过)．
		// 1. 投票情况是已经投过了
		// 2. 没投过并且没有leader
		// 3. 预投票并且term大

		// case  leader 转移 ---> raft Timeout -> follower --->  all
		// 如果转移m.From会默认填充leader id,  --->  r.Vote == m.From =true   ； r.Vote == None =false   ; m.Type == pb.MsgPreVote=false   canVote是False
		canVote := r.Vote == m.From || (r.Vote == None && r.lead == None) || (m.Type == pb.MsgPreVote && m.Term > r.Term)
		if canVote && r.raftLog.isUpToDate(m.Index, m.LogTerm) {

			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: m.Term, Type: voteRespMsgType(m.Type)})
			if m.Type == pb.MsgVote {
				// 只记录真实的投票.
				r.electionElapsed = 0
				r.Vote = m.From // 当前节点的选票投给了谁做我Leader
			}
		} else {
			// 不满足上述投赞同票条件时,当前节点会返回拒绝票(响应消息中的Reject字段会设立成true)
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] 拒绝来自投票请求 %s %x [logterm: %d, index: %d] 当前任期 %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})
		}

	default:
		err := r.step(r, m) // 如果当前节点是Follower状态,raft.step字段指向stepFollower()函数
		if err != nil {
			return err
		}
	}
	return nil
}

type stepFunc func(r *raft, m pb.Message) error

func stepLeader(r *raft, m pb.Message) error {
	// 这些消息不会处理These message types do not require any progress for m.From.
	switch m.Type {
	case pb.MsgBeat: // leader专属
		r.bcastHeartbeat()
		return nil
		//--------------------其他消息处理----------------------
	case pb.MsgCheckQuorum: // leader专属
		// 将 leader 自己的 RecentActive 状态设置为 true
		if pr := r.prstrack.Progress[r.id]; pr != nil {
			pr.RecentActive = true
		}
		if !r.prstrack.QuorumActive() {
			// 如果当前 leader 发现其不满足 quorum 的条件,则说明该 leader 有可能处于隔离状态,step down
			r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
			r.becomeFollower(r.Term, None)
		}
		// Mark everyone (but ourselves) as inactive in preparation for the next
		// CheckQuorum.
		r.prstrack.Visit(func(id uint64, pr *tracker.Progress) {
			if id != r.id {
				pr.RecentActive = false
			}
		})
		return nil
	case pb.MsgProp: // leader、Candidate、follower专属
		if len(m.Entries) == 0 {
			r.logger.Panicf("%x stepped empty MsgProp", r.id)
		}
		if r.prstrack.Progress[r.id] == nil {
			// 判断当前节点是不是已经被从集群中移除了
			return ErrProposalDropped
		}
		if r.leadTransferee != None {
			// 如果正在进行leader切换,拒绝写入
			r.logger.Debugf("%x [term %d]  // 如果正在进行leader切换,拒绝写入 %x  ", r.id, r.Term, r.leadTransferee)
			return ErrProposalDropped
		}

		for i := range m.Entries { // 判断是否有配置变更的日志,有的话做一些特殊处理
			e := &m.Entries[i]
			var cc pb.ConfChangeI
			if e.Type == pb.EntryConfChange {
				var ccc pb.ConfChangeV1
				if err := ccc.Unmarshal(e.Data); err != nil {
					panic(err)
				}
				cc = ccc
			} else if e.Type == pb.EntryConfChangeV2 {
				var ccc pb.ConfChangeV2
				if err := ccc.Unmarshal(e.Data); err != nil {
					panic(err)
				}
				cc = ccc
			}
			if cc != nil {
				alreadyPending := r.pendingConfIndex > r.raftLog.applied // 是否已经apply了该配置变更
				alreadyJoint := len(r.prstrack.Config.Voters[1]) > 0     // 判断第二个MajorityConfig:map[uint64]struct{} 有没有数据
				wantsLeaveJoint := len(cc.AsV2().Changes) == 0           // 节点个数
				// 首先切换到过渡形态,我们称之为联合共识;
				// 一旦提交了联合共识,系统就会过渡到新的配置.联合共识结合了新旧配置.
				var refused string
				if alreadyPending {
					refused = fmt.Sprintf("在索引%d处可能有未应用的conf变更(应用于%d).", r.pendingConfIndex, r.raftLog.applied)
				} else if alreadyJoint && !wantsLeaveJoint {
					refused = "必须先从联合配置中过渡出去"
				} else if !alreadyJoint && wantsLeaveJoint {
					refused = "不处于联合状态;拒绝空洞的改变"
				}
				// true, true
				// false false
				if refused != "" { // 忽略配置变更
					// 如果发现当前是在joint consensus过程中,拒绝变更,直接将message type 变成普通的entry.
					// 处理完毕后,会等待将该消息分发.
					r.logger.Infof("%x 忽略配置变更 %v  %s: %s", r.id, cc, r.prstrack.Config, refused)
					m.Entries[i] = pb.Entry{Type: pb.EntryNormal}
				} else {
					r.pendingConfIndex = r.raftLog.lastIndex() + uint64(i) + 1
				}
			}
		}
		// 将日志追加到raft unstable 中
		if !r.appendEntry(m.Entries...) { // 类似于健康检查的消息,就会走这里
			return ErrProposalDropped
		}
		// 发送日志给集群其它节点
		r.bcastAppend()
		return nil
	case pb.MsgReadIndex:
		// 表示当前集群只有一个节点,当前节点就是leader
		if r.prstrack.IsSingleton() {
			// 记录当前的commit index,称为ReadIndex；
			resp := r.responseToReadIndexReq(m, r.raftLog.committed)
			if resp.To != None {
				r.send(resp)
			}
			return nil
		}

		// 当leader在其任期内没有提交任何日志记录时,推迟只读请求.
		if !r.committedEntryInCurrentTerm() { // 任期变更时,有数据没有committed
			r.pendingReadIndexMessages = append(r.pendingReadIndexMessages, m)
			return nil
		}

		// 发送消息读取响应
		sendMsgReadIndexResponse(r, m) // case pb.MsgReadIndex:
		return nil
	}
	// 根据消息的From字段获取对应的Progress实例,为后面的消息处理做准备
	pr := r.prstrack.Progress[m.From]
	if pr == nil {
		r.logger.Debugf("%x no progress available for %x", r.id, m.From)
		return nil
	}
	switch m.Type {
	case pb.MsgAppResp:
		// Leader节点在向Follower广播日志后,就一直在等待follower的MsgAppResp消息,收到后还是会进到stepLeader函数.
		// 更新对应Progress实例的RecentActive字段,从Leader节点的角度来看,MsgAppResp消息的发送节点还是存活的

		pr.RecentActive = true

		if m.Reject { // MsgApp 消息被拒绝;如果收到的是reject消息,则根据follower反馈的index重新发送日志
			_ = r.handleAppendEntries // 含有拒绝的逻辑
			r.logger.Debugf("%x 收到 MsgAppResp(rejected, hint: (index %d, term %d)) from %x for index %d", r.id, m.RejectHint, m.LogTerm, m.From, m.Index)
			// 发送的是  9 5
			nextProbeIdx := m.RejectHint // 拒绝之处的日志索引 6
			if m.LogTerm > 0 {           // 拒绝之处的日志任期 2
				//  example 1
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 3 3 3 5 5 5 5 5
				//   term (F)   1 1 1 1 2 2
				nextProbeIdx = r.raftLog.findConflictByTerm(m.RejectHint, m.LogTerm) // 下一次直接发送索引为1的消息  🐂
			}
			// 通过MsgAppResp消息携带的信息及对应的Progress状态,重新设立其Next
			if pr.MaybeDecrTo(m.Index, nextProbeIdx) { // leader是否降低对该节点索引记录 ---- > 降低索引数据
				r.logger.Debugf("%x回滚进度  节点:%x to [%s]", r.id, m.From, pr)
				if pr.State == tracker.StateReplicate {
					pr.BecomeProbe()
				}
				r.sendAppend(m.From)
			}
		} else {
			// 走到这说明   之前发送的MsgApp消息已经被对方的Follower节点接收(Entry记录被成功追加)
			oldPaused := pr.IsPaused()
			// m.Index: 对应Follower节点收到的raftLog中最后一条Entry记录的索引,
			if pr.MaybeUpdate(m.Index) { // 更新pr的进度

				switch {
				case pr.State == tracker.StateProbe:
					// 一旦更新了pr状态,就不再进行探测
					pr.BecomeReplicate()
				case pr.State == tracker.StateSnapshot && pr.Match >= pr.PendingSnapshot:
					// 复制完快照
					r.logger.Debugf("%x 从需要的快照中恢复,恢复发送复制信息到 %x [%s]", r.id, m.From, pr)
					pr.BecomeProbe()     // 再探测一次
					pr.BecomeReplicate() // 正常发送日志
				case pr.State == tracker.StateReplicate:
					pr.Inflights.FreeLE(m.Index)
				}
				// 如果进度有更新,判断并更新commitIndex
				// 收到一个Follower节点的MsgAppResp消息之后,除了修改相应的Match和Next,还会尝试更新raftLog.committed,因为有些Entry记录可能在此次复制中被保存到了
				// 半数以上的节点中,raft.maybeCommit()方法在前面已经分析过了
				if r.maybeCommit() {
					// committed index has progressed for the term, so it is safe
					// to respond to pending read index requests
					releasePendingReadIndexMessages(r)
					// 向所有节点发送MsgApp消息,注意,此次MsgApp消息的Commit字段与上次MsgApp消息已经不同,raft.bcastAppend()方法前面已经讲过

					r.bcastAppend()
				} else if oldPaused {
					// 之前是pause状态,现在可以任性地发消息了
					// 之前Leader节点暂停向该Follower节点发送消息,收到MsgAppResp消息后,在上述代码中已经重立了相应状态,所以可以继续发送MsgApp消息
					// If we were paused before, this localNode may be missing the
					// latest commit index, so send it.
					r.sendAppend(m.From)
				}
				// We've updated flow control information above, which may
				// allow us to send multiple (size-limited) in-flight messages
				// at once (such as when transitioning from probe to
				// replicate, or when freeTo() covers multiple messages). If
				// we have more entries to send, send as many messages as we
				// can (without sending empty messages for the commit index)
				// 循环发送所有剩余的日志给follower
				for r.maybeSendAppend(m.From, false) {
				}
				// 是否正在进行leader转移
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MsgHeartbeatResp:
		pr.RecentActive = true
		pr.ProbeSent = false

		// free one slot for the full inflights window to allow progress.
		if pr.State == tracker.StateReplicate && pr.Inflights.Full() {
			pr.Inflights.FreeFirstOne()
		}
		if pr.Match < r.raftLog.lastIndex() {
			r.sendAppend(m.From)
		}

		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return nil
		}
		// 判断leader有没有收到大多数节点的确认
		// 也就是ReadIndex算法中,leader节点得到follower的确认,证明自己目前还是Leader
		readIndexStates := r.readOnly.recvAck(m.From, m.Context) // 记录了每个节点对  m.Context  的响应
		xxx := r.prstrack.Voters.VoteResult(readIndexStates)
		if xxx != quorum.VoteWon {
			return nil
		}
		// 收到了响应节点超过半数,会清空readOnly中指定消息ID及之前的所有记录
		rss := r.readOnly.advance(m) // 响应的ReadIndex
		// 返回follower的心跳回执
		for _, rs := range rss {
			if resp := r.responseToReadIndexReq(rs.req, rs.index); resp.To != None {
				r.send(resp)
			}
		}
	case pb.MsgSnapStatus:
		if pr.State != tracker.StateSnapshot {
			return nil
		}
		// TODO(tbg): this code is very similar to the snapshot handling in
		// MsgAppResp above. In fact, the code there is more correct than the
		// code here and should likely be updated to match (or even better, the
		// logic pulled into a newly created Progress state machine handler).
		if !m.Reject {
			pr.BecomeProbe()
			r.logger.Debugf("%x snapshot succeeded, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		} else {
			// NB: the order here matters or we'll be probing erroneously from
			// the snapshot index, but the snapshot never applied.
			pr.PendingSnapshot = 0
			pr.BecomeProbe()
			r.logger.Debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		}
		// If snapshot finish, wait for the MsgAppResp from the remote localNode before sending
		// out the next MsgApp.
		// If snapshot failure, wait for a heartbeat interval before next try
		pr.ProbeSent = true
	case pb.MsgUnreachable:
		// During optimistic replication, if the remote becomes unreachable,
		// there is huge probability that a MsgApp is lost.
		if pr.State == tracker.StateReplicate {
			pr.BecomeProbe()
		}
		r.logger.Debugf("%x 发送消息到 %x  失败 ,因为不可达[%s]", r.id, m.From, pr)
	case pb.MsgTransferLeader:
		// pr 当前leader对该节点状态的记录
		// client ---> raft --- > leader
		// client ---> raft --- > follower --- > leader
		if pr.IsLearner {
			r.logger.Debugf("%x 是learner.忽视转移领导", r.id)
			return nil
		}
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("%x [term %d] 正在转移leader给%x,忽略对同一个localNode的请求%x", r.id, r.Term, leadTransferee, leadTransferee)
				return nil
			}
			r.abortLeaderTransfer() // 上一个leader转移没完成,又进行下一个
			r.logger.Infof("%x [term %d] 取消先前的领导权移交 %x", r.id, r.Term, lastLeadTransferee)
		}
		if leadTransferee == r.id {
			r.logger.Debugf("%x 已经是leader", r.id)
			return nil
		}
		r.logger.Infof("%x [term %d] 开始进行leader转移to %x", r.id, r.Term, leadTransferee)
		// 转移领导权应该在一个electionTimeout中完成,因此重置r.e tionelapsed.
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.raftLog.lastIndex() {
			// leader转移 follower 发送申请投票消息,但是任期不会增加, context 是CampaignTransfer
			r.sendTimeoutNow(leadTransferee) // 发送方,指定为了下任leader
			r.logger.Infof("%x 立即发送MsgTimeoutNow到%x,因为%x已经有最新的日志", r.id, leadTransferee, leadTransferee)
		} else {
			r.sendAppend(leadTransferee) // 发送转移到哪个节点,用于加快该节点的日志进度
		}
	}
	return nil
}

// stepCandidate 两个阶段都会调用 StateCandidate and StatePreCandidate;
// 区别在于对投票请求的处理
func stepCandidate(r *raft, m pb.Message) error {
	// 根据当前节点的状态决定其能够处理的选举响应消息的类型
	var myVoteRespType pb.MessageType
	if r.state == StatePreCandidate {
		myVoteRespType = pb.MsgPreVoteResp
	} else {
		myVoteRespType = pb.MsgVoteResp
	}
	switch m.Type {
	case pb.MsgProp: // leader、Candidate、follower专属
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MsgApp:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat: // √
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case myVoteRespType: // ✅
		// 投票、预投票
		// 处理收到的选举响应消息,当前示例中处理的是MsgPreVoteResp消息
		gr, rj, res := r.poll(m.From, m.Type, !m.Reject) // 计算当前收到多少投票
		r.logger.Infof("%x 收到了 %d %s 同已投票 %d 拒绝投票", r.id, gr, m.Type, rj)
		// 投票数、拒绝数 过半判定
		switch res {
		case quorum.VoteWon: // 如果quorum 都选择了投票
			if r.state == StatePreCandidate {
				r.campaign(campaignElection) // 预投票发起正式投票
			} else {
				r.becomeLeader() // 当前节点切换成为Leader状态, 其中会重置每个节点对应的Next和Match两个索引,
				r.bcastAppend()  // 向集群中其他节点广播MsgApp消息
			}
		case quorum.VoteLost: // 集票失败,转为 follower
			r.becomeFollower(r.Term, None) // 注意,此时任期没有改变
		}
	case pb.MsgTimeoutNow:
		r.logger.Debugf("%x [term %d state %v] 忽略 MsgTimeoutNow 消息from %x", r.id, r.Term, r.state, m.From)
	}
	return nil
}

// follower 的功能
func stepFollower(r *raft, m pb.Message) error {
	switch m.Type {
	case pb.MsgProp: // leader、Candidate、follower专属
		if r.lead == None {
			r.logger.Infof("%x 由于当前没有leader   term %d; 拒绝提议", r.id, r.Term)
			return ErrProposalDropped
		} else if r.disableProposalForwarding {
			r.logger.Infof("%x 禁止转发回leader %x at term %d; 拒绝提议", r.id, r.lead, r.Term)
			return ErrProposalDropped
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgApp:
		// 配置信息也会走到这里
		// Leader节点处理完命令后,发送日志和持久化操作都是异步进行的,但是这不代表leader已经收到回复.
		// Raft协议要求在返回leader成功的时候,日志一定已经提交了,所以Leader需要等待超过半数的Follower节点处理完日志并反馈,下面先看一下Follower的日志处理.
		// 日志消息到达Follower后,也是由EtcdServer.Process()方法来处理,最终会进到Raft模块的stepFollower()函数中.
		// 重置心跳计数
		r.electionElapsed = 0
		// 设置Leader
		r.lead = m.From
		// 处理日志条目
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case pb.MsgTransferLeader:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; 丢弃leader转移消息", r.id, r.Term)
			return nil
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgTimeoutNow:
		r.logger.Infof("%x [term %d] 收到来自 %x(下任leader) 的MsgTimeoutNow,并开始选举获得领导.", r.id, r.Term, m.From)
		// 即使r.preVote为真,领导层转移也不会使用pre-vote;我们知道我们不是在从一个分区恢复,所以不需要额外的往返.
		r.hup(campaignTransfer)

	case pb.MsgReadIndex: // ✅
		if r.lead == None {
			r.logger.Infof("%x 当前任期没有leader %d; 跳过读索引", r.id, r.Term)
			return nil
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgReadIndexResp: // ✅
		if len(m.Entries) != 1 {
			r.logger.Errorf("%x  来自 %x的 MsgReadIndexResp 格式无效, 日志条数: %d", r.id, m.From, len(m.Entries))
			return nil
		}
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
	return nil
}

// roleUp 是否可以被提升为leader.
func (r *raft) roleUp() bool {
	pr := r.prstrack.Progress[r.id] // 是本节点raft的身份
	// 节点不是learner 且 没有正在应用快照
	return pr != nil && !pr.IsLearner && !r.raftLog.hasPendingSnapshot()
}

// 变成Follower       当前任期,当前leader
func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = StateFollower
	r.logger.Infof("%x 成为Follower 在任期: %d", r.id, r.Term)
}

// 变成竞选者角色
func (r *raft) becomeCandidate() {
	if r.state == StateLeader {
		panic("无效的转移 [leader -> candidate]")
	}
	r.step = stepCandidate
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	r.Vote = r.id // 当前节点的选票投给了谁做我Leader
	r.state = StateCandidate
	r.logger.Infof("%x 成为Candidate 在任期: %d", r.id, r.Term)
}

// 变成预竞选者角色
func (r *raft) becomePreCandidate() {
	if r.state == StateLeader {
		panic("无效的转移 [leader -> pre-candidate]")
	}
	// 变成预竞选者更新step func和state,但绝对不能增加任期和投票
	r.step = stepCandidate
	r.prstrack.ResetVotes() // // 清空接收到了哪些节点的投票
	r.tick = r.tickElection
	r.lead = None
	r.state = StatePreCandidate
	r.logger.Infof("%x 成为 pre-candidate在任期 %d", r.id, r.Term)
}

func (r *raft) becomeLeader() {
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.lead = r.id
	r.state = StateLeader
	// Followers enter replicate mode when they've been successfully probed
	// (perhaps after having received a snapshot as a result). The leader is
	// trivially in this state. Note that r.reset() has initialized this
	// progress with the last index already.
	r.prstrack.Progress[r.id].BecomeReplicate()

	// Conservatively set the pendingConfIndex to the last index in the
	// log. There may or may not be a pending config change, but it's
	// safe to delay any future proposals until we commit all our
	// pending log entries, and scanning the entire tail of the log
	// could be expensive.
	r.pendingConfIndex = r.raftLog.lastIndex()

	emptyEnt := pb.Entry{Data: nil}
	if !r.appendEntry(emptyEnt) {
		// This won't happen because we just called reset() above.
		r.logger.Panic("empty entry was dropped")
	}
	// As a special case, don't count the initial empty entry towards the
	// uncommitted log quota. This is because we want to preserve the
	// behavior of allowing one entry larger than quota if the current
	// usage is zero.
	r.reduceUncommittedSize([]pb.Entry{emptyEnt})
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}
