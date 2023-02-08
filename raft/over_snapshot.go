package raft

import (
	"fmt"

	"github.com/ls-2018/etcd_cn/raft/confchange"
	pb "github.com/ls-2018/etcd_cn/raft/raftpb"
	"github.com/ls-2018/etcd_cn/raft/tracker"
)

// follower、candidate 都有可能会接受到快照消息;  candidate 会变成follower
func (r *raft) handleSnapshot(m pb.Message) {
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.restore(m.Snapshot) {
		r.logger.Infof("%x [commit: %d] 重置快照 [index: %d, term: %d]", r.id, r.raftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.lastIndex()})
	} else {
		r.logger.Infof("%x [commit: %d] 忽略快照 [index: %d, term: %d]", r.id, r.raftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
	}
}

// restore 从一个快照中恢复状态机.它恢复了日志和状态机的配置.如果这个方法返回false,说明快照被忽略了,因为它已经过时了,或者是由于一个错误.
func (r *raft) restore(s pb.Snapshot) bool {
	// snapshot的index比自身committed要小,说明已有这些数据,返回false
	if s.Metadata.Index <= r.raftLog.committed {
		return false
	}
	if r.state != StateFollower { // 在收到快照消息时,成为了leader
		// 这是深度防御:如果领导者以某种方式结束了应用快照,它可以进入一个新的任期而不进入跟随者状态.
		// 这应该永远不会发生,但如果它发生了,我们会通过提前返回来防止损害,所以只记录一个响亮的警告.
		// 在写这篇文章的时候,当这个方法被调用时,实例被保证处于跟随者状态.
		r.logger.Warningf("%x 试图将快照恢复为领导者;这不应该发生.", r.id)
		r.becomeFollower(r.Term+1, None)
		return false
	}

	// 更多的深度防御:如果收件人不在配置中,就扔掉快照.
	// 这不应该发生(在写这篇文章的时候),但这里和那里的很多代码都假定r.id在进度跟踪器中.
	found := false
	cs := s.Metadata.ConfState
	for _, set := range [][]uint64{
		cs.Voters,
		cs.Learners,
		cs.VotersOutgoing,
	} {
		for _, id := range set {
			if id == r.id {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		r.logger.Warningf("%x 试图恢复快照,但它不在ConfState %v中;不应该发生这种情况.", r.id, cs)
		return false
	}

	// 现在,继续前进并实际恢复.
	if r.raftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		// 自身日志条目中有相应的term和index,说明已有这些数据,返回false
		r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] 快速提交到快照[index: %d, term: %d]",
			r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
		r.raftLog.commitTo(s.Metadata.Index)
		return false
	}
	// 自身没有这一部分数据
	r.raftLog.restore(s)

	// 重置配置并重新添加(可能更新的)对等体.
	r.prstrack = tracker.MakeProgressTracker(r.prstrack.MaxInflight) // 相当于重置prs信息
	cfg, prs, err := confchange.Restore(confchange.Changer{
		Tracker:   r.prstrack,
		LastIndex: r.raftLog.lastIndex(),
	}, cs)
	if err != nil {
		panic(fmt.Sprintf(" 要么是我们的配置变更处理有问题,要么是客户端破坏了配置变更. %+v: %s", cs, err))
	}

	assertConfStatesEquivalent(r.logger, cs, r.switchToConfig(cfg, prs))

	pr := r.prstrack.Progress[r.id]
	pr.MaybeUpdate(pr.Next - 1) // TODO(tbg): 这是未经测试的,可能不需要的.

	r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] 重置快照信息[index: %d, term: %d]",
		r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
	return true
}
