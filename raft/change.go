package raft

import (
	"github.com/ls-2018/etcd_cn/raft/confchange"
	pb "github.com/ls-2018/etcd_cn/raft/raftpb"
	"github.com/ls-2018/etcd_cn/raft/tracker"
)

// 应用变更配置
func (r *raft) applyConfChange(cc pb.ConfChangeV2) pb.ConfState {
	changer := confchange.Changer{
		Tracker:   r.prstrack,
		LastIndex: r.raftLog.lastIndex(),
	}
	var cfg tracker.Config
	var prs tracker.ProgressMap
	var err error
	if cc.LeaveJoint() { // 判断是不是一个空的ConfChangeV2,离开联合共识
		cfg, prs, err = changer.LeaveJoint()
	} else if autoLeave, ok := cc.EnterJoint(); ok { // 进入联合共识
		// change >1 或 !ConfChangeTransitionAuto
		cfg, prs, err = changer.EnterJoint(autoLeave, cc.Changes...)
	} else {
		cfg, prs, err = changer.Simple(cc.Changes...) // cfg是深拷贝,prs是浅拷贝;获取当前的配置,确保配置和进度是相互兼容的
	}

	if err != nil {
		panic(err)
	}

	return r.switchToConfig(cfg, prs)
}

// switchToConfig 重新配置这个节点以使用所提供的配置.它更新内存中的状态,并在必要时进行额外的操作,
// 如对删除节点或改变的法定人数作出反应.要求.输入通常来自于恢复一个ConfState或应用一个ConfChange.
func (r *raft) switchToConfig(cfg tracker.Config, prs tracker.ProgressMap) pb.ConfState {
	// cfg是深拷贝,prs是浅拷贝;获取当前的配置,确保配置和进度是相互兼容的
	r.prstrack.Config = cfg
	r.prstrack.Progress = prs

	r.logger.Infof("%x 切换配置 %s", r.id, r.prstrack.Config)
	cs := r.prstrack.ConfState()        // 当前集群配置的信息汇总
	pr, ok := r.prstrack.Progress[r.id] // 本机还在不在里边

	// 更新localNode本身是否是学习者,当localNode被移除时重置为false.
	r.isLearner = ok && pr.IsLearner

	if (!ok || r.isLearner) && r.state == StateLeader {
		// leader 降级或者 移除
		return cs
	}

	// 其余的步骤只有在这个localNode是领导者并且还有其他节点的情况下才有意义.
	if r.state != StateLeader || len(cs.Voters) == 0 {
		return cs
	}

	// bcastAppend、maybeSendAppend 都是异步发送的
	if r.maybeCommit() { // 节点配置变更,同步到了大多数节点上
		r.bcastAppend() // 广播消息,触发follower配置变更的落盘
	} else {
		// 否则,仍然探测新添加的副本;没有理由让他们等待心跳间隔(或下一个传入的提议).
		r.prstrack.Visit(func(id uint64, pr *tracker.Progress) {
			r.maybeSendAppend(id, false /* sendIfEmpty */) // 再次发送消息
		})
	}
	// 如果leader被免职或降职,则中止领导权转移.
	if _, tOK := r.prstrack.Config.Voters.IDs()[r.leadTransferee]; !tOK && r.leadTransferee != 0 {
		//  leader转移时;leader转移的目标 不是可投票节点, 要停止领导者转移
		r.abortLeaderTransfer()
	}

	return cs
}
