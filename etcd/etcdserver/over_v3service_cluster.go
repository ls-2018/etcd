package etcdserver

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/membership"
	"github.com/ls-2018/etcd_cn/raft/raftpb"
	"go.uber.org/zap"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
)

// LinearizableReadNotify 一致性读
func (s *EtcdServer) LinearizableReadNotify(ctx context.Context) error {
	return s.linearizeReadNotify(ctx)
}

// AddMember ok
func (s *EtcdServer) AddMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error) {
	if err := s.checkMembershipOperationPermission(ctx); err != nil {
		return nil, err
	}

	b, err := json.Marshal(memb)
	if err != nil {
		return nil, err
	}

	lg := s.Logger()
	// 默认情况下,StrictReconfigCheck是启用的;拒绝不健康的新成员.
	if !s.Cfg.StrictReconfigCheck {
	} else {
		// 添加投票成员时保护法定人数
		if !memb.IsLearner && !s.cluster.IsReadyToAddVotingMember() {
			lg.Warn("拒绝成员添加申请;健康成员个数不足", zap.String("local-member-id", s.ID().String()), zap.String("requested-member-add", fmt.Sprintf("%+v", memb)),
				zap.Error(ErrNotEnoughStartedMembers),
			)
			return nil, ErrNotEnoughStartedMembers
		}
		// 一个心跳间隔之前,是否与所有节点建立了链接
		if !isConnectedFullySince(s.r.transport, time.Now().Add(-HealthInterval), s.ID(), s.cluster.VotingMembers()) {
			lg.Warn(
				"拒绝成员添加请求;本地成员尚未连接到所有对等体,请重新配置中断活动仲裁",
				zap.String("local-member-id", s.ID().String()),
				zap.String("requested-member-add", fmt.Sprintf("%+v", memb)),
				zap.Error(ErrUnhealthy),
			)
			return nil, ErrUnhealthy
		}
	}
	cc := raftpb.ConfChangeV1{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  uint64(memb.ID),
		Context: string(b),
	}

	if memb.IsLearner {
		cc.Type = raftpb.ConfChangeAddLearnerNode
	}

	return s.configureAndSendRaft(ctx, cc)
}

// RemoveMember ok
func (s *EtcdServer) RemoveMember(ctx context.Context, id uint64) ([]*membership.Member, error) {
	if err := s.checkMembershipOperationPermission(ctx); err != nil {
		return nil, err
	}

	if err := s.mayRemoveMember(types.ID(id)); err != nil {
		return nil, err
	}

	cc := raftpb.ConfChangeV1{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: id,
	}
	return s.configureAndSendRaft(ctx, cc)
}

// UpdateMember ok
func (s *EtcdServer) UpdateMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error) {
	b, merr := json.Marshal(memb)
	if merr != nil {
		return nil, merr
	}

	if err := s.checkMembershipOperationPermission(ctx); err != nil {
		return nil, err
	}
	cc := raftpb.ConfChangeV1{
		Type:    raftpb.ConfChangeUpdateNode,
		NodeID:  uint64(memb.ID),
		Context: string(b),
	}
	return s.configureAndSendRaft(ctx, cc)
}

// PromoteMember 将learner节点提升为voter
func (s *EtcdServer) PromoteMember(ctx context.Context, id uint64) ([]*membership.Member, error) {
	// 只有raft leader有信息,知道learner是否准备好.
	resp, err := s.promoteMember(ctx, id) // raft已经同步消息了
	if err == nil {
		return resp, nil
	}
	if err != ErrNotLeader {
		return resp, err
	}

	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()
	// 转发到leader
	for cctx.Err() == nil {
		leader, err := s.waitLeader(cctx)
		if err != nil {
			return nil, err
		}
		for _, url := range leader.PeerURLs {
			resp, err := promoteMemberHTTP(cctx, url, id, s.peerRt)
			if err == nil {
				return resp, nil
			}
			if err == ErrLearnerNotReady || err == membership.ErrIDNotFound || err == membership.ErrMemberNotLearner {
				return nil, err
			}
		}
	}

	if cctx.Err() == context.DeadlineExceeded {
		return nil, ErrTimeout
	}
	return nil, ErrCanceled
}

// promoteMember
func (s *EtcdServer) promoteMember(ctx context.Context, id uint64) ([]*membership.Member, error) {
	if err := s.checkMembershipOperationPermission(ctx); err != nil {
		return nil, err
	}

	if err := s.mayPromoteMember(types.ID(id)); err != nil {
		return nil, err
	}

	promoteChangeContext := membership.ConfigChangeContext{
		Member: membership.Member{
			ID: types.ID(id),
		},
		IsPromote: true,
	}

	b, err := json.Marshal(promoteChangeContext)
	if err != nil {
		return nil, err
	}

	cc := raftpb.ConfChangeV1{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  id,
		Context: string(b),
	}

	return s.configureAndSendRaft(ctx, cc)
}

// OK
func (s *EtcdServer) mayPromoteMember(id types.ID) error {
	lg := s.Logger()
	err := s.isLearnerReady(uint64(id)) // 检查learner的同步的数据有没有打到90%
	if err != nil {
		return err
	}

	if !s.Cfg.StrictReconfigCheck { // 严格配置变更检查
		return nil
	}
	if !s.cluster.IsReadyToPromoteMember(uint64(id)) {
		lg.Warn("拒绝成员提升申请;健康成员个数不足", zap.String("local-member-id", s.ID().String()),
			zap.String("requested-member-remove-id", id.String()),
			zap.Error(ErrNotEnoughStartedMembers),
		)
		return ErrNotEnoughStartedMembers
	}

	return nil
}
