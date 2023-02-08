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

package etcdserver

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ls-2018/etcd_cn/raft"

	"github.com/ls-2018/etcd_cn/etcd/auth"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/membership"
	"github.com/ls-2018/etcd_cn/etcd/mvcc"
	"github.com/ls-2018/etcd_cn/offical/api/v3/membershippb"
	pb "github.com/ls-2018/etcd_cn/offical/etcdserverpb"
	"github.com/ls-2018/etcd_cn/pkg/traceutil"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
)

const (
	// In the health case, there might backend a small gap (10s of entries) between
	// the applied index and committed index.
	// However, if the committed entries are very heavy to apply, the gap might grow.
	// We should stop accepting new proposals if the gap growing to a certain point.
	maxGapBetweenApplyAndCommitIndex = 5000
	readIndexRetryTime               = 500 * time.Millisecond
)

type Authenticator interface {
	AuthEnable(ctx context.Context, r *pb.AuthEnableRequest) (*pb.AuthEnableResponse, error)
	AuthDisable(ctx context.Context, r *pb.AuthDisableRequest) (*pb.AuthDisableResponse, error)
	AuthStatus(ctx context.Context, r *pb.AuthStatusRequest) (*pb.AuthStatusResponse, error)
	Authenticate(ctx context.Context, r *pb.AuthenticateRequest) (*pb.AuthenticateResponse, error)
	UserAdd(ctx context.Context, r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error)
	UserDelete(ctx context.Context, r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error)
	UserChangePassword(ctx context.Context, r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error)
	UserGrantRole(ctx context.Context, r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error)
	UserGet(ctx context.Context, r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error)
	UserRevokeRole(ctx context.Context, r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error)
	RoleAdd(ctx context.Context, r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error)
	RoleGrantPermission(ctx context.Context, r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error)
	RoleGet(ctx context.Context, r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error)
	RoleRevokePermission(ctx context.Context, r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error)
	RoleDelete(ctx context.Context, r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error)
	UserList(ctx context.Context, r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error)
	RoleList(ctx context.Context, r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error)
}

func isTxnSerializable(r *pb.TxnRequest) bool {
	for _, u := range r.Success {
		if r := u.GetRequestRange(); r == nil || !r.Serializable {
			return false
		}
	}
	for _, u := range r.Failure {
		if r := u.GetRequestRange(); r == nil || !r.Serializable {
			return false
		}
	}
	return true
}

func isTxnReadonly(r *pb.TxnRequest) bool {
	for _, u := range r.Success {
		if r := u.GetRequestRange(); r == nil {
			return false
		}
	}
	for _, u := range r.Failure {
		if r := u.GetRequestRange(); r == nil {
			return false
		}
	}
	return true
}

// Watchable returns a watchable interface attached to the etcdserver.
func (s *EtcdServer) Watchable() mvcc.WatchableKV { return s.KV() }

func isStopped(err error) bool {
	return err == raft.ErrStopped || err == ErrStopped
}

func uint64ToBigEndianBytes(number uint64) []byte {
	byteResult := make([]byte, 8)
	binary.BigEndian.PutUint64(byteResult, number)
	return byteResult
}

func (s *EtcdServer) Downgrade(ctx context.Context, r *pb.DowngradeRequest) (*pb.DowngradeResponse, error) {
	switch r.Action {
	case pb.DowngradeRequest_VALIDATE:
		return s.downgradeValidate(ctx, r.Version)
	case pb.DowngradeRequest_ENABLE:
		return s.downgradeEnable(ctx, r)
	case pb.DowngradeRequest_CANCEL:
		return s.downgradeCancel(ctx)
	default:
		return nil, ErrUnknownMethod
	}
}

func (s *EtcdServer) downgradeValidate(ctx context.Context, v string) (*pb.DowngradeResponse, error) {
	resp := &pb.DowngradeResponse{}

	targetVersion, err := convertToClusterVersion(v)
	if err != nil {
		return nil, err
	}

	// gets leaders commit index and wait for local store to finish applying that index
	// to avoid using stale downgrade information
	err = s.linearizeReadNotify(ctx)
	if err != nil {
		return nil, err
	}

	cv := s.ClusterVersion()
	if cv == nil {
		return nil, ErrClusterVersionUnavailable
	}
	resp.Version = cv.String()

	allowedTargetVersion := membership.AllowedDowngradeVersion(cv)
	if !targetVersion.Equal(*allowedTargetVersion) {
		return nil, ErrInvalidDowngradeTargetVersion
	}

	downgradeInfo := s.cluster.DowngradeInfo()
	if downgradeInfo.Enabled {
		// Todo: return the downgrade status along with the error msg
		return nil, ErrDowngradeInProcess
	}
	return resp, nil
}

func (s *EtcdServer) downgradeEnable(ctx context.Context, r *pb.DowngradeRequest) (*pb.DowngradeResponse, error) {
	// validate downgrade capability before starting downgrade
	v := r.Version
	lg := s.Logger()
	if resp, err := s.downgradeValidate(ctx, v); err != nil {
		lg.Warn("reject downgrade request", zap.Error(err))
		return resp, err
	}
	targetVersion, err := convertToClusterVersion(v)
	if err != nil {
		lg.Warn("reject downgrade request", zap.Error(err))
		return nil, err
	}

	raftRequest := membershippb.DowngradeInfoSetRequest{Enabled: true, Ver: targetVersion.String()}
	_, err = s.raftRequest(ctx, pb.InternalRaftRequest{DowngradeInfoSet: &raftRequest})
	if err != nil {
		lg.Warn("reject downgrade request", zap.Error(err))
		return nil, err
	}
	resp := pb.DowngradeResponse{Version: s.ClusterVersion().String()}
	return &resp, nil
}

func (s *EtcdServer) downgradeCancel(ctx context.Context) (*pb.DowngradeResponse, error) {
	// gets leaders commit index and wait for local store to finish applying that index
	// to avoid using stale downgrade information
	if err := s.linearizeReadNotify(ctx); err != nil {
		return nil, err
	}

	downgradeInfo := s.cluster.DowngradeInfo()
	if !downgradeInfo.Enabled {
		return nil, ErrNoInflightDowngrade
	}

	raftRequest := membershippb.DowngradeInfoSetRequest{Enabled: false}
	_, err := s.raftRequest(ctx, pb.InternalRaftRequest{DowngradeInfoSet: &raftRequest})
	if err != nil {
		return nil, err
	}
	resp := pb.DowngradeResponse{Version: s.ClusterVersion().String()}
	return &resp, nil
}

// ----------------------------------------   OVER  ------------------------------------------------------------

// AuthInfoFromCtx 获取认证信息
func (s *EtcdServer) AuthInfoFromCtx(ctx context.Context) (*auth.AuthInfo, error) {
	authInfo, err := s.AuthStore().AuthInfoFromCtx(ctx) // 用户认证
	if authInfo != nil || err != nil {
		return authInfo, err
	}
	if !s.Cfg.ClientCertAuthEnabled { // 是否验证客户端证书
		return nil, nil
	}
	authInfo = s.AuthStore().AuthInfoFromTLS(ctx)
	return authInfo, nil
}

// doSerialize 为序列化的请求“get"处理认证逻辑,并由“chk"检查权限.身份验证失败时返回一个非空错误.
func (s *EtcdServer) doSerialize(ctx context.Context, chk func(*auth.AuthInfo) error, get func()) error {
	trace := traceutil.Get(ctx) // 从上下文获取trace
	ai, err := s.AuthInfoFromCtx(ctx)
	if err != nil {
		return err
	}
	if ai == nil {
		// chk期望非nil AuthInfo;使用空的凭证
		ai = &auth.AuthInfo{}
	}
	// 检查权限
	if err = chk(ai); err != nil {
		return err
	}
	trace.Step("获取认证元数据")
	// 获取序列化请求的响应
	get()
	// 如果在处理请求时更新了身份验证存储,请检查过时的令牌修订情况.
	if ai.Revision != 0 && ai.Revision != s.authStore.Revision() {
		// 节点在 Apply 流程的时候,会判断 Raft 日志条目中的请求鉴权版本号是否小于当前鉴权版本号,如果小于就拒绝写入.
		// 请求认证的版本小于当前节点认证的版本
		return auth.ErrAuthOldRevision
	}
	return nil
}

// OK 对外提供的接口
func (s *EtcdServer) raftRequest(ctx context.Context, r pb.InternalRaftRequest) (proto.Message, error) {
	return s.raftRequestOnce(ctx, r)
}

// ok
func (s *EtcdServer) raftRequestOnce(ctx context.Context, r pb.InternalRaftRequest) (proto.Message, error) {
	result, err := s.processInternalRaftRequestOnce(ctx, r)
	if err != nil {
		return nil, err
	}
	if result.err != nil {
		return nil, result.err
	}
	// startTime
	startTime, ok := ctx.Value(traceutil.StartTimeKey).(time.Time)
	if ok && result.trace != nil {
		applyStart := result.trace.GetStartTime()
		result.trace.SetStartTime(startTime)
		result.trace.InsertStep(0, applyStart, "处理raft请求")
	}
	marshal, _ := json.Marshal(result.trace)
	fmt.Println("trace--->", string(marshal))
	return result.resp, nil
}

// 当客户端提交一条数据变更请求时
func (s *EtcdServer) processInternalRaftRequestOnce(ctx context.Context, r pb.InternalRaftRequest) (*applyResult, error) {
	// 判断已提交未apply的记录是否超过限制
	ai := s.getAppliedIndex()
	ci := s.getCommittedIndex()
	if ci > ai+maxGapBetweenApplyAndCommitIndex {
		return nil, ErrTooManyRequests
	}

	r.Header = &pb.RequestHeader{
		ID: s.reqIDGen.Next(), // 生成一个requestID
	}

	// 检查authinfo是否不是InternalAuthenticateRequest
	if r.Authenticate == nil {
		authInfo, err := s.AuthInfoFromCtx(ctx)
		if err != nil {
			return nil, err
		}
		if authInfo != nil {
			r.Header.Username = authInfo.Username
			r.Header.AuthRevision = authInfo.Revision
		}
	}
	// 反序列化请求数据

	data, err := r.Marshal()
	if err != nil {
		return nil, err
	}

	if len(data) > int(s.Cfg.MaxRequestBytes) {
		return nil, ErrRequestTooLarge
	}

	id := r.ID // 0
	if id == 0 {
		id = r.Header.ID
	}
	ch := s.w.Register(id) // 注册一个channel,等待处理完成

	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout()) // 设置请求超时
	// cctx, cancel := context.WithTimeout(ctx, time.Second*1000) // 设置请求超时
	defer cancel()

	start := time.Now()
	_ = s.applyEntryNormal
	err = s.r.Propose(cctx, data) // 调用raft模块的Propose处理请求,存入到了待发送队列
	if err != nil {
		s.w.Trigger(id, nil)
		return nil, err
	}

	select {
	// 等待收到apply结果返回给客户端
	case x := <-ch:
		return x.(*applyResult), nil
	case <-cctx.Done():
		s.w.Trigger(id, nil)
		return nil, s.parseProposeCtxErr(cctx.Err(), start)
	case <-s.done:
		return nil, ErrStopped
	}
}

// Apply 入口函数,负责处理内部的消息
func (a *applierV3backend) Apply(r *pb.InternalRaftRequest, shouldApplyV3 membership.ShouldApplyV3) *applyResult {
	ar := &applyResult{}
	defer func(start time.Time) {
		success := ar.err == nil || ar.err == mvcc.ErrCompacted
		if !success {
			warnOfFailedRequest(a.s.Logger(), start, &pb.InternalRaftStringer{Request: r}, ar.resp, ar.err)
		}
	}(time.Now())

	switch {
	case r.ClusterVersionSet != nil: //     3.5.x 实现
		// 设置集群版本
		a.s.applyV3Internal.ClusterVersionSet(r.ClusterVersionSet, shouldApplyV3)
		return nil
	case r.ClusterMemberAttrSet != nil:
		// 集群成员属性
		a.s.applyV3Internal.ClusterMemberAttrSet(r.ClusterMemberAttrSet, shouldApplyV3)
		return nil
	case r.DowngradeInfoSet != nil:
		// 成员降级
		a.s.applyV3Internal.DowngradeInfoSet(r.DowngradeInfoSet, shouldApplyV3)
		return nil
	}

	if !shouldApplyV3 {
		return nil
	}

	switch {
	case r.Range != nil:
		ar.resp, ar.err = a.s.applyV3.Range(context.TODO(), nil, r.Range) // ✅
	case r.Put != nil:
		ar.resp, ar.trace, ar.err = a.s.applyV3.Put(context.TODO(), nil, r.Put) // ✅
	case r.DeleteRange != nil:
		ar.resp, ar.err = a.s.applyV3.DeleteRange(nil, r.DeleteRange) // ✅
	case r.Txn != nil:
		ar.resp, ar.trace, ar.err = a.s.applyV3.Txn(context.TODO(), r.Txn)
	case r.Compaction != nil:
		ar.resp, ar.physc, ar.trace, ar.err = a.s.applyV3.Compaction(r.Compaction) // ✅ 压缩kv 历史事件
	case r.LeaseGrant != nil:
		ar.resp, ar.err = a.s.applyV3.LeaseGrant(r.LeaseGrant) // ✅ 创建租约
	case r.LeaseRevoke != nil:
		ar.resp, ar.err = a.s.applyV3.LeaseRevoke(r.LeaseRevoke) // ✅ 删除租约
	case r.LeaseCheckpoint != nil:
		// 避免 leader 变更时,导致的租约重置
		ar.resp, ar.err = a.s.applyV3.LeaseCheckpoint(r.LeaseCheckpoint) // ✅
	case r.Alarm != nil:
		ar.resp, ar.err = a.s.applyV3.Alarm(r.Alarm) // ✅
	case r.Authenticate != nil:
		ar.resp, ar.err = a.s.applyV3.Authenticate(r.Authenticate) // ✅
	case r.AuthEnable != nil:
		ar.resp, ar.err = a.s.applyV3.AuthEnable() // ✅
	case r.AuthDisable != nil:
		ar.resp, ar.err = a.s.applyV3.AuthDisable() // ✅
	case r.AuthStatus != nil:
		ar.resp, ar.err = a.s.applyV3.AuthStatus() // ✅
	case r.AuthUserAdd != nil:
		ar.resp, ar.err = a.s.applyV3.UserAdd(r.AuthUserAdd) // ✅
	case r.AuthUserDelete != nil:
		ar.resp, ar.err = a.s.applyV3.UserDelete(r.AuthUserDelete) // ✅
	case r.AuthUserChangePassword != nil:
		ar.resp, ar.err = a.s.applyV3.UserChangePassword(r.AuthUserChangePassword) // ✅
	case r.AuthUserGrantRole != nil:
		ar.resp, ar.err = a.s.applyV3.UserGrantRole(r.AuthUserGrantRole) // ✅
	case r.AuthUserGet != nil:
		ar.resp, ar.err = a.s.applyV3.UserGet(r.AuthUserGet) // ✅
	case r.AuthUserRevokeRole != nil:
		ar.resp, ar.err = a.s.applyV3.UserRevokeRole(r.AuthUserRevokeRole) // ✅
	case r.AuthUserList != nil:
		ar.resp, ar.err = a.s.applyV3.UserList(r.AuthUserList) // ✅
	case r.AuthRoleAdd != nil:
		ar.resp, ar.err = a.s.applyV3.RoleAdd(r.AuthRoleAdd) // ✅
	case r.AuthRoleGrantPermission != nil:
		ar.resp, ar.err = a.s.applyV3.RoleGrantPermission(r.AuthRoleGrantPermission) // ✅
	case r.AuthRoleGet != nil:
		ar.resp, ar.err = a.s.applyV3.RoleGet(r.AuthRoleGet) // ✅
	case r.AuthRoleRevokePermission != nil:
		ar.resp, ar.err = a.s.applyV3.RoleRevokePermission(r.AuthRoleRevokePermission) // ✅
	case r.AuthRoleDelete != nil:
		ar.resp, ar.err = a.s.applyV3.RoleDelete(r.AuthRoleDelete) // ✅
	case r.AuthRoleList != nil:
		ar.resp, ar.err = a.s.applyV3.RoleList(r.AuthRoleList) // ✅
	default:
		a.s.lg.Panic("没有实现应用", zap.Stringer("raft-request", r))
	}
	return ar
}

// 等待leader就绪
func (s *EtcdServer) waitLeader(ctx context.Context) (*membership.Member, error) {
	leader := s.cluster.Member(s.Leader())
	for leader == nil {
		// 等待选举超时
		dur := time.Duration(s.Cfg.ElectionTicks) * time.Duration(s.Cfg.TickMs) * time.Millisecond
		select {
		case <-time.After(dur):
			leader = s.cluster.Member(s.Leader())
		case <-s.stopping:
			return nil, ErrStopped
		case <-ctx.Done():
			return nil, ErrNoLeader
		}
	}
	if leader == nil || len(leader.PeerURLs) == 0 {
		return nil, ErrNoLeader
	}
	return leader, nil
}

// Alarm 发送警报信息
func (s *EtcdServer) Alarm(ctx context.Context, r *pb.AlarmRequest) (*pb.AlarmResponse, error) {
	req := pb.InternalRaftRequest{Alarm: r}
	// marshal, _ := json.Marshal(req)
	// fmt.Println("marshal-->",string(marshal))
	resp, err := s.raftRequestOnce(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AlarmResponse), nil
}
