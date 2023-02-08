// Copyright 2016 The etcd Authors
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

package v3rpc

import (
	"context"
	"crypto/sha256"
	"io"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/ls-2018/etcd_cn/raft"

	"github.com/ls-2018/etcd_cn/etcd/auth"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver"
	"github.com/ls-2018/etcd_cn/etcd/mvcc"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"
	"github.com/ls-2018/etcd_cn/offical/api/v3/v3rpc/rpctypes"
	"github.com/ls-2018/etcd_cn/offical/api/v3/version"
	pb "github.com/ls-2018/etcd_cn/offical/etcdserverpb"

	"go.uber.org/zap"
)

type KVGetter interface {
	KV() mvcc.WatchableKV
}

type BackendGetter interface {
	Backend() backend.Backend
}

type Alarmer interface {
	Alarms() []*pb.AlarmMember
	Alarm(ctx context.Context, ar *pb.AlarmRequest) (*pb.AlarmResponse, error)
}

type Downgrader interface {
	Downgrade(ctx context.Context, dr *pb.DowngradeRequest) (*pb.DowngradeResponse, error)
}

type LeaderTransferrer interface {
	MoveLeader(ctx context.Context, lead, target uint64) error
}

type AuthGetter interface {
	AuthInfoFromCtx(ctx context.Context) (*auth.AuthInfo, error)
	AuthStore() auth.AuthStore
}

type ClusterStatusGetter interface {
	IsLearner() bool
}

type maintenanceServer struct {
	lg  *zap.Logger
	rg  etcdserver.RaftStatusGetter // 获取raft状态
	kg  KVGetter
	bg  BackendGetter
	a   Alarmer
	lt  LeaderTransferrer
	hdr header
	cs  ClusterStatusGetter
	d   Downgrader
}

func NewMaintenanceServer(s *etcdserver.EtcdServer) pb.MaintenanceServer {
	srv := &maintenanceServer{lg: s.Cfg.Logger, rg: s, kg: s, bg: s, a: s, lt: s, hdr: newHeader(s), cs: s, d: s}
	if srv.lg == nil {
		srv.lg = zap.NewNop()
	}
	return &authMaintenanceServer{srv, s}
}

// Defragment 碎片整理
func (ms *maintenanceServer) Defragment(ctx context.Context, sr *pb.DefragmentRequest) (*pb.DefragmentResponse, error) {
	ms.lg.Info("开始 碎片整理")
	err := ms.bg.Backend().Defrag()
	if err != nil {
		ms.lg.Warn("碎片整理是啊比", zap.Error(err))
		return nil, err
	}
	ms.lg.Info("结束 碎片整理")
	return &pb.DefragmentResponse{}, nil
}

// big enough size to hold >1 OS pages in the buffer
const snapshotSendBufferSize = 32 * 1024

// MoveLeader OK
func (ms *maintenanceServer) MoveLeader(ctx context.Context, tr *pb.MoveLeaderRequest) (*pb.MoveLeaderResponse, error) {
	if ms.rg.ID() != ms.rg.Leader() {
		return nil, rpctypes.ErrGRPCNotLeader
	}

	if err := ms.lt.MoveLeader(ctx, uint64(ms.rg.Leader()), tr.TargetID); err != nil {
		return nil, togRPCError(err)
	}
	return &pb.MoveLeaderResponse{}, nil
}

func (ms *maintenanceServer) Downgrade(ctx context.Context, r *pb.DowngradeRequest) (*pb.DowngradeResponse, error) {
	resp, err := ms.d.Downgrade(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	resp.Header = &pb.ResponseHeader{}
	ms.hdr.fill(resp.Header)
	return resp, nil
}

type authMaintenanceServer struct {
	*maintenanceServer
	ag AuthGetter
}

func (ams *authMaintenanceServer) isAuthenticated(ctx context.Context) error {
	authInfo, err := ams.ag.AuthInfoFromCtx(ctx)
	if err != nil {
		return err
	}

	return ams.ag.AuthStore().IsAdminPermitted(authInfo)
}

func (ams *authMaintenanceServer) Defragment(ctx context.Context, sr *pb.DefragmentRequest) (*pb.DefragmentResponse, error) {
	if err := ams.isAuthenticated(ctx); err != nil {
		return nil, err
	}

	return ams.maintenanceServer.Defragment(ctx, sr)
}

func (ams *authMaintenanceServer) Hash(ctx context.Context, r *pb.HashRequest) (*pb.HashResponse, error) {
	if err := ams.isAuthenticated(ctx); err != nil {
		return nil, err
	}

	return ams.maintenanceServer.Hash(ctx, r)
}

func (ams *authMaintenanceServer) HashKV(ctx context.Context, r *pb.HashKVRequest) (*pb.HashKVResponse, error) {
	if err := ams.isAuthenticated(ctx); err != nil {
		return nil, err
	}
	return ams.maintenanceServer.HashKV(ctx, r)
}

func (ams *authMaintenanceServer) Status(ctx context.Context, ar *pb.StatusRequest) (*pb.StatusResponse, error) {
	return ams.maintenanceServer.Status(ctx, ar)
}

func (ams *authMaintenanceServer) MoveLeader(ctx context.Context, tr *pb.MoveLeaderRequest) (*pb.MoveLeaderResponse, error) {
	return ams.maintenanceServer.MoveLeader(ctx, tr)
}

func (ams *authMaintenanceServer) Downgrade(ctx context.Context, r *pb.DowngradeRequest) (*pb.DowngradeResponse, error) {
	return ams.maintenanceServer.Downgrade(ctx, r)
}

// ------------------------------------  OVER ---------------------------------------------------------------

// Alarm ok
func (ms *maintenanceServer) Alarm(ctx context.Context, ar *pb.AlarmRequest) (*pb.AlarmResponse, error) {
	resp, err := ms.a.Alarm(ctx, ar)
	if err != nil {
		return nil, togRPCError(err)
	}
	if resp.Header == nil {
		resp.Header = &pb.ResponseHeader{}
	}
	ms.hdr.fill(resp.Header)
	return resp, nil
}

// Status ok
func (ms *maintenanceServer) Status(ctx context.Context, ar *pb.StatusRequest) (*pb.StatusResponse, error) {
	hdr := &pb.ResponseHeader{}
	ms.hdr.fill(hdr)
	resp := &pb.StatusResponse{
		Header:           hdr,
		Version:          version.Version,
		Leader:           uint64(ms.rg.Leader()),
		RaftIndex:        ms.rg.CommittedIndex(),
		RaftAppliedIndex: ms.rg.AppliedIndex(),
		RaftTerm:         ms.rg.Term(),
		DbSize:           ms.bg.Backend().Size(),
		DbSizeInUse:      ms.bg.Backend().SizeInUse(),
		IsLearner:        ms.cs.IsLearner(),
	}
	if resp.Leader == raft.None {
		resp.Errors = append(resp.Errors, etcdserver.ErrNoLeader.Error())
	}
	for _, a := range ms.a.Alarms() {
		resp.Errors = append(resp.Errors, a.String())
	}
	return resp, nil
}

// Snapshot 获取一个快照
func (ams *authMaintenanceServer) Snapshot(sr *pb.SnapshotRequest, srv pb.Maintenance_SnapshotServer) error {
	if err := ams.isAuthenticated(srv.Context()); err != nil {
		return err
	}

	return ams.maintenanceServer.Snapshot(sr, srv)
}

// Snapshot 获取一个快照
func (ms *maintenanceServer) Snapshot(sr *pb.SnapshotRequest, srv pb.Maintenance_SnapshotServer) error {
	snap := ms.bg.Backend().Snapshot() // 快照结构体, 初始化 发送超时
	pr, pw := io.Pipe()
	defer pr.Close()

	go func() {
		snap.WriteTo(pw)
		if err := snap.Close(); err != nil {
			ms.lg.Warn("关闭快照失败", zap.Error(err))
		}
		pw.Close()
	}()

	// record快照恢复操作中用于完整性检查的快照数据的SHA摘要
	h := sha256.New()

	sent := int64(0)
	total := snap.Size()
	size := humanize.Bytes(uint64(total))

	start := time.Now()
	ms.lg.Info("往客户端发送一个快照", zap.Int64("total-bytes", total), zap.String("size", size))
	for total-sent > 0 {
		// buffer只保存从流中读取的字节,响应大小是OS页面大小的倍数,从boltdb中获取
		// 例如4 * 1024
		// Send并不等待消息被客户端接收.因此,在Send操作之间不能安全地重用缓冲区

		buf := make([]byte, snapshotSendBufferSize)

		n, err := io.ReadFull(pr, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return togRPCError(err)
		}
		sent += int64(n)

		// 如果total是x * snapshotSendBufferSize.这种反应是有可能的.RemainingBytes = = 0
		// 分别地.这是否使etcd响应发送到客户端nil在原型和客户端停止接收从快照流之前etcd发送快照SHA?
		// 不,客户端仍然会收到非nil响应,直到etcd用EOF关闭流
		resp := &pb.SnapshotResponse{
			RemainingBytes: uint64(total - sent),
			Blob:           buf[:n],
		}
		if err = srv.Send(resp); err != nil {
			return togRPCError(err)
		}
		h.Write(buf[:n])
	}

	sha := h.Sum(nil)

	ms.lg.Info("往客户端发送一个快照校验和", zap.Int64("total-bytes", total), zap.Int("checksum-size", len(sha)))
	hresp := &pb.SnapshotResponse{RemainingBytes: 0, Blob: sha}
	if err := srv.Send(hresp); err != nil {
		return togRPCError(err)
	}

	ms.lg.Info("成功发送快照到客户端", zap.Int64("total-bytes", total),
		zap.String("size", size),
		zap.String("took", humanize.Time(start)),
	)
	return nil
}

// Hash ok
func (ms *maintenanceServer) Hash(ctx context.Context, r *pb.HashRequest) (*pb.HashResponse, error) {
	h, rev, err := ms.kg.KV().Hash()
	if err != nil {
		return nil, togRPCError(err)
	}
	resp := &pb.HashResponse{Header: &pb.ResponseHeader{Revision: rev}, Hash: h}
	ms.hdr.fill(resp.Header)
	return resp, nil
}

// HashKV OK
func (ms *maintenanceServer) HashKV(ctx context.Context, r *pb.HashKVRequest) (*pb.HashKVResponse, error) {
	h, rev, compactRev, err := ms.kg.KV().HashByRev(r.Revision)
	if err != nil {
		return nil, togRPCError(err)
	}

	resp := &pb.HashKVResponse{Header: &pb.ResponseHeader{Revision: rev}, Hash: h, CompactRevision: compactRev}
	ms.hdr.fill(resp.Header)
	return resp, nil
}
