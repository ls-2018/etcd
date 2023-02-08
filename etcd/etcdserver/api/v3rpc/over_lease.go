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
	"io"

	"github.com/ls-2018/etcd_cn/etcd/etcdserver"
	"github.com/ls-2018/etcd_cn/etcd/lease"
	"github.com/ls-2018/etcd_cn/offical/api/v3/v3rpc/rpctypes"
	pb "github.com/ls-2018/etcd_cn/offical/etcdserverpb"

	"go.uber.org/zap"
)

type LeaseServer struct {
	lg  *zap.Logger
	hdr header
	le  etcdserver.Lessor
}

func NewLeaseServer(s *etcdserver.EtcdServer) pb.LeaseServer {
	srv := &LeaseServer{lg: s.Cfg.Logger, le: s, hdr: newHeader(s)}
	if srv.lg == nil {
		srv.lg = zap.NewNop()
	}
	return srv
}

func (ls *LeaseServer) LeaseTimeToLive(ctx context.Context, rr *pb.LeaseTimeToLiveRequest) (*pb.LeaseTimeToLiveResponse, error) {
	resp, err := ls.le.LeaseTimeToLive(ctx, rr)
	if err != nil && err != lease.ErrLeaseNotFound {
		return nil, togRPCError(err)
	}
	if err == lease.ErrLeaseNotFound {
		resp = &pb.LeaseTimeToLiveResponse{
			Header: &pb.ResponseHeader{},
			ID:     rr.ID,
			TTL:    -1,
		}
	}
	ls.hdr.fill(resp.Header)
	return resp, nil
}

// LeaseLeases 获取当前节点上的所有租约
func (ls *LeaseServer) LeaseLeases(ctx context.Context, rr *pb.LeaseLeasesRequest) (*pb.LeaseLeasesResponse, error) {
	resp, err := ls.le.LeaseLeases(ctx, rr)
	if err != nil && err != lease.ErrLeaseNotFound {
		return nil, togRPCError(err)
	}
	if err == lease.ErrLeaseNotFound {
		resp = &pb.LeaseLeasesResponse{
			Header: &pb.ResponseHeader{},
			Leases: []*pb.LeaseStatus{},
		}
	}
	ls.hdr.fill(resp.Header)
	return resp, nil
}

// LeaseKeepAlive OK
func (ls *LeaseServer) LeaseKeepAlive(stream pb.Lease_LeaseKeepAliveServer) (err error) {
	errc := make(chan error, 1)
	go func() {
		errc <- ls.leaseKeepAlive(stream)
	}()
	select {
	case err = <-errc:
	case <-stream.Context().Done():
		err = stream.Context().Err()
		if err == context.Canceled {
			err = rpctypes.ErrGRPCNoLeader
		}
	}
	return err
}

func (ls *LeaseServer) leaseKeepAlive(stream pb.Lease_LeaseKeepAliveServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if isClientCtxErr(stream.Context().Err(), err) {
				ls.lg.Debug("从gRPC流获取lease keepalive请求失败", zap.Error(err))
			} else {
				ls.lg.Warn("从gRPC流获取lease keepalive请求失败", zap.Error(err))
			}
			return err
		}

		// 在发送更新请求之前创建报头.这可以确保修订严格小于或等于本地etcd(当本地etcd是leader时)或远端leader发生keepalive.
		// 如果没有这个,租约可能在rev 3被撤销,但客户端可以看到在rev 4成功的keepalive.
		resp := &pb.LeaseKeepAliveResponse{ID: req.ID, Header: &pb.ResponseHeader{}}
		ls.hdr.fill(resp.Header)

		ttl, err := ls.le.LeaseRenew(stream.Context(), lease.LeaseID(req.ID))
		if err == lease.ErrLeaseNotFound {
			err = nil
			ttl = 0
		}

		if err != nil {
			return togRPCError(err)
		}

		resp.TTL = ttl
		err = stream.Send(resp)
		if err != nil {
			if isClientCtxErr(stream.Context().Err(), err) {
				ls.lg.Debug("往grpc Stream发送lease Keepalive响应失败", zap.Error(err))
			} else {
				ls.lg.Warn("往grpc Stream发送lease Keepalive响应失败", zap.Error(err))
			}
			return err
		}
	}
}

type quotaLeaseServer struct {
	pb.LeaseServer
	qa quotaAlarmer
}

// LeaseGrant 创建租约
func (s *quotaLeaseServer) LeaseGrant(ctx context.Context, cr *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	if err := s.qa.check(ctx, cr); err != nil { // 检查存储空间是否还有空余,以及抛出警报
		return nil, err
	}
	return s.LeaseServer.LeaseGrant(ctx, cr)
}

// LeaseGrant 创建租约
func (ls *LeaseServer) LeaseGrant(ctx context.Context, cr *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	resp, err := ls.le.LeaseGrant(ctx, cr)
	if err != nil {
		return nil, togRPCError(err)
	}
	ls.hdr.fill(resp.Header)
	return resp, nil
}

func NewQuotaLeaseServer(s *etcdserver.EtcdServer) pb.LeaseServer {
	return &quotaLeaseServer{
		NewLeaseServer(s),
		quotaAlarmer{etcdserver.NewBackendQuota(s, "lease"), s, s.ID()},
	}
}

// LeaseRevoke OK
func (ls *LeaseServer) LeaseRevoke(ctx context.Context, rr *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	resp, err := ls.le.LeaseRevoke(ctx, rr)
	if err != nil {
		return nil, togRPCError(err)
	}
	ls.hdr.fill(resp.Header)
	return resp, nil
}
