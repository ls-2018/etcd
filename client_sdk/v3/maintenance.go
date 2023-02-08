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

package clientv3

import (
	"context"
	"fmt"
	"io"

	pb "github.com/ls-2018/etcd_cn/offical/etcdserverpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type (
	DefragmentResponse pb.DefragmentResponse
	AlarmResponse      pb.AlarmResponse
	AlarmMember        pb.AlarmMember
	StatusResponse     pb.StatusResponse
	HashKVResponse     pb.HashKVResponse
	MoveLeaderResponse pb.MoveLeaderResponse
)

type Maintenance interface {
	AlarmList(ctx context.Context) (*AlarmResponse, error)                            // 获取目前所有的警报
	AlarmDisarm(ctx context.Context, m *AlarmMember) (*AlarmResponse, error)          // 解除警报
	Defragment(ctx context.Context, endpoint string) (*DefragmentResponse, error)     // 碎片整理
	Status(ctx context.Context, endpoint string) (*StatusResponse, error)             // 获取端点的状态
	HashKV(ctx context.Context, endpoint string, rev int64) (*HashKVResponse, error)  //
	Snapshot(ctx context.Context) (io.ReadCloser, error)                              // 返回一个快照
	MoveLeader(ctx context.Context, transfereeID uint64) (*MoveLeaderResponse, error) // leader 转移
}

type maintenance struct {
	lg       *zap.Logger
	dial     func(endpoint string) (pb.MaintenanceClient, func(), error)
	remote   pb.MaintenanceClient
	callOpts []grpc.CallOption
}

func NewMaintenance(c *Client) Maintenance {
	api := &maintenance{
		lg: c.lg,
		dial: func(endpoint string) (pb.MaintenanceClient, func(), error) {
			conn, err := c.Dial(endpoint)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to dial endpoint %s with maintenance client: %v", endpoint, err)
			}

			// get token with established connection
			dctx := c.ctx
			cancel := func() {}
			if c.cfg.DialTimeout > 0 {
				dctx, cancel = context.WithTimeout(c.ctx, c.cfg.DialTimeout)
			}
			err = c.getToken(dctx)
			cancel()
			if err != nil {
				return nil, nil, fmt.Errorf("failed to getToken from endpoint %s with maintenance client: %v", endpoint, err)
			}
			cancel = func() { conn.Close() }
			return RetryMaintenanceClient(c, conn), cancel, nil
		},
		remote: RetryMaintenanceClient(c, c.conn),
	}
	if c != nil {
		api.callOpts = c.callOpts
	}
	return api
}

func NewMaintenanceFromMaintenanceClient(remote pb.MaintenanceClient, c *Client) Maintenance {
	api := &maintenance{
		lg: c.lg,
		dial: func(string) (pb.MaintenanceClient, func(), error) {
			return remote, func() {}, nil
		},
		remote: remote,
	}
	if c != nil {
		api.callOpts = c.callOpts
	}
	return api
}

// AlarmList OK
func (m *maintenance) AlarmList(ctx context.Context) (*AlarmResponse, error) {
	req := &pb.AlarmRequest{
		Action:   pb.AlarmRequest_GET,
		MemberID: 0,                 // all
		Alarm:    pb.AlarmType_NONE, // all
	}
	resp, err := m.remote.Alarm(ctx, req, m.callOpts...)
	if err == nil {
		return (*AlarmResponse)(resp), nil
	}
	return nil, toErr(ctx, err)
}

func (m *maintenance) AlarmDisarm(ctx context.Context, am *AlarmMember) (*AlarmResponse, error) {
	req := &pb.AlarmRequest{
		Action:   pb.AlarmRequest_DEACTIVATE,
		MemberID: am.MemberID,
		Alarm:    am.Alarm,
	}

	if req.MemberID == 0 && req.Alarm == pb.AlarmType_NONE {
		ar, err := m.AlarmList(ctx)
		if err != nil {
			return nil, toErr(ctx, err)
		}
		ret := AlarmResponse{}
		for _, am := range ar.Alarms {
			dresp, derr := m.AlarmDisarm(ctx, (*AlarmMember)(am))
			if derr != nil {
				return nil, toErr(ctx, derr)
			}
			ret.Alarms = append(ret.Alarms, dresp.Alarms...)
		}
		return &ret, nil
	}

	resp, err := m.remote.Alarm(ctx, req, m.callOpts...)
	if err == nil {
		return (*AlarmResponse)(resp), nil
	}
	return nil, toErr(ctx, err)
}

// Defragment 碎片整理
func (m *maintenance) Defragment(ctx context.Context, endpoint string) (*DefragmentResponse, error) {
	remote, cancel, err := m.dial(endpoint)
	if err != nil {
		return nil, toErr(ctx, err)
	}
	defer cancel()
	resp, err := remote.Defragment(ctx, &pb.DefragmentRequest{}, m.callOpts...)
	if err != nil {
		return nil, toErr(ctx, err)
	}
	return (*DefragmentResponse)(resp), nil
}

func (m *maintenance) Status(ctx context.Context, endpoint string) (*StatusResponse, error) {
	remote, cancel, err := m.dial(endpoint)
	if err != nil {
		return nil, toErr(ctx, err)
	}
	defer cancel()
	resp, err := remote.Status(ctx, &pb.StatusRequest{}, m.callOpts...)
	if err != nil {
		return nil, toErr(ctx, err)
	}
	return (*StatusResponse)(resp), nil
}

func (m *maintenance) HashKV(ctx context.Context, endpoint string, rev int64) (*HashKVResponse, error) {
	remote, cancel, err := m.dial(endpoint)
	if err != nil {
		return nil, toErr(ctx, err)
	}
	defer cancel()
	resp, err := remote.HashKV(ctx, &pb.HashKVRequest{Revision: rev}, m.callOpts...)
	if err != nil {
		return nil, toErr(ctx, err)
	}
	return (*HashKVResponse)(resp), nil
}

func (m *maintenance) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	ss, err := m.remote.Snapshot(ctx, &pb.SnapshotRequest{}, append(m.callOpts, withMax(defaultStreamMaxRetries))...)
	if err != nil {
		return nil, toErr(ctx, err)
	}

	m.lg.Info("打开快照流;下载ing")
	pr, pw := io.Pipe()
	go func() {
		for {
			resp, err := ss.Recv()
			if err != nil {
				switch err {
				case io.EOF:
					m.lg.Info("快照读取完成;关闭ing")
				default:
					m.lg.Warn("从快照流接收失败;关闭ing", zap.Error(err))
				}
				pw.CloseWithError(err)
				return
			}
			if _, werr := pw.Write(resp.Blob); werr != nil {
				pw.CloseWithError(werr)
				return
			}
		}
	}()
	return &snapshotReadCloser{ctx: ctx, ReadCloser: pr}, nil
}

type snapshotReadCloser struct {
	ctx context.Context
	io.ReadCloser
}

func (rc *snapshotReadCloser) Read(p []byte) (n int, err error) {
	n, err = rc.ReadCloser.Read(p)
	return n, toErr(rc.ctx, err)
}

func (m *maintenance) MoveLeader(ctx context.Context, transfereeID uint64) (*MoveLeaderResponse, error) {
	resp, err := m.remote.MoveLeader(ctx, &pb.MoveLeaderRequest{TargetID: transfereeID}, m.callOpts...)
	return (*MoveLeaderResponse)(resp), toErr(ctx, err)
}
