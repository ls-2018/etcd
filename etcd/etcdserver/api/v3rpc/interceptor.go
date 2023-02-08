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
	"sync"
	"time"

	"github.com/ls-2018/etcd_cn/raft"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api"
	"github.com/ls-2018/etcd_cn/offical/api/v3/v3rpc/rpctypes"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	maxNoLeaderCnt          = 3
	warnUnaryRequestLatency = 300 * time.Millisecond
	snapshotMethod          = "/etcdserverpb.Maintenance/Snapshot"
)

type streamsMap struct {
	mu      sync.Mutex
	streams map[grpc.ServerStream]struct{}
}

func newUnaryInterceptor(s *etcdserver.EtcdServer) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !api.IsCapabilityEnabled(api.V3rpcCapability) { // 是否启用了v3rpc
			return nil, rpctypes.ErrGRPCNotCapable
		}
		// 集群包含自己,自己是learner,
		if s.IsMemberExist(s.ID()) && s.IsLearner() && !isRPCSupportedForLearner(req) {
			return nil, rpctypes.ErrGPRCNotSupportedForLearner
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			// data, _ := json.Marshal(md)
			// s.Logger().Info("-", zap.String("metadata", string(data)))
			// hasleader
			if ks := md[rpctypes.MetadataRequireLeaderKey]; len(ks) > 0 && ks[0] == rpctypes.MetadataHasLeader {
				if s.Leader() == types.ID(raft.None) {
					return nil, rpctypes.ErrGRPCNoLeader
				}
			}
		}

		return handler(ctx, req)
	}
}

func newStreamInterceptor(s *etcdserver.EtcdServer) grpc.StreamServerInterceptor {
	smap := monitorLeader(s)

	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !api.IsCapabilityEnabled(api.V3rpcCapability) {
			return rpctypes.ErrGRPCNotCapable
		}

		if s.IsMemberExist(s.ID()) && s.IsLearner() && info.FullMethod != snapshotMethod { // 除了快照,学习者不支持流RPC
			return rpctypes.ErrGPRCNotSupportedForLearner
		}

		md, ok := metadata.FromIncomingContext(ss.Context())
		if ok {
			ks := md[rpctypes.MetadataRequireLeaderKey] // hasleader
			if len(ks) > 0 && ks[0] == rpctypes.MetadataHasLeader {
				if s.Leader() == types.ID(raft.None) {
					return rpctypes.ErrGRPCNoLeader
				}

				ctx := newCancellableContext(ss.Context())
				ss = serverStreamWithCtx{ctx: ctx, ServerStream: ss}

				smap.mu.Lock()
				smap.streams[ss] = struct{}{}
				smap.mu.Unlock()

				defer func() {
					smap.mu.Lock()
					delete(smap.streams, ss)
					smap.mu.Unlock()
					// TODO: investigate whether the reason for cancellation here is useful to know
					ctx.Cancel(nil)
				}()
			}
		}

		return handler(srv, ss)
	}
}

// cancellableContext wraps a context with new cancellable context that allows a
// specific cancellation error to be preserved and later retrieved using the
// Context.Err() function. This is so downstream context users can disambiguate
// the reason for the cancellation which could be from the client (for example)
// or from this interceptor code.
type cancellableContext struct {
	context.Context

	lock         sync.RWMutex
	cancel       context.CancelFunc
	cancelReason error
}

func newCancellableContext(parent context.Context) *cancellableContext {
	ctx, cancel := context.WithCancel(parent)
	return &cancellableContext{
		Context: ctx,
		cancel:  cancel,
	}
}

// Cancel stores the cancellation reason and then delegates to context.WithCancel
// against the parent context.
func (c *cancellableContext) Cancel(reason error) {
	c.lock.Lock()
	c.cancelReason = reason
	c.lock.Unlock()
	c.cancel()
}

// Err will return the preserved cancel reason error if present, and will
// otherwise return the underlying error from the parent context.
func (c *cancellableContext) Err() error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.cancelReason != nil {
		return c.cancelReason
	}
	return c.Context.Err()
}

type serverStreamWithCtx struct {
	grpc.ServerStream

	// ctx is used so that we can preserve a reason for cancellation.
	ctx *cancellableContext
}

func (ssc serverStreamWithCtx) Context() context.Context { return ssc.ctx }

func monitorLeader(s *etcdserver.EtcdServer) *streamsMap {
	smap := &streamsMap{
		streams: make(map[grpc.ServerStream]struct{}),
	}

	s.GoAttach(func() {
		election := time.Duration(s.Cfg.TickMs) * time.Duration(s.Cfg.ElectionTicks) * time.Millisecond
		noLeaderCnt := 0

		for {
			select {
			case <-s.StoppingNotify():
				return
			case <-time.After(election):
				if s.Leader() == types.ID(raft.None) {
					noLeaderCnt++
				} else {
					noLeaderCnt = 0
				}

				// We are more conservative on canceling existing streams. Reconnecting streams
				// cost much more than just rejecting new requests. So we wait until the member
				// cannot find a leader for maxNoLeaderCnt election timeouts to cancel existing streams.
				if noLeaderCnt >= maxNoLeaderCnt {
					smap.mu.Lock()
					for ss := range smap.streams {
						if ssWithCtx, ok := ss.(serverStreamWithCtx); ok {
							ssWithCtx.ctx.Cancel(rpctypes.ErrGRPCNoLeader)
							<-ss.Context().Done()
						}
					}
					smap.streams = make(map[grpc.ServerStream]struct{})
					smap.mu.Unlock()
				}
			}
		}
	})

	return smap
}
