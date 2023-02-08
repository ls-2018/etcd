// Copyright 2017 The etcd Authors
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
	"math"
	"time"

	"google.golang.org/grpc"
)

var (
	// client-side handling retrying of request failures where data was not written to the wire or
	// where etcd indicates it did not process the data. gRPC default is default is "WaitForReady(false)"
	// but for etcd we default to "WaitForReady(true)" to minimize client request error responses due to
	// transient failures.
	defaultWaitForReady            = grpc.WaitForReady(true)
	defaultMaxCallSendMsgSize      = grpc.MaxCallSendMsgSize(2 * 1024 * 1024)
	defaultMaxCallRecvMsgSize      = grpc.MaxCallRecvMsgSize(math.MaxInt32)
	defaultUnaryMaxRetries    uint = 100
	defaultStreamMaxRetries        = ^uint(0)              // max uint
	defaultBackoffWaitBetween      = 25 * time.Millisecond // 重试间隔

	// client-side retry backoff default jitter fraction.
	defaultBackoffJitterFraction = 0.10
)

// "clientv3.Config" 默认的 "gRPC.CallOption".
var defaultCallOpts = []grpc.CallOption{
	defaultWaitForReady,
	defaultMaxCallSendMsgSize,
	defaultMaxCallRecvMsgSize,
}
