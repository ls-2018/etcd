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
	"crypto/tls"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Config struct {
	Endpoints            []string      `json:"endpoints"`               // etcd client --> etcd 的地址
	AutoSyncInterval     time.Duration `json:"auto-sync-interval"`      // 是用其最新成员更新端点的时间间隔.0禁止自动同步.默认情况下自动同步被禁用.
	DialTimeout          time.Duration `json:"dial-timeout"`            // 建立链接的超时时间
	DialKeepAliveTime    time.Duration `json:"dial-keep-alive-time"`    // client 向服务端发送发包确保链接存活
	DialKeepAliveTimeout time.Duration `json:"dial-keep-alive-timeout"` // 长时间没有接收到响应关闭链接
	MaxCallSendMsgSize   int           // 默认2MB
	MaxCallRecvMsgSize   int
	TLS                  *tls.Config // 客户端sdk证书
	Username             string      `json:"username"`
	Password             string      `json:"password"`
	RejectOldCluster     bool        `json:"reject-old-cluster"` // 是否拒绝老版本服务器

	// DialOptions is a list of dial options for the grpc client (e.g., for interceptors).
	// For example, pass "grpc.WithBlock()" to block until the underlying connection is up.
	// Without this, Dial returns immediately and connecting the etcd happens in background.
	DialOptions []grpc.DialOption

	// Context is the default client context; it can be used to cancel grpc dial out and
	// other operations that do not have an explicit context.
	Context   context.Context
	Logger    *zap.Logger
	LogConfig *zap.Config

	// PermitWithoutStream when set will allow client to send keepalive pings to etcd without any active streams(RPCs).
	PermitWithoutStream bool `json:"permit-without-stream"`

	// TODO: support custom balancer picker
}
