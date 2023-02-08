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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ls-2018/etcd_cn/client_sdk/v3/credentials"
	"github.com/ls-2018/etcd_cn/client_sdk/v3/internal/endpoint"
	"github.com/ls-2018/etcd_cn/client_sdk/v3/internal/resolver"
	"github.com/ls-2018/etcd_cn/offical/api/v3/v3rpc/rpctypes"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpccredentials "google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

var (
	ErrNoAvailableEndpoints = errors.New("etcdclient: 端点不可用")
	ErrOldCluster           = errors.New("etcdclient: 旧的集群版本")
)

// Client 提供并管理一个etcd v3客户端会话.
type Client struct {
	Cluster
	KV
	Lease
	Watcher
	Auth
	Maintenance
	conn            *grpc.ClientConn
	cfg             Config                               // 配置信息
	creds           grpccredentials.TransportCredentials // 证书信息
	resolver        *resolver.EtcdManualResolver
	mu              *sync.RWMutex
	ctx             context.Context    // 上下文
	cancel          context.CancelFunc // 上下文 cancel func
	Username        string
	Password        string
	authTokenBundle credentials.Bundle
	callOpts        []grpc.CallOption
	lgMu            *sync.RWMutex
	lg              *zap.Logger
}

// New 创建一个client用于与etcd server 通信
func New(cfg Config) (*Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, ErrNoAvailableEndpoints
	}

	return newClient(&cfg)
}

// NewCtxClient creates a client with a context but no underlying grpc
// connection. This is useful for embedded cases that override the
// service interface implementations and do not need connection management.
func NewCtxClient(ctx context.Context, opts ...Option) *Client {
	cctx, cancel := context.WithCancel(ctx)
	c := &Client{ctx: cctx, cancel: cancel, lgMu: new(sync.RWMutex)}
	for _, opt := range opts {
		opt(c)
	}
	if c.lg == nil {
		c.lg = zap.NewNop()
	}
	return c
}

// Option is a function type that can be passed as argument to NewCtxClient to configure client
type Option func(*Client)

// WithZapLogger is a NewCtxClient option that overrides the logger
func WithZapLogger(lg *zap.Logger) Option {
	return func(c *Client) {
		c.lg = lg
	}
}

// WithLogger overrides the logger.
//
// Deprecated: Please use WithZapLogger or Logger field in clientv3.Config
//
// Does not changes grpcLogger, that can be explicitly configured
// using grpc_zap.ReplaceGrpcLoggerV2(..) method.
func (c *Client) WithLogger(lg *zap.Logger) *Client {
	c.lgMu.Lock()
	c.lg = lg
	c.lgMu.Unlock()
	return c
}

// GetLogger gets the logger.
// NOTE: This method is for internal use of etcd-client library and should not be used as general-purpose logger.
func (c *Client) GetLogger() *zap.Logger {
	c.lgMu.RLock()
	l := c.lg
	c.lgMu.RUnlock()
	return l
}

// Close shuts down the client's etcd connections.
func (c *Client) Close() error {
	c.cancel()
	if c.Watcher != nil {
		c.Watcher.Close()
	}
	if c.Lease != nil {
		c.Lease.Close()
	}
	if c.conn != nil {
		return toErr(c.ctx, c.conn.Close())
	}
	return c.ctx.Err()
}

func (c *Client) Ctx() context.Context { return c.ctx }

// Dial connects to a single endpoint using the client's config.
func (c *Client) Dial(ep string) (*grpc.ClientConn, error) {
	creds := c.credentialsForEndpoint(ep)

	// Using ad-hoc created resolver, to guarantee only explicitly given
	// endpoint is used.
	return c.dial(creds, grpc.WithResolvers(resolver.New(ep)))
}

// roundRobinQuorumBackoff retries against quorum between each backoff.
// This is intended for use with a round robin load balancer.
func (c *Client) roundRobinQuorumBackoff(waitBetween time.Duration, jitterFraction float64) backoffFunc {
	return func(attempt uint) time.Duration {
		// after each round robin across quorum, backoff for our wait between duration
		n := uint(len(c.Endpoints()))
		quorum := (n/2 + 1)
		if attempt%quorum == 0 {
			c.lg.Debug("backoff", zap.Uint("attempt", attempt), zap.Uint("quorum", quorum), zap.Duration("waitBetween", waitBetween), zap.Float64("jitterFraction", jitterFraction))
			return jitterUp(waitBetween, jitterFraction)
		}
		c.lg.Debug("backoff skipped", zap.Uint("attempt", attempt), zap.Uint("quorum", quorum))
		return 0
	}
}

// ---------------------------------------------  OVER  ------------------------------------------------------------

func (c *Client) SetEndpoints(eps ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cfg.Endpoints = eps
	c.resolver.SetEndpoints(eps)
}

// Sync 将客户端的端点与来自etcd成员的已知端点进行同步.
func (c *Client) Sync(ctx context.Context) error {
	mresp, err := c.MemberList(ctx)
	if err != nil {
		return err
	}
	var eps []string
	for _, m := range mresp.Members {
		eps = append(eps, m.ClientURLs...)
	}
	c.SetEndpoints(eps...)
	return nil
}

func (c *Client) autoSync() {
	if c.cfg.AutoSyncInterval == time.Duration(0) {
		return
	}

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(c.cfg.AutoSyncInterval):
			ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
			err := c.Sync(ctx)
			cancel()
			if err != nil && err != c.ctx.Err() {
				c.lg.Info("Auto sync endpoints failed.", zap.Error(err))
			}
		}
	}
}

// dialSetupOpts 链接参数
func (c *Client) dialSetupOpts(creds grpccredentials.TransportCredentials, dopts ...grpc.DialOption) (opts []grpc.DialOption, err error) {
	if c.cfg.DialKeepAliveTime > 0 {
		params := keepalive.ClientParameters{
			Time:                c.cfg.DialKeepAliveTime,
			Timeout:             c.cfg.DialKeepAliveTimeout,
			PermitWithoutStream: c.cfg.PermitWithoutStream,
		}
		opts = append(opts, grpc.WithKeepaliveParams(params))
	}
	opts = append(opts, dopts...)

	if creds != nil {
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	// Interceptor retry and backoff.
	// TODO: Replace all of clientv3/retry.go with RetryPolicy:
	// https://github.com/grpc/grpc-proto/blob/cdd9ed5c3d3f87aef62f373b93361cf7bddc620d/grpc/service_config/service_config.proto#L130
	rrBackoff := withBackoff(c.roundRobinQuorumBackoff(defaultBackoffWaitBetween, defaultBackoffJitterFraction))
	opts = append(opts,
		// Disable stream retry by default since go-grpc-middleware/retry does not support client streams.
		// Streams that are safe to retry are enabled individually.
		grpc.WithStreamInterceptor(c.streamClientInterceptor(withMax(0), rrBackoff)),
		grpc.WithUnaryInterceptor(c.unaryClientInterceptor(withMax(defaultUnaryMaxRetries), rrBackoff)),
	)

	return opts, nil
}

// 检查服务端版本
func (c *Client) checkVersion() (err error) {
	var wg sync.WaitGroup

	eps := c.Endpoints()
	errc := make(chan error, len(eps))
	ctx, cancel := context.WithCancel(c.ctx)
	if c.cfg.DialTimeout > 0 {
		cancel()
		ctx, cancel = context.WithTimeout(c.ctx, c.cfg.DialTimeout)
	}

	wg.Add(len(eps))
	for _, ep := range eps {
		// 如果集群是当前的任何端点都会给出一个最新的版本
		go func(e string) {
			defer wg.Done()
			resp, rerr := c.Status(ctx, e)
			if rerr != nil {
				errc <- rerr
				return
			}
			vs := strings.Split(resp.Version, ".") // [3 5 2]
			maj, min := 0, 0
			if len(vs) >= 2 {
				var serr error
				if maj, serr = strconv.Atoi(vs[0]); serr != nil {
					errc <- serr
					return
				}
				if min, serr = strconv.Atoi(vs[1]); serr != nil {
					errc <- serr
					return
				}
			}
			// 3.2版本以下
			if maj < 3 || (maj == 3 && min < 2) {
				rerr = ErrOldCluster
			}
			errc <- rerr
		}(ep)
	}
	for range eps {
		if err = <-errc; err == nil {
			break
		}
	}
	cancel()
	wg.Wait()
	return err
}

func (c *Client) ActiveConnection() *grpc.ClientConn { return c.conn }

// isHaltErr 如果给定的错误和上下文表明无法取得进展甚至在重新连接后返回true.
func isHaltErr(ctx context.Context, err error) bool {
	if ctx != nil && ctx.Err() != nil {
		return true
	}
	if err == nil {
		return false
	}
	ev, _ := status.FromError(err)
	return ev.Code() != codes.Unavailable && ev.Code() != codes.Internal
}

// isUnavailableErr 返回错误是不是 不可用 类型
func isUnavailableErr(ctx context.Context, err error) bool {
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	if err == nil {
		return false
	}
	ev, ok := status.FromError(err)
	if ok {
		// Unavailable codes mean the system will be right back.
		// (e.g., can't connect, lost leader)
		return ev.Code() == codes.Unavailable
	}
	return false
}

func toErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	err = rpctypes.Error(err)
	if _, ok := err.(rpctypes.EtcdError); ok {
		return err
	}
	if ev, ok := status.FromError(err); ok {
		code := ev.Code()
		switch code {
		case codes.DeadlineExceeded:
			fallthrough
		case codes.Canceled:
			if ctx.Err() != nil {
				err = ctx.Err()
			}
		}
	}
	return err
}

func canceledByCaller(stopCtx context.Context, err error) bool {
	if stopCtx.Err() == nil || err == nil {
		return false
	}

	return err == context.Canceled || err == context.DeadlineExceeded
}

// IsConnCanceled returns true, if error is from a closed gRPC connection.
// ref. https://github.com/grpc/grpc-go/pull/1854
func IsConnCanceled(err error) bool {
	if err == nil {
		return false
	}

	// >= gRPC v1.23.x
	s, ok := status.FromError(err)
	if ok {
		// connection is canceled or etcd has already closed the connection
		return s.Code() == codes.Canceled || s.Message() == "transport is closing"
	}

	// >= gRPC v1.10.x
	if err == context.Canceled {
		return true
	}

	// <= gRPC v1.7.x returns 'errors.New("grpc: the client connection is closing")'
	return strings.Contains(err.Error(), "grpc: the client connection is closing")
}

func (c *Client) Endpoints() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	eps := make([]string, len(c.cfg.Endpoints))
	copy(eps, c.cfg.Endpoints)
	return eps
}

// OK
func (c *Client) getToken(ctx context.Context) error {
	var err error

	if c.Username == "" || c.Password == "" {
		return nil
	}

	resp, err := c.Auth.Authenticate(ctx, c.Username, c.Password)
	if err != nil {
		if err == rpctypes.ErrAuthNotEnabled {
			return nil
		}
		return err
	}
	c.authTokenBundle.UpdateAuthToken(resp.Token)
	return nil
}

// OK
func (c *Client) dialWithBalancer(dopts ...grpc.DialOption) (*grpc.ClientConn, error) {
	creds := c.credentialsForEndpoint(c.Endpoints()[0]) // 根据第一个判断是不需要证书
	opts := append(dopts, grpc.WithResolvers(c.resolver))
	return c.dial(creds, opts...)
}

// OK
func (c *Client) dial(creds grpccredentials.TransportCredentials, dopts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts, err := c.dialSetupOpts(creds, dopts...)
	if err != nil {
		return nil, fmt.Errorf("配置dialer失败: %v", err)
	}
	if c.Username != "" && c.Password != "" {
		c.authTokenBundle = credentials.NewBundle(credentials.Config{})
		opts = append(opts, grpc.WithPerRPCCredentials(c.authTokenBundle.PerRPCCredentials()))
	}

	opts = append(opts, c.cfg.DialOptions...)

	dctx := c.ctx
	if c.cfg.DialTimeout > 0 {
		var cancel context.CancelFunc
		dctx, cancel = context.WithTimeout(c.ctx, c.cfg.DialTimeout)
		defer cancel()
	}
	target := fmt.Sprintf("%s://%p/%s", resolver.Schema, c, authority(c.Endpoints()[0]))
	conn, err := grpc.DialContext(dctx, target, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// 返回地址
func authority(endpoint string) string {
	spl := strings.SplitN(endpoint, "://", 2)
	if len(spl) < 2 {
		if strings.HasPrefix(endpoint, "unix:") {
			return endpoint[len("unix:"):]
		}
		if strings.HasPrefix(endpoint, "unixs:") {
			return endpoint[len("unixs:"):]
		}
		return endpoint
	}
	return spl[1]
}

// OK
func (c *Client) credentialsForEndpoint(ep string) grpccredentials.TransportCredentials {
	r := endpoint.RequiresCredentials(ep) // 127.0.0.1:2379
	switch r {
	case endpoint.CREDS_DROP:
		return nil
	case endpoint.CREDS_OPTIONAL:
		return c.creds
	case endpoint.CREDS_REQUIRE:
		if c.creds != nil {
			return c.creds
		}
		return credentials.NewBundle(credentials.Config{}).TransportCredentials()
	default:
		panic(fmt.Errorf("unsupported CredsRequirement: %v", r))
	}
}

// 创建一个client用于与etcd server 通信
func newClient(cfg *Config) (*Client, error) {
	if cfg == nil {
		cfg = &Config{}
	}
	var creds grpccredentials.TransportCredentials
	if cfg.TLS != nil {
		creds = credentials.NewBundle(credentials.Config{TLSConfig: cfg.TLS}).TransportCredentials()
	}

	// 使用一个临时的客户端来启动第一个连接
	baseCtx := context.TODO()
	if cfg.Context != nil {
		baseCtx = cfg.Context
	}

	ctx, cancel := context.WithCancel(baseCtx)
	client := &Client{
		conn:     nil,
		cfg:      *cfg,
		creds:    creds,
		ctx:      ctx,
		cancel:   cancel,
		mu:       new(sync.RWMutex),
		callOpts: defaultCallOpts,
		lgMu:     new(sync.RWMutex),
	}

	var err error
	if cfg.Logger != nil {
		client.lg = cfg.Logger
	} else if cfg.LogConfig != nil {
		client.lg, err = cfg.LogConfig.Build()
	} else {
		client.lg, err = CreateDefaultZapLogger()
	}
	if err != nil {
		return nil, err
	}

	if cfg.Username != "" && cfg.Password != "" {
		client.Username = cfg.Username
		client.Password = cfg.Password
	}
	if cfg.MaxCallSendMsgSize > 0 || cfg.MaxCallRecvMsgSize > 0 {
		if cfg.MaxCallRecvMsgSize > 0 && cfg.MaxCallSendMsgSize > cfg.MaxCallRecvMsgSize {
			return nil, fmt.Errorf("gRPC消息接收大小 (%d bytes)必须是大于发送的 (%d bytes)", cfg.MaxCallRecvMsgSize, cfg.MaxCallSendMsgSize)
		}
		callOpts := []grpc.CallOption{
			defaultWaitForReady,
			defaultMaxCallSendMsgSize,
			defaultMaxCallRecvMsgSize,
		}
		if cfg.MaxCallSendMsgSize > 0 {
			callOpts[1] = grpc.MaxCallSendMsgSize(cfg.MaxCallSendMsgSize)
		}
		if cfg.MaxCallRecvMsgSize > 0 {
			callOpts[2] = grpc.MaxCallRecvMsgSize(cfg.MaxCallRecvMsgSize)
		}
		client.callOpts = callOpts
	}
	client.resolver = resolver.New(cfg.Endpoints...)

	if len(cfg.Endpoints) < 1 {
		client.cancel()
		return nil, fmt.Errorf("至少需要一个端点")
	}

	conn, err := client.dialWithBalancer()
	if err != nil {
		client.cancel()
		client.resolver.Close()
		return nil, err
	}
	client.conn = conn

	client.Cluster = NewCluster(client)
	client.KV = NewKV(client)
	client.Lease = NewLease(client)
	client.Watcher = NewWatcher(client)
	client.Auth = NewAuth(client)
	client.Maintenance = NewMaintenance(client)

	// 获得已建立连接的令牌
	ctx, cancel = client.ctx, func() {}
	if client.cfg.DialTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, client.cfg.DialTimeout)
	}
	err = client.getToken(ctx)
	if err != nil {
		client.Close()
		cancel()
		return nil, err
	}
	cancel()
	if cfg.RejectOldCluster { // false
		if err := client.checkVersion(); err != nil {
			client.Close()
			return nil, err
		}
	}

	go client.autoSync()
	return client, nil
}
