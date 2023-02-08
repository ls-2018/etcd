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

package embed

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	defaultLog "log"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2http"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2v3"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v3client"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/transport"
	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/etcd/config"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/etcdhttp"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/rafthttp"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v3rpc"
	"github.com/ls-2018/etcd_cn/etcd/verify"
	"github.com/ls-2018/etcd_cn/offical/api/v3/version"
	"github.com/ls-2018/etcd_cn/pkg/debugutil"
	runtimeutil "github.com/ls-2018/etcd_cn/pkg/runtime"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/soheilhy/cmux"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/exporters/otlp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpgrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	// internal fd usage includes disk usage and transport usage.
	// To read/write snapshot, snap pkg needs 1. In normal case, wal pkg needs
	// at most 2 to read/lock/write WALs. One case that it needs to 2 is to
	// read all logs after some snapshot index, which locates at the end of
	// the second last and the head of the last. For purging, it needs to read
	// directory, so it needs 1. For fd monitor, it needs 1.
	// For transport, rafthttp builds two long-polling connections and at most
	// four temporary connections with each member. There are at most 9 members
	// in a cluster, so it should reserve 96.
	// For the safety, we set the total reserved number to 150.
	reservedInternalFDNum = 150
)

// Etcd 包含一个正在运行的etcd etcd和它的监听器.
type Etcd struct {
	Peers   []*peerListener
	Clients []net.Listener
	// 本机节点监听本地网卡的map     例如   localhost:2379   127.0.0.1:2379  0.0.0.0:2379 等等
	sctxs                   map[string]*serveCtx
	metricsListeners        []net.Listener
	tracingExporterShutdown func()
	Server                  *etcdserver.EtcdServer
	cfg                     Config
	stopc                   chan struct{} // raft 停止,消息通道
	errc                    chan error    // 接收运行过程中产生的err
	closeOnce               sync.Once
}

// 每个server的Listener
type peerListener struct {
	net.Listener
	serve func() error
	close func(context.Context) error // 替换为net.Listener.Close()
}

// StartEtcd 启动 用于客户端/etcd通信的 `etcd和HTTP处理程序` .不保证返回的Etcd.Server已经加入集群.
// 等待Etcd.Server.ReadyNotify()通道,以了解它何时完成并可以使用.
func StartEtcd(inCfg *Config) (e *Etcd, err error) {
	if err = inCfg.Validate(); err != nil {
		return nil, err
	}
	serving := false
	e = &Etcd{cfg: *inCfg, stopc: make(chan struct{})}
	cfg := &e.cfg
	defer func() {
		if e == nil || err == nil {
			return
		}
		if !serving {
			// 在为serveCtx.servicesC启动gRPC etcd之前出现错误.
			for _, sctx := range e.sctxs {
				close(sctx.serversC)
			}
		}
		e.Close() // 启动失败时, 优雅关闭
		e = nil
	}()

	if !cfg.SocketOpts.Empty() {
		cfg.logger.Info("配置socket选项", zap.Bool("reuse-address", cfg.SocketOpts.ReuseAddress), zap.Bool("reuse-port", cfg.SocketOpts.ReusePort))
	}
	e.cfg.logger.Info("", zap.Strings("listen-peer-urls", e.cfg.getLPURLs()))
	// 设置每个server listener 的超时时间、证书、socket选项
	if e.Peers, err = configurePeerListeners(cfg); err != nil {
		return e, err
	}

	e.cfg.logger.Info("配置peer listener", zap.Strings("listen-client-urls", e.cfg.getLCURLs()))
	// 设置每个client listener 的超时时间、证书、socket选项
	if e.sctxs, err = configureClientListeners(cfg); err != nil {
		return e, err
	}

	for _, sctx := range e.sctxs {
		e.Clients = append(e.Clients, sctx.l)
	}

	var (
		urlsmap types.URLsMap
		token   string
	)
	// 成员初始化
	memberInitialized := true
	if !isMemberInitialized(cfg) { // 判断wal目录存不存在
		memberInitialized = false
		urlsmap, token, err = cfg.PeerURLsMapAndToken("etcd") // token  {name:urls[]}
		if err != nil {
			return e, fmt.Errorf("设置初始化集群出错: %v", err)
		}
	}
	// 自动压缩配置
	if len(cfg.AutoCompactionRetention) == 0 { // 没有设置
		cfg.AutoCompactionRetention = "0"
	}
	// 根据压缩类型、压缩配置    返回时间、或条数
	autoCompactionRetention, err := parseCompactionRetention(cfg.AutoCompactionMode, cfg.AutoCompactionRetention)
	if err != nil {
		return e, err
	}
	// 返回boltdb存储的数据类型,array \ map
	backendFreelistType := parseBackendFreelistType(cfg.BackendFreelistType)

	srvcfg := config.ServerConfig{
		Name:                                     cfg.Name,
		ClientURLs:                               cfg.ACUrls,
		PeerURLs:                                 cfg.APUrls,
		DataDir:                                  cfg.Dir,
		DedicatedWALDir:                          cfg.WalDir,
		SnapshotCount:                            cfg.SnapshotCount,          // 触发一次磁盘快照的提交事务的次数
		SnapshotCatchUpEntries:                   cfg.SnapshotCatchUpEntries, //  快照追赶数据量
		MaxSnapFiles:                             cfg.MaxSnapFiles,
		MaxWALFiles:                              cfg.MaxWalFiles, // 要保留的最大wal文件数(0表示不受限制). 5
		InitialPeerURLsMap:                       urlsmap,         //  节点--> url
		InitialClusterToken:                      token,
		DiscoveryURL:                             cfg.Durl,
		DiscoveryProxy:                           cfg.Dproxy,
		NewCluster:                               cfg.IsNewCluster(),             // new existing
		PeerTLSInfo:                              cfg.PeerTLSInfo,                // server 证书信息
		TickMs:                                   cfg.TickMs,                     // tick计时器触发间隔
		ElectionTicks:                            cfg.ElectionTicks(),            // 返回选举权检查对应多少次tick触发次数
		InitialElectionTickAdvance:               cfg.InitialElectionTickAdvance, // 是否提前初始化选举时钟启动,以便更快的选举
		AutoCompactionRetention:                  autoCompactionRetention,        // 自动压缩值
		AutoCompactionMode:                       cfg.AutoCompactionMode,         // 自动压缩模式
		QuotaBackendBytes:                        cfg.QuotaBackendBytes,          // 资源存储阈值
		BackendBatchLimit:                        cfg.BoltBackendBatchLimit,      // BackendBatchLimit是提交后端事务前的最大操作数
		BackendFreelistType:                      backendFreelistType,            // 返回boltdb存储的数据类型
		BackendBatchInterval:                     cfg.BoltBackendBatchInterval,   // BackendBatchInterval是提交后端事务前的最长时间.
		MaxTxnOps:                                cfg.MaxTxnOps,
		MaxRequestBytes:                          cfg.MaxRequestBytes, // 服务器将接受的最大客户端请求大小(字节).
		SocketOpts:                               cfg.SocketOpts,
		StrictReconfigCheck:                      cfg.StrictReconfigCheck, // 严格配置变更检查
		ClientCertAuthEnabled:                    cfg.ClientTLSInfo.ClientCertAuth,
		AuthToken:                                cfg.AuthToken,  // 认证格式  simple、jwt
		BcryptCost:                               cfg.BcryptCost, // 为散列身份验证密码指定bcrypt算法的成本/强度
		TokenTTL:                                 cfg.AuthTokenTTL,
		CORS:                                     cfg.CORS,
		HostWhitelist:                            cfg.HostWhitelist,
		InitialCorruptCheck:                      cfg.ExperimentalInitialCorruptCheck, // 数据毁坏检测功能
		CorruptCheckTime:                         cfg.ExperimentalCorruptCheckTime,
		PreVote:                                  cfg.PreVote, // PreVote 是否启用PreVote
		Logger:                                   cfg.logger,
		ForceNewCluster:                          cfg.ForceNewCluster,
		EnableGRPCGateway:                        cfg.EnableGRPCGateway,                    // 启用grpc网关,将 http 转换成 grpc / true
		ExperimentalEnableDistributedTracing:     cfg.ExperimentalEnableDistributedTracing, // 默认false
		UnsafeNoFsync:                            cfg.UnsafeNoFsync,
		EnableLeaseCheckpoint:                    cfg.ExperimentalEnableLeaseCheckpoint, // 允许leader定期向其他成员发送检查点,以防止leader变化时剩余TTL重置.
		LeaseCheckpointPersist:                   cfg.ExperimentalEnableLeaseCheckpointPersist,
		CompactionBatchLimit:                     cfg.ExperimentalCompactionBatchLimit,
		WatchProgressNotifyInterval:              cfg.ExperimentalWatchProgressNotifyInterval,
		DowngradeCheckTime:                       cfg.ExperimentalDowngradeCheckTime,   // 两次降级状态检查之间的时间间隔.
		WarningApplyDuration:                     cfg.ExperimentalWarningApplyDuration, // 是时间长度.如果应用请求的时间超过这个值.就会产生一个警告.
		ExperimentalMemoryMlock:                  cfg.ExperimentalMemoryMlock,
		ExperimentalTxnModeWriteWithSharedBuffer: cfg.ExperimentalTxnModeWriteWithSharedBuffer,
		ExperimentalBootstrapDefragThresholdMegabytes: cfg.ExperimentalBootstrapDefragThresholdMegabytes,
	}

	if srvcfg.ExperimentalEnableDistributedTracing { // 使用OpenTelemetry协议实现分布式跟踪.默认false
		tctx := context.Background()
		tracingExporter, opts, err := e.setupTracing(tctx)
		if err != nil {
			return e, err
		}
		if tracingExporter == nil || len(opts) == 0 {
			return e, fmt.Errorf("error setting up distributed tracing")
		}
		e.tracingExporterShutdown = func() { tracingExporter.Shutdown(tctx) }
		srvcfg.ExperimentalTracerOptions = opts
	}

	print(e.cfg.logger, *cfg, srvcfg, memberInitialized)

	// TODO 在看
	if e.Server, err = etcdserver.NewServer(srvcfg); err != nil {
		return e, err
	}

	// buffer channel so goroutines on closed connections won't wait forever
	e.errc = make(chan error, len(e.Peers)+len(e.Clients)+2*len(e.sctxs))

	// newly started member ("memberInitialized==false")
	// does not need corruption check
	if memberInitialized {
		if err = e.Server.CheckInitialHashKV(); err != nil {
			// set "EtcdServer" to nil, so that it does not block on "EtcdServer.Close()"
			// (nothing to close since rafthttp transports have not been started)

			e.cfg.logger.Error("checkInitialHashKV failed", zap.Error(err))
			e.Server.Cleanup()
			e.Server = nil
			return e, err
		}
	}
	e.Server.Start()

	if err = e.servePeers(); err != nil {
		return e, err
	}
	if err = e.serveClients(); err != nil {
		return e, err
	}
	if err = e.serveMetrics(); err != nil { // ✅
		return e, err
	}

	e.cfg.logger.Info(
		"启动服务 peer/client/metrics",
		zap.String("local-member-id", e.Server.ID().String()),
		zap.Strings("initial-advertise-peer-urls", e.cfg.getAPURLs()),
		zap.Strings("listen-peer-urls", e.cfg.getLPURLs()), // 集群节点之间通信监听的URL;如果指定的IP是0.0.0.0,那么etcd 会监昕所有网卡的指定端口
		zap.Strings("advertise-client-urls", e.cfg.getACURLs()),
		zap.Strings("listen-client-urls", e.cfg.getLCURLs()),
		zap.Strings("listen-metrics-urls", e.cfg.getMetricsURLs()),
	)
	serving = true
	return e, nil
}

func print(lg *zap.Logger, ec Config, sc config.ServerConfig, memberInitialized bool) {
	cors := make([]string, 0, len(ec.CORS))
	for v := range ec.CORS {
		cors = append(cors, v)
	}
	sort.Strings(cors)

	hss := make([]string, 0, len(ec.HostWhitelist))
	for v := range ec.HostWhitelist {
		hss = append(hss, v)
	}
	sort.Strings(hss)

	quota := ec.QuotaBackendBytes
	if quota == 0 {
		quota = etcdserver.DefaultQuotaBytes
	}

	fmt.Println("------->",
		zap.String("etcd-version", version.Version),
		zap.String("git-sha", version.GitSHA),
		zap.String("go-version", runtime.Version()),
		zap.String("go-os", runtime.GOOS),
		zap.String("go-arch", runtime.GOARCH),
		zap.Int("max-cpu-set", runtime.GOMAXPROCS(0)),
		zap.Int("max-cpu-available", runtime.NumCPU()),
		zap.Bool("member-initialized", memberInitialized),
		zap.String("name", sc.Name),
		zap.String("data-dir", sc.DataDir),
		zap.String("wal-dir", ec.WalDir),
		zap.String("wal-dir-dedicated", sc.DedicatedWALDir),
		zap.String("member-dir", sc.MemberDir()),
		zap.Bool("force-new-cluster", sc.ForceNewCluster),
		zap.String("heartbeat-interval", fmt.Sprintf("%v", time.Duration(sc.TickMs)*time.Millisecond)),
		zap.String("election-timeout", fmt.Sprintf("%v", time.Duration(sc.ElectionTicks*int(sc.TickMs))*time.Millisecond)),
		zap.Bool("initial-election-tick-advance", sc.InitialElectionTickAdvance), // 是否提前初始化选举时钟启动,以便更快的选举
		zap.Uint64("snapshot-count", sc.SnapshotCount),                           // 触发一次磁盘快照的提交事务的次数
		zap.Uint64("snapshot-catchup-entries", sc.SnapshotCatchUpEntries),
		zap.Strings("initial-advertise-peer-urls", ec.getAPURLs()),
		zap.Strings("listen-peer-urls", ec.getLPURLs()), // 集群节点之间通信监听的URL;如果指定的IP是0.0.0.0,那么etcd 会监昕所有网卡的指定端口
		zap.Strings("advertise-client-urls", ec.getACURLs()),
		zap.Strings("listen-client-urls", ec.getLCURLs()),
		zap.Strings("listen-metrics-urls", ec.getMetricsURLs()),
		zap.Strings("cors", cors),
		zap.Strings("host-whitelist", hss),
		zap.String("initial-cluster", sc.InitialPeerURLsMap.String()),
		zap.String("initial-cluster-state", ec.ClusterState),
		zap.String("initial-cluster-token", sc.InitialClusterToken),
		zap.Int64("quota-size-bytes", quota),
		zap.Bool("pre-vote", sc.PreVote),
		zap.Bool("initial-corrupt-check", sc.InitialCorruptCheck),
		zap.String("corrupt-check-time-interval", sc.CorruptCheckTime.String()),
		zap.String("auto-compaction-mode", sc.AutoCompactionMode),
		zap.Duration("auto-compaction-retention", sc.AutoCompactionRetention),
		zap.String("auto-compaction-interval", sc.AutoCompactionRetention.String()),
		zap.String("discovery-url", sc.DiscoveryURL),
		zap.String("discovery-proxy", sc.DiscoveryProxy),
		zap.String("downgrade-check-interval", sc.DowngradeCheckTime.String()),
	)
}

// Config returns the current configuration.
func (e *Etcd) Config() Config {
	return e.cfg
}

// Close 优雅关闭server 以及所有链接
// 客户端请求在超时之后会终止,之后会被关闭
func (e *Etcd) Close() {
	fields := []zap.Field{
		zap.String("name", e.cfg.Name),
		zap.String("data-dir", e.cfg.Dir),
		zap.Strings("advertise-peer-urls", e.cfg.getAPURLs()),
		zap.Strings("advertise-client-urls", e.cfg.getACURLs()),
	}
	lg := e.GetLogger()
	lg.Info("关闭etcd ing...", fields...)
	defer func() {
		lg.Info("关闭etcd", fields...)
		verify.MustVerifyIfEnabled(verify.Config{Logger: lg, DataDir: e.cfg.Dir, ExactIndex: false})
		lg.Sync() // log都刷到磁盘
	}()

	e.closeOnce.Do(func() {
		close(e.stopc)
	})

	// 使用请求超时关闭客户端请求
	timeout := 2 * time.Second
	if e.Server != nil {
		timeout = e.Server.Cfg.ReqTimeout()
	}
	for _, sctx := range e.sctxs {
		for ss := range sctx.serversC {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			stopServers(ctx, ss)
			cancel()
		}
	}

	for _, sctx := range e.sctxs {
		sctx.cancel()
	}

	for i := range e.Clients {
		if e.Clients[i] != nil {
			e.Clients[i].Close()
		}
	}

	for i := range e.metricsListeners {
		e.metricsListeners[i].Close()
	}

	// shutdown tracing exporter
	if e.tracingExporterShutdown != nil {
		e.tracingExporterShutdown()
	}

	// 关闭 rafthttp transports
	if e.Server != nil {
		e.Server.Stop()
	}

	// close all idle connections in peer handler (wait up to 1-second)
	for i := range e.Peers {
		if e.Peers[i] != nil && e.Peers[i].close != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			e.Peers[i].close(ctx)
			cancel()
		}
	}
	if e.errc != nil {
		close(e.errc)
	}
}

func stopServers(ctx context.Context, ss *servers) {
	// first, close the http.Server
	ss.http.Shutdown(ctx)
	// do not grpc.Server.GracefulStop with TLS enabled etcd etcd
	// See https://github.com/grpc/grpc-go/issues/1384#issuecomment-317124531
	// and https://github.com/etcd-io/etcd/issues/8916
	if ss.secure {
		ss.grpc.Stop()
		return
	}

	ch := make(chan struct{})
	go func() {
		defer close(ch)
		// close listeners to stop accepting new connections,
		// will block on any existing transports
		ss.grpc.GracefulStop()
	}()

	// wait until all pending RPCs are finished
	select {
	case <-ch:
	case <-ctx.Done():
		// took too long, manually close open transports
		// e.g. watch streams
		ss.grpc.Stop()

		// concurrent GracefulStop should be interrupted
		<-ch
	}
}

// Err - return channel used to report errors during etcd run/shutdown.
// Since etcd 3.5 the channel is being closed when the etcd is over.
func (e *Etcd) Err() <-chan error {
	return e.errc
}

// 配置 peer listeners
func configurePeerListeners(cfg *Config) (peers []*peerListener, err error) {
	// 更新密码套件
	if err = updateCipherSuites(&cfg.PeerTLSInfo, cfg.CipherSuites); err != nil {
		return nil, err
	}

	if err = cfg.PeerSelfCert(); err != nil {
		cfg.logger.Fatal("未能获得peer的自签名证书", zap.Error(err))
	}
	if !cfg.PeerTLSInfo.Empty() {
		cfg.logger.Info(
			"从peer的TLS开始",
			zap.String("tls-info", fmt.Sprintf("%+v", cfg.PeerTLSInfo)),
			zap.Strings("cipher-suites", cfg.CipherSuites),
		)
	}

	peers = make([]*peerListener, len(cfg.LPUrls))
	defer func() {
		if err == nil {
			return
		}
		for i := range peers {
			if peers[i] != nil && peers[i].close != nil {
				cfg.logger.Warn(
					"关闭节点listener",
					zap.String("address", cfg.LPUrls[i].String()),
					zap.Error(err),
				)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				peers[i].close(ctx)
				cancel()
			}
		}
	}()

	for i, u := range cfg.LPUrls {
		if u.Scheme == "http" {
			if !cfg.PeerTLSInfo.Empty() {
				cfg.logger.Warn("在钥匙和证书文件存在的情况下,方案为HTTP;忽略钥匙和证书文件", zap.String("peer-url", u.String()))
			}
			if cfg.PeerTLSInfo.ClientCertAuth {
				cfg.logger.Warn("方案为HTTP;当启用 --peer-client-cert-auth;忽略钥匙和证书文件", zap.String("peer-url", u.String()))
			}
		}
		// 构造peerListener对象 监听2380 作为服务端模式
		peers[i] = &peerListener{close: func(context.Context) error { return nil }}
		// 调用接口,创建listener对象,返回来之后,
		// socket套接字已经完成listener监听流程
		peers[i].Listener, err = transport.NewListenerWithOpts(u.Host, u.Scheme,
			transport.WithTLSInfo(&cfg.PeerTLSInfo),
			transport.WithSocketOpts(&cfg.SocketOpts),
			transport.WithTimeout(rafthttp.ConnReadTimeout, rafthttp.ConnWriteTimeout),
		)
		if err != nil {
			return nil, err
		}
		//
		peers[i].close = func(context.Context) error {
			return peers[i].Listener.Close()
		}
	}
	return peers, nil
}

// 在rafthttp.Transport启动后配置对等处理程序
func (e *Etcd) servePeers() (err error) {
	// 生成http.hander 用于处理peer请求
	httpHandler := etcdhttp.NewPeerHandler(e.GetLogger(), e.Server)
	var peerTLScfg *tls.Config
	if !e.cfg.PeerTLSInfo.Empty() {
		if peerTLScfg, err = e.cfg.PeerTLSInfo.ServerConfig(); err != nil {
			return err
		}
	}

	for _, p := range e.Peers {

		u := p.Listener.Addr().String()
		grpcServer := v3rpc.Server(e.Server, peerTLScfg, nil)
		m := cmux.New(p.Listener)
		go grpcServer.Serve(m.Match(cmux.HTTP2())) // 基于http2 tcp://127.0.0.1:2380

		httpServer := &http.Server{
			Handler:     grpcHandlerFunc(grpcServer, httpHandler),
			ReadTimeout: 5 * time.Minute,
			ErrorLog:    defaultLog.New(ioutil.Discard, "", 0), // do not log user error
		}
		go httpServer.Serve(m.Match(cmux.Any())) // http1

		p.serve = func() error {
			// 回调函数,激活服务,主要是Accept方法
			e.cfg.logger.Info("cmux::serve", zap.String("address", u))
			return m.Serve()
		}

		p.close = func(ctx context.Context) error {
			// 优雅关闭 http.Server、打开的listeners、空闲的connections 直到超时或上下文关闭
			e.cfg.logger.Info("开始停止服务", zap.String("address", u))
			stopServers(ctx, &servers{secure: peerTLScfg != nil, grpc: grpcServer, http: httpServer})
			e.cfg.logger.Info("已停止服务", zap.String("address", u))
			m.Close()
			return nil
		}
	}

	// start peer servers in a goroutine
	for _, pl := range e.Peers {
		go func(l *peerListener) {
			u := l.Addr().String()
			e.cfg.logger.Info(
				"serving peer traffic",
				zap.String("address", u),
			)
			e.errHandler(l.serve())
		}(pl)
	}
	return nil
}

// 配置与etcdctl客户端的listener选项
func configureClientListeners(cfg *Config) (sctxs map[string]*serveCtx, err error) {
	// 更新密码套件
	if err = updateCipherSuites(&cfg.ClientTLSInfo, cfg.CipherSuites); err != nil {
		return nil, err
	}
	// LCURLS 自签证书
	if err = cfg.ClientSelfCert(); err != nil {
		cfg.logger.Fatal("未能获得客户自签名的证书", zap.Error(err))
	}
	if cfg.EnablePprof {
		cfg.logger.Info("允许性能分析", zap.String("path", debugutil.HTTPPrefixPProf))
	}

	sctxs = make(map[string]*serveCtx)
	for _, u := range cfg.LCUrls {
		sctx := newServeCtx(cfg.logger)
		if u.Scheme == "http" || u.Scheme == "unix" {
			if !cfg.ClientTLSInfo.Empty() {
				cfg.logger.Warn("在钥匙和证书文件存在的情况下,方案为HTTP;忽略钥匙和证书文件", zap.String("client-url", u.String()))
			}
			if cfg.ClientTLSInfo.ClientCertAuth {
				cfg.logger.Warn("方案是HTTP,同时启用了-客户证书认证;该URL忽略了客户证书认证.", zap.String("client-url", u.String()))
			}
		}
		if (u.Scheme == "https" || u.Scheme == "unixs") && cfg.ClientTLSInfo.Empty() {
			return nil, fmt.Errorf("TLS key/cert (--cert-file, --key-file)必须提供,当协议是%q", u.String())
		}

		network := "tcp"
		addr := u.Host
		if u.Scheme == "unix" || u.Scheme == "unixs" {
			network = "unix"
			addr = u.Host + u.Path
		}
		sctx.network = network

		sctx.secure = u.Scheme == "https" || u.Scheme == "unixs"
		sctx.insecure = !sctx.secure // 在处理etcdctl 请求上,是不是启用证书
		if oldctx := sctxs[addr]; oldctx != nil {
			oldctx.secure = oldctx.secure || sctx.secure
			oldctx.insecure = oldctx.insecure || sctx.insecure
			continue
		}

		if sctx.l, err = transport.NewListenerWithOpts(addr, u.Scheme,
			transport.WithSocketOpts(&cfg.SocketOpts),
			transport.WithSkipTLSInfoCheck(true),
		); err != nil {
			return nil, err
		}
		// net.Listener will rewrite ipv4 0.0.0.0 to ipv6 [::], breaking
		// hosts that disable ipv6. So, use the address given by the user.
		sctx.addr = addr

		if fdLimit, fderr := runtimeutil.FDLimit(); fderr == nil {
			if fdLimit <= reservedInternalFDNum {
				cfg.logger.Fatal(
					"file descriptor limit of etcd process is too low; please set higher",
					zap.Uint64("limit", fdLimit),
					zap.Int("recommended-limit", reservedInternalFDNum),
				)
			}
			sctx.l = transport.LimitListener(sctx.l, int(fdLimit-reservedInternalFDNum))
		}

		if network == "tcp" {
			if sctx.l, err = transport.NewKeepAliveListener(sctx.l, network, nil); err != nil {
				return nil, err
			}
		}

		defer func(u url.URL) {
			if err == nil {
				return
			}
			sctx.l.Close()
			cfg.logger.Warn("关闭peer listener", zap.String("address", u.Host), zap.Error(err))
		}(u)
		for k := range cfg.UserHandlers {
			sctx.userHandlers[k] = cfg.UserHandlers[k]
		}
		sctx.serviceRegister = cfg.ServiceRegister
		if cfg.EnablePprof || cfg.LogLevel == "debug" {
			sctx.registerPprof()
		}
		if cfg.LogLevel == "debug" {
			sctx.registerTrace()
		}
		sctxs[addr] = sctx
	}
	return sctxs, nil
}

// OK
func (e *Etcd) serveClients() (err error) {
	if !e.cfg.ClientTLSInfo.Empty() {
		e.cfg.logger.Info(
			"使用证书启动client",
			zap.String("tls-info", fmt.Sprintf("%+v", e.cfg.ClientTLSInfo)),
			zap.Strings("cipher-suites", e.cfg.CipherSuites),
		)
	}

	// Start a client server goroutine for each listen address
	var h http.Handler
	if e.Config().EnableV2 {
		if e.Config().V2DeprecationEffective().IsAtLeast(config.V2_DEPR_1_WRITE_ONLY) {
			return fmt.Errorf("--enable-v2 and --v2-deprecation=%s are mutually exclusive", e.Config().V2DeprecationEffective())
		}
		e.cfg.logger.Warn("Flag `enable-v2` is deprecated and will get removed in etcd 3.6.")
		if len(e.Config().ExperimentalEnableV2V3) > 0 {
			e.cfg.logger.Warn("Flag `experimental-enable-v2v3` is deprecated and will get removed in etcd 3.6.")
			srv := v2v3.NewServer(e.cfg.logger, v3client.New(e.Server), e.cfg.ExperimentalEnableV2V3)
			h = v2http.NewClientHandler(e.GetLogger(), srv, e.Server.Cfg.ReqTimeout())
		} else {
			h = v2http.NewClientHandler(e.GetLogger(), e.Server, e.Server.Cfg.ReqTimeout())
		}
	} else {
		mux := http.NewServeMux()
		etcdhttp.HandleBasic(e.cfg.logger, mux, e.Server)
		etcdhttp.HandleMetricsHealthForV3(e.cfg.logger, mux, e.Server)
		h = mux
	}

	mux := http.NewServeMux()                         // ✅
	etcdhttp.HandleBasic(e.cfg.logger, mux, e.Server) // ✅
	h = mux

	var gopts []grpc.ServerOption
	if e.cfg.GRPCKeepAliveMinTime > time.Duration(0) {
		gopts = append(gopts, grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             e.cfg.GRPCKeepAliveMinTime,
			PermitWithoutStream: false, // 默认false
			// 如果是true,即使没有活动流(RPCs),服务器也允许keepalive pings.如果是假的,客户端在没有活动流的情况下发送ping 流,服务器将发送GOAWAY并关闭连接.
		}))
	}
	if e.cfg.GRPCKeepAliveInterval > time.Duration(0) && e.cfg.GRPCKeepAliveTimeout > time.Duration(0) {
		gopts = append(gopts, grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    e.cfg.GRPCKeepAliveInterval,
			Timeout: e.cfg.GRPCKeepAliveTimeout,
		}))
	}

	// 启动每一个监听网卡的程序
	for _, sctx := range e.sctxs {
		go func(s *serveCtx) {
			e.errHandler(s.serve(e.Server, &e.cfg.ClientTLSInfo, h, e.errHandler, gopts...))
		}(sctx)
	}
	return nil
}

func (e *Etcd) serveMetrics() (err error) {
	if e.cfg.Metrics == "extensive" { // basic
		grpc_prometheus.EnableHandlingTimeHistogram()
	}
	// 长度为0, 监听etcd ctl客户端请求
	if len(e.cfg.ListenMetricsUrls) > 0 {
		for _, murl := range e.cfg.ListenMetricsUrls {
			tlsInfo := &e.cfg.ClientTLSInfo
			if murl.Scheme == "http" {
				tlsInfo = nil
			}
			ml, err := transport.NewListenerWithOpts(murl.Host, murl.Scheme,
				transport.WithTLSInfo(tlsInfo),
				transport.WithSocketOpts(&e.cfg.SocketOpts),
			)
			if err != nil {
				return err
			}
			e.metricsListeners = append(e.metricsListeners, ml)
			go func(u url.URL, ln net.Listener) {
				e.cfg.logger.Info(
					"serving metrics",
					zap.String("address", u.String()),
				)
			}(murl, ml)
		}
	}
	return nil
}

// 处理err
func (e *Etcd) errHandler(err error) {
	select {
	case <-e.stopc:
		return
	default:
	}
	// 一般都卡在这
	select {
	case <-e.stopc:
	case e.errc <- err:
	}
}

// GetLogger returns the logger.
func (e *Etcd) GetLogger() *zap.Logger {
	e.cfg.loggerMu.RLock()
	l := e.cfg.logger
	e.cfg.loggerMu.RUnlock()
	return l
}

// 解析返回条数、时间
func parseCompactionRetention(mode, retention string) (ret time.Duration, err error) {
	h, err := strconv.Atoi(retention)
	if err == nil && h >= 0 {
		switch mode {
		case CompactorModeRevision:
			ret = time.Duration(int64(h))
		case CompactorModePeriodic:
			ret = time.Duration(int64(h)) * time.Hour
		}
	} else {
		// 周期性压缩
		ret, err = time.ParseDuration(retention)
		if err != nil {
			return 0, fmt.Errorf("解析失败CompactionRetention: %v", err)
		}
	}
	return ret, nil
}

func (e *Etcd) setupTracing(ctx context.Context) (exporter tracesdk.SpanExporter, options []otelgrpc.Option, err error) {
	exporter, err = otlp.NewExporter(ctx,
		otlpgrpc.NewDriver(
			otlpgrpc.WithEndpoint(e.cfg.ExperimentalDistributedTracingAddress),
			otlpgrpc.WithInsecure(),
		))
	if err != nil {
		return nil, nil, err
	}
	res := resource.NewWithAttributes(
		semconv.ServiceNameKey.String(e.cfg.ExperimentalDistributedTracingServiceName),
	)
	// As Tracing service Instance ID必须是unique, it should
	// never use the empty default string value, so we only set it
	// if it's a non empty string.
	if e.cfg.ExperimentalDistributedTracingServiceInstanceID != "" {
		resWithIDKey := resource.NewWithAttributes(
			(semconv.ServiceInstanceIDKey.String(e.cfg.ExperimentalDistributedTracingServiceInstanceID)),
		)
		// Merge resources to combine into a new
		// resource in case of duplicates.
		res = resource.Merge(res, resWithIDKey)
	}

	options = append(options,
		otelgrpc.WithPropagators(
			propagation.NewCompositeTextMapPropagator(
				propagation.TraceContext{},
				propagation.Baggage{},
			),
		),
		otelgrpc.WithTracerProvider(
			tracesdk.NewTracerProvider(
				tracesdk.WithBatcher(exporter),
				tracesdk.WithResource(res),
			),
		),
	)

	e.cfg.logger.Info(
		"distributed tracing enabled",
		zap.String("distributed-tracing-address", e.cfg.ExperimentalDistributedTracingAddress),
		zap.String("distributed-tracing-service-name", e.cfg.ExperimentalDistributedTracingServiceName),
		zap.String("distributed-tracing-service-instance-id", e.cfg.ExperimentalDistributedTracingServiceInstanceID),
	)

	return exporter, options, err
}
