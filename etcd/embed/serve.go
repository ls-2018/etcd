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

package embed

import (
	"context"
	"fmt"
	"io/ioutil"
	defaultLog "log"
	"math"
	"net"
	"net/http"
	"strings"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/transport"
	"github.com/ls-2018/etcd_cn/client_sdk/v3/credentials"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v3client"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v3election"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v3election/v3electionpb"
	v3electiongw "github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v3election/v3electionpb/gw"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v3lock"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v3lock/v3lockpb"
	v3lockgw "github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v3lock/v3lockpb/gw"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v3rpc"
	etcdservergw "github.com/ls-2018/etcd_cn/offical/etcdserverpb/gw"
	"github.com/ls-2018/etcd_cn/pkg/debugutil"
	"github.com/ls-2018/etcd_cn/pkg/httputil"

	gw "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/soheilhy/cmux"
	"github.com/tmc/grpc-websocket-proxy/wsproxy"
	"go.uber.org/zap"
	"golang.org/x/net/trace"
	"google.golang.org/grpc"
)

// 监听一个端口,提供服务, http, rpc
type serveCtx struct {
	lg       *zap.Logger
	l        net.Listener // 单个监听本地网卡2379端口的listener
	addr     string
	network  string // tcp  unix
	secure   bool   // 安全的
	insecure bool   // 不安全的   // 在处理etcdctl 请求上,是不是启用证书   由  lcurl 的协议决定, 与secure相反

	ctx    context.Context
	cancel context.CancelFunc

	userHandlers    map[string]http.Handler
	serviceRegister func(*grpc.Server) // 预置的服务注册函数扩展
	serversC        chan *servers
}

type servers struct {
	secure bool
	grpc   *grpc.Server
	http   *http.Server
}

// OK
func newServeCtx(lg *zap.Logger) *serveCtx {
	ctx, cancel := context.WithCancel(context.Background())
	if lg == nil {
		lg = zap.NewNop() // 不会输出的logger
	}
	return &serveCtx{
		lg:           lg,
		ctx:          ctx,
		cancel:       cancel,
		userHandlers: make(map[string]http.Handler),
		serversC:     make(chan *servers, 2), // in case sctx.insecure,sctx.secure true
	}
}

// serve 为接收入站请求创建一个goroutine
func (sctx *serveCtx) serve(s *etcdserver.EtcdServer, tlsinfo *transport.TLSInfo, handler http.Handler, errHandler func(error),
	gopts ...grpc.ServerOption,
) (err error) {
	logger := defaultLog.New(ioutil.Discard, "etcdhttp", 0)
	<-s.ReadyNotify() // 准备好了,该channel会被关闭

	sctx.lg.Info("随时准备为客户的要求提供服务")
	// 实例化 连接多路复用器.可以同时解析不同的协议,都跑在一个listener上
	m := cmux.New(sctx.l)
	v3c := v3client.New(s) // server的客户端,可以直接操作server
	servElection := v3election.NewElectionServer(v3c)
	servLock := v3lock.NewLockServer(v3c)

	var gs *grpc.Server
	defer func() {
		if err != nil && gs != nil {
			gs.Stop()
		}
	}()
	// 不安全
	if sctx.insecure {
		gs = v3rpc.Server(s, nil, nil, gopts...) // 注册服务、链接参数
		v3electionpb.RegisterElectionServer(gs, servElection)
		v3lockpb.RegisterLockServer(gs, servLock)
		if sctx.serviceRegister != nil {
			sctx.serviceRegister(gs)
		}
		grpcListener := m.Match(cmux.HTTP2()) //

		go func() { errHandler(gs.Serve(grpcListener)) }()

		var gwmux *gw.ServeMux
		// 启用grpc网关,将 http 转换成 grpc / true
		if s.Cfg.EnableGRPCGateway {
			gwmux, err = sctx.registerGateway([]grpc.DialOption{grpc.WithInsecure()}) // ✅
			if err != nil {
				return err
			}
		}
		// 该handler
		httpmux := sctx.createMux(gwmux, handler) // http->grpc

		srvhttp := &http.Server{
			Handler:  createAccessController(sctx.lg, s, httpmux), // ✅
			ErrorLog: logger,
		}
		httpl := m.Match(cmux.HTTP1())
		go func() { errHandler(srvhttp.Serve(httpl)) }()

		sctx.serversC <- &servers{grpc: gs, http: srvhttp}
		sctx.lg.Info("以不安全的方式为客户流量提供服务;这是被强烈反对的.", zap.String("address", sctx.l.Addr().String()))
	}

	if sctx.secure {
		tlscfg, tlsErr := tlsinfo.ServerConfig()
		if tlsErr != nil {
			return tlsErr
		}
		gs = v3rpc.Server(s, tlscfg, nil, gopts...)
		v3electionpb.RegisterElectionServer(gs, servElection)
		v3lockpb.RegisterLockServer(gs, servLock)
		if sctx.serviceRegister != nil {
			sctx.serviceRegister(gs)
		}
		handler = grpcHandlerFunc(gs, handler)

		var gwmux *gw.ServeMux
		if s.Cfg.EnableGRPCGateway {
			dtls := tlscfg.Clone()
			// trust local etcd
			dtls.InsecureSkipVerify = true
			bundle := credentials.NewBundle(credentials.Config{TLSConfig: dtls})
			opts := []grpc.DialOption{grpc.WithTransportCredentials(bundle.TransportCredentials())}
			gwmux, err = sctx.registerGateway(opts)
			if err != nil {
				return err
			}
		}

		var tlsl net.Listener
		tlsl, err = transport.NewTLSListener(m.Match(cmux.Any()), tlsinfo)
		if err != nil {
			return err
		}
		// TODO: add debug flag; enable logging when debug flag is set
		httpmux := sctx.createMux(gwmux, handler)

		srv := &http.Server{
			Handler:   createAccessController(sctx.lg, s, httpmux),
			TLSConfig: tlscfg,
			ErrorLog:  logger, // do not log user error
		}
		go func() { errHandler(srv.Serve(tlsl)) }()

		sctx.serversC <- &servers{secure: true, grpc: gs, http: srv}
		sctx.lg.Info(
			"serving client traffic securely",
			zap.String("address", sctx.l.Addr().String()),
		)
	}

	close(sctx.serversC)
	return m.Serve()
}

type registerHandlerFunc func(context.Context, *gw.ServeMux, *grpc.ClientConn) error

// 注册网关     http 转换成 grpc / true
func (sctx *serveCtx) registerGateway(opts []grpc.DialOption) (*gw.ServeMux, error) {
	ctx := sctx.ctx

	addr := sctx.addr
	// tcp  unix
	if network := sctx.network; network == "unix" {
		// 明确定义unix网络以支持gRPC套接字
		addr = fmt.Sprintf("%s://%s", network, addr)
	}

	opts = append(opts, grpc.WithDefaultCallOptions([]grpc.CallOption{
		grpc.MaxCallRecvMsgSize(math.MaxInt32),
	}...))
	// 与etcd 建立grpc连接
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	gwmux := gw.NewServeMux()

	handlers := []registerHandlerFunc{
		etcdservergw.RegisterKVHandler, // 将grpc转换成了http
		etcdservergw.RegisterWatchHandler,
		etcdservergw.RegisterLeaseHandler,
		etcdservergw.RegisterClusterHandler,
		etcdservergw.RegisterMaintenanceHandler,
		etcdservergw.RegisterAuthHandler,
		v3lockgw.RegisterLockHandler,
		v3electiongw.RegisterElectionHandler,
	}
	for _, h := range handlers {
		if err := h(ctx, gwmux, conn); err != nil {
			return nil, err
		}
	}
	go func() {
		<-ctx.Done()
		if cerr := conn.Close(); cerr != nil {
			sctx.lg.Warn("关闭连接", zap.String("address", sctx.l.Addr().String()), zap.Error(cerr))
		}
	}()

	return gwmux, nil
}

// OK 将http转换成grpc
func (sctx *serveCtx) createMux(gwmux *gw.ServeMux, handler http.Handler) *http.ServeMux {
	httpmux := http.NewServeMux() // mux 数据选择器
	for path, h := range sctx.userHandlers {
		httpmux.Handle(path, h)
	}

	if gwmux != nil {
		httpmux.Handle(
			"/v3/",
			wsproxy.WebsocketProxy(
				gwmux,
				wsproxy.WithRequestMutator(
					// 默认为流的POST方法
					func(_ *http.Request, outgoing *http.Request) *http.Request {
						outgoing.Method = "POST"
						return outgoing
					},
				),
				wsproxy.WithMaxRespBodyBufferSize(0x7fffffff),
			),
		)
	}
	if handler != nil {
		httpmux.Handle("/", handler)
	}
	return httpmux
}

// createAccessController包装了HTTP多路复用器.
// - 突变gRPC 网关请求路径
// - 检查主机名白名单
// 客户端HTTP请求首先在这里进行
func createAccessController(lg *zap.Logger, s *etcdserver.EtcdServer, mux *http.ServeMux) http.Handler {
	if lg == nil {
		lg = zap.NewNop()
	}
	return &accessController{lg: lg, s: s, mux: mux}
}

type accessController struct {
	lg  *zap.Logger
	s   *etcdserver.EtcdServer //
	mux *http.ServeMux
}

func (ac *accessController) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if req == nil {
		http.Error(rw, "请求是空的", http.StatusBadRequest)
		return
	}
	// 重定向以实现向后兼容
	if req.URL != nil && strings.HasPrefix(req.URL.Path, "/v3beta/") {
		req.URL.Path = strings.Replace(req.URL.Path, "/v3beta/", "/v3/", 1)
	}

	if req.TLS == nil { // 如果客户端连接不安全,则检查origin
		host := httputil.GetHostname(req) // 请求的主机名、域名、IP
		if !ac.s.AccessController.IsHostWhitelisted(host) {
			ac.lg.Warn("拒绝HTTP请求,以防止DNS重新绑定攻击", zap.String("host", host))
			http.Error(rw, errCVE20185702(host), http.StatusMisdirectedRequest)
			return
		}
	} else if ac.s.Cfg.ClientCertAuthEnabled && ac.s.Cfg.EnableGRPCGateway &&
		ac.s.AuthStore().IsAuthEnabled() && strings.HasPrefix(req.URL.Path, "/v3/") {
		// TODO 待看
		for _, chains := range req.TLS.VerifiedChains {
			if len(chains) < 1 {
				continue
			}
			if len(chains[0].Subject.CommonName) != 0 {
				http.Error(rw, "对网关发送请求的客户端的CommonName将被忽略,不按预期使用.", http.StatusBadRequest)
				return
			}
		}
	}

	// 写Origin头
	// 允不允许跨域
	if ac.s.AccessController.OriginAllowed("*") {
		addCORSHeader(rw, "*")
	} else if origin := req.Header.Get("Origin"); ac.s.OriginAllowed(origin) {
		addCORSHeader(rw, origin)
	}

	if req.Method == "OPTIONS" {
		rw.WriteHeader(http.StatusOK)
		return
	}

	ac.mux.ServeHTTP(rw, req)
}

// addCORSHeader 在给定Origin的情况下,添加正确的cors头信息.
func addCORSHeader(w http.ResponseWriter, origin string) {
	w.Header().Add("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Add("Access-Control-Allow-Origin", origin)
	w.Header().Add("Access-Control-Allow-Headers", "accept, content-type, authorization")
}

// https://github.com/transmission/transmission/pull/468
func errCVE20185702(host string) string {
	return fmt.Sprintf(`
etcd received your request, but the Host header was unrecognized.

To fix this, choose one of the following options:
- Enable TLS, then any HTTPS request will be allowed.
- Add the hostname you want to use to the whitelist in settings.
  - e.g. etcd --host-whitelist %q

This requirement has been added to help prevent "DNS Rebinding" attacks (CVE-2018-5702).
`, host)
}

// WrapCORS wraps existing handler with CORS.
// TODO: deprecate this after v2 proxy deprecate
func WrapCORS(cors map[string]struct{}, h http.Handler) http.Handler {
	return &corsHandler{
		ac: &etcdserver.AccessController{CORS: cors},
		h:  h,
	}
}

type corsHandler struct {
	ac *etcdserver.AccessController
	h  http.Handler
}

func (ch *corsHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if ch.ac.OriginAllowed("*") {
		addCORSHeader(rw, "*")
	} else if origin := req.Header.Get("Origin"); ch.ac.OriginAllowed(origin) {
		addCORSHeader(rw, origin)
	}

	if req.Method == "OPTIONS" {
		rw.WriteHeader(http.StatusOK)
		return
	}

	ch.h.ServeHTTP(rw, req)
}

func (sctx *serveCtx) registerUserHandler(s string, h http.Handler) {
	if sctx.userHandlers[s] != nil {
		sctx.lg.Warn("路径已被用户处理程序注册", zap.String("path", s))
		return
	}
	sctx.userHandlers[s] = h
}

func (sctx *serveCtx) registerPprof() {
	for p, h := range debugutil.PProfHandlers() {
		sctx.registerUserHandler(p, h)
	}
}

func (sctx *serveCtx) registerTrace() {
	reqf := func(w http.ResponseWriter, r *http.Request) { trace.Render(w, r, true) }
	sctx.registerUserHandler("/debug/requests", http.HandlerFunc(reqf))
	evf := func(w http.ResponseWriter, r *http.Request) { trace.RenderEvents(w, r, true) }
	sctx.registerUserHandler("/debug/events", http.HandlerFunc(evf))
}

// ----------------------------------------  OVER  --------------------------------------------------------------

// grpcHandlerFunc 返回一个http.Handler,该Handler在接收到gRPC连接时委托给grpcServer,否则返回otherHandler.在gRPC文档中给出.
func grpcHandlerFunc(grpcServer *grpc.Server, otherHandler http.Handler) http.Handler {
	if otherHandler == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			grpcServer.ServeHTTP(w, r)
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
		} else {
			otherHandler.ServeHTTP(w, r)
		}
	})
}
