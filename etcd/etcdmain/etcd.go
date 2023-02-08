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

package etcdmain

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/fileutil"
	"github.com/ls-2018/etcd_cn/client_sdk/pkg/transport"
	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/etcd/embed"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/etcdhttp"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2discovery"
	"github.com/ls-2018/etcd_cn/etcd/proxy/httpproxy"
	pkgioutil "github.com/ls-2018/etcd_cn/pkg/ioutil"
	"github.com/ls-2018/etcd_cn/pkg/osutil"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// 数据目录下的几种子目录
type dirType string

// member、proxy只能存在一种
// 都没有就返回empty[节点运行之初]
var (
	dirMember = dirType("member")
	dirProxy  = dirType("proxy")
	dirEmpty  = dirType("empty")
)

func startEtcdOrProxy(args []string) {
	grpc.EnableTracing = false

	cfg := newConfig()
	defaultInitialCluster := cfg.ec.InitialCluster

	err := cfg.parse(args[1:])
	lg := cfg.ec.GetLogger()
	// 如果我们未能解析整个配置,最好使用配置中已解决的记录器来打印错误,但如果不存在,则创建一个新的临时记录器.
	if lg == nil {
		var zapError error
		lg, zapError = zap.NewProduction()
		if zapError != nil {
			fmt.Printf("创建zap logger失败%v", zapError)
			os.Exit(1)
		}
	}
	lg.Info("运行中:", zap.Strings("args", args))
	if err != nil {
		lg.Warn("未能验证标志", zap.Error(err))
		switch err {
		case embed.ErrUnsetAdvertiseClientURLsFlag:
			lg.Warn("advertise client URLs are not set", zap.Error(err))
		}
		os.Exit(1)
	}
	// err := cfg.ZapLoggerBuilder(cfg)
	cfg.ec.SetupGlobalLoggers()

	defer func() {
		logger := cfg.ec.GetLogger()
		if logger != nil {
			logger.Sync()
		}
	}()
	// TODO 没明白这个函数是干啥的,  防止Name发生变化,InitialCluster没有生效
	defaultHost, dhErr := (&cfg.ec).UpdateDefaultClusterFromName(defaultInitialCluster)
	if defaultHost != "" {
		lg.Info("检测到默认的advertise主机", zap.String("host", defaultHost))
	}
	if dhErr != nil {
		lg.Info("未能检测到默认主机", zap.Error(dhErr))
	}

	if cfg.ec.Dir == "" {
		cfg.ec.Dir = fmt.Sprintf("%v.etcd", cfg.ec.Name)
		lg.Warn("'data-dir'是空的,使用默认的default", zap.String("data-dir", cfg.ec.Dir))
	}

	var stopped <-chan struct{}
	var errc <-chan error
	// 识别数据目录,  返回data dir的类型.
	which := identifyDataDirOrDie(cfg.ec.GetLogger(), cfg.ec.Dir)
	if which != dirEmpty {
		lg.Info("etcd数据已经被初始化了", zap.String("data-dir", cfg.ec.Dir), zap.String("dir-type", string(which)))
		switch which {
		case dirMember:
			stopped, errc, err = startEtcd(&cfg.ec)
		case dirProxy:
			err = startProxy(cfg)
		default:
			lg.Panic("未知目录类型", zap.String("dir-type", string(which)))
		}
	} else {
		shouldProxy := cfg.isProxy() // 是否开启代理模式
		if !shouldProxy {            // 一般不会开启
			stopped, errc, err = startEtcd(&cfg.ec)
			// todo 还没看
			if derr, ok := err.(*etcdserver.DiscoveryError); ok && derr.Err == v2discovery.ErrFullCluster {
				if cfg.shouldFallbackToProxy() {
					lg.Warn("discovery cluster is full, falling back to proxy", zap.String("fallback-proxy", fallbackFlagProxy), zap.Error(err))
					shouldProxy = true
				}
			} else if err != nil {
				lg.Warn("failed to start etcd", zap.Error(err))
			}
		}
		if shouldProxy {
			err = startProxy(cfg)
		}
	}

	if err != nil {
		if derr, ok := err.(*etcdserver.DiscoveryError); ok {
			switch derr.Err {
			case v2discovery.ErrDuplicateID:
				lg.Warn("member has been registered with discovery service", zap.String("name", cfg.ec.Name), zap.String("discovery-token", cfg.ec.Durl), zap.Error(derr.Err))
				lg.Warn("but could not find valid cluster configuration", zap.String("data-dir", cfg.ec.Dir))
				lg.Warn("check data dir if previous bootstrap succeeded")
				lg.Warn("or use a new discovery token if previous bootstrap failed")

			case v2discovery.ErrDuplicateName:
				lg.Warn("member with duplicated name has already been registered", zap.String("discovery-token", cfg.ec.Durl), zap.Error(derr.Err))
				lg.Warn("cURL the discovery token URL for details")
				lg.Warn("do not reuse discovery token; generate a new one to bootstrap a cluster")

			default:
				lg.Warn("failed to bootstrap; discovery token was already used", zap.String("discovery-token", cfg.ec.Durl), zap.Error(err))
				lg.Warn("do not reuse discovery token; generate a new one to bootstrap a cluster")
			}
			os.Exit(1)
		}

		if strings.Contains(err.Error(), "include") && strings.Contains(err.Error(), "--initial-cluster") {
			lg.Warn("failed to start", zap.Error(err))
			if cfg.ec.InitialCluster == cfg.ec.InitialClusterFromName(cfg.ec.Name) {
				lg.Warn("forgot to set --initial-cluster?")
			}
			if types.URLs(cfg.ec.APUrls).String() == embed.DefaultInitialAdvertisePeerURLs {
				lg.Warn("forgot to set --initial-advertise-peer-urls?")
			}
			if cfg.ec.InitialCluster == cfg.ec.InitialClusterFromName(cfg.ec.Name) && len(cfg.ec.Durl) == 0 {
				lg.Warn("--discovery flag is not set")
			}
			os.Exit(1)
		}
		lg.Fatal("discovery failed", zap.Error(err))
	}

	osutil.HandleInterrupts(lg)

	// At this point, the initialization of etcd is done.
	// The listeners are listening on the TCP ports and ready
	// for accepting connections. The etcd instance should be
	// joined with the cluster and ready to serve incoming
	// connections.
	notifySystemd(lg)

	select {
	case lerr := <-errc:
		// fatal out on listener errors
		lg.Fatal("listener failed", zap.Error(lerr))
	case <-stopped:
	}

	osutil.Exit(0)
}

// startEtcd
func startEtcd(cfg *embed.Config) (<-chan struct{}, <-chan error, error) {
	e, err := embed.StartEtcd(cfg) // 异步启动etcd| http
	if err != nil {
		return nil, nil, err
	}
	osutil.RegisterInterruptHandler(e.Close) // 注册中断处理程序,但不会执行
	select {
	case <-e.Server.ReadyNotify(): // 等待本节点加入集群
	case <-e.Server.StopNotify(): // 收到了异常
	}
	return e.Server.StopNotify(), e.Err(), nil
}

// startProxy launches an HTTP proxy for client communication which proxies to other etcd nodes.
func startProxy(cfg *config) error {
	lg := cfg.ec.GetLogger()
	lg.Info("v2 API proxy starting")

	clientTLSInfo := cfg.ec.ClientTLSInfo
	if clientTLSInfo.Empty() {
		// Support old proxy behavior of defaulting to PeerTLSInfo
		// for both client and peer connections.
		clientTLSInfo = cfg.ec.PeerTLSInfo
	}
	clientTLSInfo.InsecureSkipVerify = cfg.ec.ClientAutoTLS
	cfg.ec.PeerTLSInfo.InsecureSkipVerify = cfg.ec.PeerAutoTLS

	pt, err := transport.NewTimeoutTransport(
		clientTLSInfo,
		time.Duration(cfg.cp.ProxyDialTimeoutMs)*time.Millisecond,
		time.Duration(cfg.cp.ProxyReadTimeoutMs)*time.Millisecond,
		time.Duration(cfg.cp.ProxyWriteTimeoutMs)*time.Millisecond,
	)
	if err != nil {
		return err
	}
	pt.MaxIdleConnsPerHost = httpproxy.DefaultMaxIdleConnsPerHost

	if err = cfg.ec.PeerSelfCert(); err != nil {
		lg.Fatal("failed to get self-signed certs for peer", zap.Error(err))
	}
	tr, err := transport.NewTimeoutTransport(
		cfg.ec.PeerTLSInfo,
		time.Duration(cfg.cp.ProxyDialTimeoutMs)*time.Millisecond,
		time.Duration(cfg.cp.ProxyReadTimeoutMs)*time.Millisecond,
		time.Duration(cfg.cp.ProxyWriteTimeoutMs)*time.Millisecond,
	)
	if err != nil {
		return err
	}

	cfg.ec.Dir = filepath.Join(cfg.ec.Dir, "proxy")
	err = fileutil.TouchDirAll(cfg.ec.Dir)
	if err != nil {
		return err
	}

	var peerURLs []string
	clusterfile := filepath.Join(cfg.ec.Dir, "cluster")

	b, err := ioutil.ReadFile(clusterfile)
	switch {
	case err == nil:
		if cfg.ec.Durl != "" {
			lg.Warn(
				"discovery token ignored since the proxy has already been initialized; valid cluster file found",
				zap.String("cluster-file", clusterfile),
			)
		}
		if cfg.ec.DNSCluster != "" {
			lg.Warn(
				"DNS SRV discovery ignored since the proxy has already been initialized; valid cluster file found",
				zap.String("cluster-file", clusterfile),
			)
		}
		urls := struct{ PeerURLs []string }{}
		err = json.Unmarshal(b, &urls)
		if err != nil {
			return err
		}
		peerURLs = urls.PeerURLs
		lg.Info(
			"proxy using peer URLS from cluster file",
			zap.Strings("peer-urls", peerURLs),
			zap.String("cluster-file", clusterfile),
		)

	case os.IsNotExist(err):
		var urlsmap types.URLsMap
		urlsmap, _, err = cfg.ec.PeerURLsMapAndToken("proxy")
		if err != nil {
			return fmt.Errorf("error setting up initial cluster: %v", err)
		}

		if cfg.ec.Durl != "" {
			var s string
			s, err = v2discovery.GetCluster(lg, cfg.ec.Durl, cfg.ec.Dproxy)
			if err != nil {
				return err
			}
			if urlsmap, err = types.NewURLsMap(s); err != nil {
				return err
			}
		}
		peerURLs = urlsmap.URLs()
		lg.Info("proxy using peer URLS", zap.Strings("peer-urls", peerURLs))

	default:
		return err
	}

	clientURLs := []string{}
	uf := func() []string {
		gcls, gerr := etcdserver.GetClusterFromRemotePeers(lg, peerURLs, tr)
		if gerr != nil {
			lg.Warn(
				"failed to get cluster from remote peers",
				zap.Strings("peer-urls", peerURLs),
				zap.Error(gerr),
			)
			return []string{}
		}

		clientURLs = gcls.ClientURLs()
		urls := struct{ PeerURLs []string }{gcls.PeerURLs()}
		b, jerr := json.Marshal(urls)
		if jerr != nil {
			lg.Warn("proxy failed to marshal peer URLs", zap.Error(jerr))
			return clientURLs
		}

		err = pkgioutil.WriteAndSyncFile(clusterfile+".bak", b, 0o600)
		if err != nil {
			lg.Warn("proxy failed to write cluster file", zap.Error(err))
			return clientURLs
		}
		err = os.Rename(clusterfile+".bak", clusterfile)
		if err != nil {
			lg.Warn(
				"proxy failed to rename cluster file",
				zap.String("path", clusterfile),
				zap.Error(err),
			)
			return clientURLs
		}
		if !reflect.DeepEqual(gcls.PeerURLs(), peerURLs) {
			lg.Info(
				"proxy updated peer URLs",
				zap.Strings("from", peerURLs),
				zap.Strings("to", gcls.PeerURLs()),
			)
		}
		peerURLs = gcls.PeerURLs()

		return clientURLs
	}
	ph := httpproxy.NewHandler(lg, pt, uf, time.Duration(cfg.cp.ProxyFailureWaitMs)*time.Millisecond, time.Duration(cfg.cp.ProxyRefreshIntervalMs)*time.Millisecond)
	ph = embed.WrapCORS(cfg.ec.CORS, ph)

	if cfg.isReadonlyProxy() {
		ph = httpproxy.NewReadonlyHandler(ph)
	}

	// setup self signed certs when serving https
	cHosts, cTLS := []string{}, false
	for _, u := range cfg.ec.LCUrls {
		cHosts = append(cHosts, u.Host)
		cTLS = cTLS || u.Scheme == "https"
	}
	for _, u := range cfg.ec.ACUrls {
		cHosts = append(cHosts, u.Host)
		cTLS = cTLS || u.Scheme == "https"
	}
	listenerTLS := cfg.ec.ClientTLSInfo
	if cfg.ec.ClientAutoTLS && cTLS {
		listenerTLS, err = transport.SelfCert(cfg.ec.GetLogger(), filepath.Join(cfg.ec.Dir, "clientCerts"), cHosts, cfg.ec.SelfSignedCertValidity)
		if err != nil {
			lg.Fatal("failed to initialize self-signed client cert", zap.Error(err))
		}
	}

	// Start a proxy etcd goroutine for each listen address
	for _, u := range cfg.ec.LCUrls {
		l, err := transport.NewListener(u.Host, u.Scheme, &listenerTLS)
		if err != nil {
			return err
		}

		host := u.String()
		go func() {
			lg.Info("v2 proxy started listening on client requests", zap.String("host", host))
			mux := http.NewServeMux()
			etcdhttp.HandlePrometheus(mux) // v2 proxy just uses the same port
			mux.Handle("/", ph)
			lg.Fatal("done serving", zap.Error(http.Serve(l, mux)))
		}()
	}
	return nil
}

// identifyDataDirOrDie 识别数据目录,  返回data dir的类型. 如果datadir无效,则视为无效.
func identifyDataDirOrDie(lg *zap.Logger, dir string) dirType {
	names, err := fileutil.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return dirEmpty
		}
		lg.Fatal("未能列出数据目录", zap.String("dir", dir), zap.Error(err))
	}

	var m, p bool
	for _, name := range names {
		switch dirType(name) {
		case dirMember:
			m = true
		case dirProxy:
			p = true
		default:
			lg.Warn("在数据目录下发现无效的文件", zap.String("filename", name), zap.String("data-dir", dir))
		}
	}

	if m && p {
		lg.Fatal("无效的数据目录,成员目录和代理目录都存在")
	}
	if m {
		return dirMember
	}
	if p {
		return dirProxy
	}
	return dirEmpty
}

// 检查系统是否支持
func checkSupportArch() {
	if runtime.GOARCH == "amd64" ||
		runtime.GOARCH == "arm64" ||
		runtime.GOARCH == "ppc64le" ||
		runtime.GOARCH == "s390x" {
		return
	}
	// 不支持的架构 仅通过环境变量配置,因此在这里取消设置不通过解析标志
	defer os.Unsetenv("ETCD_UNSUPPORTED_ARCH")
	if env, ok := os.LookupEnv("ETCD_UNSUPPORTED_ARCH"); ok && env == runtime.GOARCH {
		fmt.Printf("在不支持的体系结构上运行etcd%q 当 ETCD_UNSUPPORTED_ARCH 设置了\n", env)
		return
	}

	fmt.Printf("etcd在不支持ETCD_UNSUPPORTED_ARCH的平台上=%s set\n", runtime.GOARCH)
	os.Exit(1)
}
