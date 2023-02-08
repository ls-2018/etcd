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

// Every change should be reflected on help.go as well.

package etcdmain

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/logutil"
	cconfig "github.com/ls-2018/etcd_cn/etcd/config"
	"github.com/ls-2018/etcd_cn/etcd/embed"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/rafthttp"
	"github.com/ls-2018/etcd_cn/offical/api/v3/version"
	"github.com/ls-2018/etcd_cn/pkg/flags"

	"go.uber.org/zap"
	"sigs.k8s.io/yaml"
)

var (
	proxyFlagOff      = "off"
	proxyFlagReadonly = "readonly"
	proxyFlagOn       = "on"

	fallbackFlagExit  = "exit"
	fallbackFlagProxy = "proxy"

	ignored = []string{
		"cluster-active-size",
		"cluster-remove-delay",
		"cluster-sync-interval",
		"config",
		"force",
		"max-result-buffer",
		"max-retry-attempts",
		"peer-heartbeat-interval",
		"peer-election-timeout",
		"retry-interval",
		"snapshot",
		"v",
		"vv",
		// for coverage testing
		"test.coverprofile",
		"test.outputdir",
	}
)

type configProxy struct {
	ProxyFailureWaitMs     uint `json:"proxy-failure-wait"` // 在重新考虑代理请求之前.endpoints 将处于失败状态的时间(以毫秒为单位).
	ProxyRefreshIntervalMs uint `json:"proxy-refresh-interval"`
	ProxyDialTimeoutMs     uint `json:"proxy-dial-timeout"`
	ProxyWriteTimeoutMs    uint `json:"proxy-write-timeout"`
	ProxyReadTimeoutMs     uint `json:"proxy-read-timeout"`
	Fallback               string
	Proxy                  string
	ProxyJSON              string `json:"proxy"`
	FallbackJSON           string `json:"discovery-fallback"`
}

// configFlags 是否有一组标志用于命令行解析配置
type configFlags struct {
	flagSet       *flag.FlagSet
	clusterState  *flags.SelectiveStringValue // todo 设置new为初始静态或DNS引导期间出现的所有成员.如果将此选项设置为existing.则etcd将尝试加入现有群集.
	fallback      *flags.SelectiveStringValue
	proxy         *flags.SelectiveStringValue
	v2deprecation *flags.SelectiveStringsValue
}

// config 保存etcd命令行调用的配置
type config struct {
	ec           embed.Config
	cp           configProxy // 代理配置
	cf           configFlags // 是否有一组标志用于命令行解析配置
	configFile   string      // 从文件加载服务器配置.
	printVersion bool        // 打印版本并退出
	ignored      []string
}

// OK
func newConfig() *config {
	cfg := &config{
		ec: *embed.NewConfig(),
		cp: configProxy{
			Proxy:                  proxyFlagOff, // off
			ProxyFailureWaitMs:     5000,
			ProxyRefreshIntervalMs: 30000,
			ProxyDialTimeoutMs:     1000,
			ProxyWriteTimeoutMs:    5000,
		},
		ignored: ignored,
	}
	cfg.cf = configFlags{
		flagSet: flag.NewFlagSet("etcd", flag.ContinueOnError),
		clusterState: flags.NewSelectiveStringValue(
			embed.ClusterStateFlagNew,
			embed.ClusterStateFlagExisting,
		),
		fallback: flags.NewSelectiveStringValue(
			fallbackFlagProxy,
			fallbackFlagExit,
		),
		proxy: flags.NewSelectiveStringValue(
			proxyFlagOff,      // off
			proxyFlagReadonly, // readonly
			proxyFlagOn,       // on
		),
		v2deprecation: flags.NewSelectiveStringsValue(
			string(cconfig.V2_DEPR_0_NOT_YET),
			string(cconfig.V2_DEPR_1_WRITE_ONLY),
			string(cconfig.V2_DEPR_1_WRITE_ONLY_DROP),
			string(cconfig.V2_DEPR_2_GONE)),
	}

	fs := cfg.cf.flagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, usageline)
	}

	fs.StringVar(&cfg.configFile, "config-file", "", "从文件加载服务器配置.")

	// member
	fs.StringVar(&cfg.ec.Dir, "data-dir", cfg.ec.Dir, "服务运行数据保存的路径. ${name}.etcd")
	fs.StringVar(&cfg.ec.WalDir, "wal-dir", cfg.ec.WalDir, "专用wal目录的路径.默认值:--data-dir的路径下")
	fs.Var(flags.NewUniqueURLsWithExceptions(embed.DefaultListenPeerURLs, ""), "listen-peer-urls", "和成员之间通信的地址.用于监听其他etcd member的url")
	fs.Var(flags.NewUniqueURLsWithExceptions(embed.DefaultListenClientURLs, ""), "listen-client-urls", "对外提供服务的地址")
	fs.Var(flags.NewUniqueURLsWithExceptions("", ""), "listen-metrics-urls", "要监听指标和运行状况端点的url列表.")
	fs.UintVar(&cfg.ec.MaxSnapFiles, "max-snapshots", cfg.ec.MaxSnapFiles, "要保留的最大快照文件数(0表示不受限制).5")
	fs.UintVar(&cfg.ec.MaxWalFiles, "max-wals", cfg.ec.MaxWalFiles, "要保留的最大wal文件数(0表示不受限制). 5")
	fs.StringVar(&cfg.ec.Name, "name", cfg.ec.Name, "本节点.人类可读的名字")
	// 作用:此配置值作为此节点在--initial-cluster标志中列出的条目(例如.default=http://localhost:2380)引用.若使用静态引导.则需要匹配标志中使用的密钥.使用发现时.每个成员必须具有唯一的名称.建议使用Hostname或者machine-id.
	fs.Uint64Var(&cfg.ec.SnapshotCount, "snapshot-count", cfg.ec.SnapshotCount, "// 触发一次磁盘快照的提交事务的次数.")
	fs.UintVar(&cfg.ec.TickMs, "heartbeat-interval", cfg.ec.TickMs, "心跳间隔 100ms")
	fs.UintVar(&cfg.ec.ElectionMs, "election-timeout", cfg.ec.ElectionMs, "选举超时")
	fs.BoolVar(&cfg.ec.InitialElectionTickAdvance, "initial-election-tick-advance", cfg.ec.InitialElectionTickAdvance, "是否提前初始化选举时钟启动,以便更快的选举.")
	fs.Int64Var(&cfg.ec.QuotaBackendBytes, "quota-backend-bytes", cfg.ec.QuotaBackendBytes, "当后端大小超过给定配额时(0默认为低空间配额).引发警报.")
	fs.StringVar(&cfg.ec.BackendFreelistType, "backend-bbolt-freelist-type", cfg.ec.BackendFreelistType, "BackendFreelistType指定boltdb后端使用的freelist的类型(array and map是支持的类型). map ")
	fs.DurationVar(&cfg.ec.BoltBackendBatchInterval, "backend-batch-interval", cfg.ec.BoltBackendBatchInterval, "BackendBatchInterval是提交后端事务前的最长时间.")
	fs.IntVar(&cfg.ec.BoltBackendBatchLimit, "backend-batch-limit", cfg.ec.BoltBackendBatchLimit, "BackendBatchLimit是提交后端事务前的最大操作数.")
	fs.UintVar(&cfg.ec.MaxTxnOps, "max-txn-ops", cfg.ec.MaxTxnOps, "事务中允许的最大操作数.")
	fs.UintVar(&cfg.ec.MaxRequestBytes, "max-request-bytes", cfg.ec.MaxRequestBytes, "服务器将接受的最大客户端请求大小(字节).")
	fs.DurationVar(&cfg.ec.GRPCKeepAliveMinTime, "grpc-keepalive-min-time", cfg.ec.GRPCKeepAliveMinTime, "客户端在ping服务器之前应等待的最短持续时间间隔.")
	fs.DurationVar(&cfg.ec.GRPCKeepAliveInterval, "grpc-keepalive-interval", cfg.ec.GRPCKeepAliveInterval, "服务器到客户端ping的频率持续时间.以检查连接是否处于活动状态(0表示禁用).")
	fs.DurationVar(&cfg.ec.GRPCKeepAliveTimeout, "grpc-keepalive-timeout", cfg.ec.GRPCKeepAliveTimeout, "关闭非响应连接之前的额外持续等待时间(0表示禁用).20s")
	fs.BoolVar(&cfg.ec.SocketOpts.ReusePort, "socket-reuse-port", cfg.ec.SocketOpts.ReusePort, "启用在listener上设置套接字选项SO_REUSEPORT.允许重新绑定一个已经在使用的端口.false")
	fs.BoolVar(&cfg.ec.SocketOpts.ReuseAddress, "socket-reuse-address", cfg.ec.SocketOpts.ReuseAddress, "启用在listener上设置套接字选项SO_REUSEADDR 允许重新绑定一个已经在使用的端口 在`TIME_WAIT` 状态.")

	// raft 连接超时
	fs.DurationVar(&rafthttp.ConnReadTimeout, "raft-read-timeout", rafthttp.DefaultConnReadTimeout, "在每个rafthttp连接上设置的读取超时 5s")
	fs.DurationVar(&rafthttp.ConnWriteTimeout, "raft-write-timeout", rafthttp.DefaultConnWriteTimeout, "在每个rafthttp连接上设置写入超时 5s")

	// 集群
	fs.Var(flags.NewUniqueURLsWithExceptions(embed.DefaultInitialAdvertisePeerURLs, ""), "initial-advertise-peer-urls", "集群成员的 URL地址.且会通告群集的其余成员节点.")
	fs.Var(flags.NewUniqueURLsWithExceptions(embed.DefaultAdvertiseClientURLs, ""), "advertise-client-urls", "就是客户端(etcdctl/curl等)跟etcd服务进行交互时请求的url")
	// 注意,不能写http://localhost:237,这样就是通知其他节点,可以用localhost访问,将导致ectd的客户端用localhost访问本地,导致访问不通.还有一个更可怕情况,ectd布置了代理层,代理层将一直通过locahost访问自己的代理接口,导致无限循环
	fs.StringVar(&cfg.ec.Durl, "discovery", cfg.ec.Durl, "用于引导群集的发现URL.")
	fs.Var(cfg.cf.fallback, "discovery-fallback", fmt.Sprintf(`发现服务失败时的预期行为("退出"或"代理")."proxy"仅支持v2 API. %q`, cfg.cf.fallback.Valids()))

	fs.StringVar(&cfg.ec.Dproxy, "discovery-proxy", cfg.ec.Dproxy, "用于流量到发现服务的HTTP代理.")
	fs.StringVar(&cfg.ec.DNSCluster, "discovery-srv", cfg.ec.DNSCluster, "DNS srv域用于引导群集.")
	fs.StringVar(&cfg.ec.DNSClusterServiceName, "discovery-srv-name", cfg.ec.DNSClusterServiceName, "使用DNS引导时查询的DNS srv名称的后缀.")
	fs.StringVar(&cfg.ec.InitialCluster, "initial-cluster", cfg.ec.InitialCluster, "用于引导初始集群配置,集群中所有节点的信息..")
	fs.StringVar(&cfg.ec.InitialClusterToken, "initial-cluster-token", cfg.ec.InitialClusterToken, "创建集群的 token.这个值每个集群保持唯一.")
	fs.Var(cfg.cf.clusterState, "initial-cluster-state", "初始集群状态 ('new' or 'existing').")

	fs.BoolVar(&cfg.ec.StrictReconfigCheck, "strict-reconfig-check", cfg.ec.StrictReconfigCheck, "拒绝可能导致仲裁丢失的重新配置请求.true")

	fs.BoolVar(&cfg.ec.PreVote, "pre-vote", cfg.ec.PreVote, "是否启用PreVote扩展,解决分区恢复选举bug")

	fs.StringVar(&cfg.ec.ExperimentalEnableV2V3, "experimental-enable-v2v3", cfg.ec.ExperimentalEnableV2V3, "v3 prefix for serving emulated v2 state. Deprecated in 3.5. Will be decomissioned in 3.6.")
	fs.Var(cfg.cf.v2deprecation, "v2-deprecation", fmt.Sprintf("v2store deprecation stage: %q. ", cfg.cf.proxy.Valids())) // off readonly on

	// proxy
	fs.Var(cfg.cf.proxy, "proxy", fmt.Sprintf("代理模式设置  %q", cfg.cf.proxy.Valids()))
	fs.UintVar(&cfg.cp.ProxyFailureWaitMs, "proxy-failure-wait", cfg.cp.ProxyFailureWaitMs, "在重新考虑代理请求之前.endpoints 将处于失败状态的时间(以毫秒为单位).")
	fs.UintVar(&cfg.cp.ProxyRefreshIntervalMs, "proxy-refresh-interval", cfg.cp.ProxyRefreshIntervalMs, "endpoints 刷新间隔的时间(以毫秒为单位).")
	fs.UintVar(&cfg.cp.ProxyDialTimeoutMs, "proxy-dial-timeout", cfg.cp.ProxyDialTimeoutMs, "拨号超时的时间(以毫秒为单位)或0表示禁用超时")
	fs.UintVar(&cfg.cp.ProxyWriteTimeoutMs, "proxy-write-timeout", cfg.cp.ProxyWriteTimeoutMs, "写入超时的时间(以毫秒为单位)或0以禁用超时.")
	fs.UintVar(&cfg.cp.ProxyReadTimeoutMs, "proxy-read-timeout", cfg.cp.ProxyReadTimeoutMs, "读取超时的时间(以毫秒为单位)或0以禁用超时.")

	// etcdctl通信的证书配置
	fs.StringVar(&cfg.ec.ClientTLSInfo.CertFile, "cert-file", "", "客户端证书")
	fs.StringVar(&cfg.ec.ClientTLSInfo.KeyFile, "key-file", "", "客户端私钥")

	fs.StringVar(&cfg.ec.ClientTLSInfo.ClientCertFile, "client-cert-file", "", "验证client客户端时使用的 证书文件路径,否则在需要客户认证时将使用cert-file文件")
	fs.StringVar(&cfg.ec.ClientTLSInfo.ClientKeyFile, "client-key-file", "", "验证client客户端时使用的 密钥文件路径,否则在需要客户认证时将使用key-file文件.")
	fs.BoolVar(&cfg.ec.ClientTLSInfo.ClientCertAuth, "client-cert-auth", false, "启用客户端证书验证;默认false")
	fs.StringVar(&cfg.ec.ClientTLSInfo.CRLFile, "client-crl-file", "", "客户端证书吊销列表文件的路径.")
	fs.StringVar(&cfg.ec.ClientTLSInfo.AllowedHostname, "client-cert-allowed-hostname", "", "允许客户端证书认证使用TLS主机名.")
	fs.StringVar(&cfg.ec.ClientTLSInfo.TrustedCAFile, "trusted-ca-file", "", "客户端etcd通信 的可信CA证书文件")
	fs.BoolVar(&cfg.ec.ClientAutoTLS, "auto-tls", false, "客户端TLS使用自动生成的证书")
	// etcd通信之间的证书配置
	fs.StringVar(&cfg.ec.PeerTLSInfo.CertFile, "peer-cert-file", "", "证书路径")
	fs.StringVar(&cfg.ec.PeerTLSInfo.KeyFile, "peer-key-file", "", "私钥路径")

	fs.StringVar(&cfg.ec.PeerTLSInfo.ClientCertFile, "peer-client-cert-file", "", "验证server客户端时使用的 证书文件路径,否则在需要客户认证时将使用cert-file文件")
	fs.StringVar(&cfg.ec.PeerTLSInfo.ClientKeyFile, "peer-client-key-file", "", "验证server客户端时使用的 密钥文件路径,否则在需要客户认证时将使用key-file文件.")

	fs.BoolVar(&cfg.ec.PeerTLSInfo.ClientCertAuth, "peer-client-cert-auth", false, "启用server客户端证书验证;默认false")
	fs.StringVar(&cfg.ec.PeerTLSInfo.TrustedCAFile, "peer-trusted-ca-file", "", "服务器端ca证书")
	fs.BoolVar(&cfg.ec.PeerAutoTLS, "peer-auto-tls", false, "节点之间使用生成的证书通信;默认false")
	fs.UintVar(&cfg.ec.SelfSignedCertValidity, "self-signed-cert-validity", 1, "客户端证书和同级证书的有效期,单位为年 ;etcd自动生成的 如果指定了ClientAutoTLS and PeerAutoTLS,")
	fs.StringVar(&cfg.ec.PeerTLSInfo.CRLFile, "peer-crl-file", "", "服务端证书吊销列表文件的路径.")
	fs.StringVar(&cfg.ec.PeerTLSInfo.AllowedCN, "peer-cert-allowed-cn", "", "允许的server客户端证书CommonName")
	fs.StringVar(&cfg.ec.PeerTLSInfo.AllowedHostname, "peer-cert-allowed-hostname", "", "允许的server客户端证书hostname")
	fs.Var(flags.NewStringsValue(""), "cipher-suites", "客户端/etcds之间支持的TLS加密套件的逗号分隔列表(空将由Go自动填充).")
	fs.BoolVar(&cfg.ec.PeerTLSInfo.SkipClientSANVerify, "experimental-peer-skip-client-san-verification", false, "跳过server 客户端证书中SAN字段的验证.默认false")

	fs.Var(flags.NewUniqueURLsWithExceptions("*", "*"), "cors", "逗号分隔的CORS白名单.或跨来源资源共享.(空或*表示允许所有)")
	fs.Var(flags.NewUniqueStringsValue("*"), "host-whitelist", "如果etcd是不安全的(空意味着允许所有).用逗号分隔HTTP客户端请求中的可接受主机名.")

	// 日志
	fs.StringVar(&cfg.ec.Logger, "logger", "zap", "当前只支持zap,结构化数据")
	fs.Var(flags.NewUniqueStringsValue(embed.DefaultLogOutput), "log-outputs", "指定'stdout'或'stderr'以跳过日志记录,即使在systemd或逗号分隔的输出目标列表下运行也是如此.")
	fs.StringVar(&cfg.ec.LogLevel, "log-level", logutil.DefaultLogLevel, "日志等级,只支持 debug, info, warn, error, panic, or fatal. Default 'info'.")
	fs.BoolVar(&cfg.ec.EnableLogRotation, "enable-log-rotation", false, "启用单个日志输出文件目标的日志旋转.")
	fs.StringVar(&cfg.ec.LogRotationConfigJSON, "log-rotation-config-json", embed.DefaultLogRotationConfig, "是用于日志轮换的默认配置. 默认情况下,日志轮换是禁用的.")

	// 版本
	fs.BoolVar(&cfg.printVersion, "version", false, "打印版本并退出.")
	//--auto-compaction-mode=revision --auto-compaction-retention=1000 每5分钟自动压缩"latest revision" - 1000;
	//--auto-compaction-mode=periodic --auto-compaction-retention=12h 每1小时自动压缩并保留12小时窗口.
	fs.StringVar(&cfg.ec.AutoCompactionRetention, "auto-compaction-retention", "0", "在一个小时内为mvcc键值存储的自动压缩.0表示禁用自动压缩.")
	fs.StringVar(&cfg.ec.AutoCompactionMode, "auto-compaction-mode", "periodic", "基于时间保留的三种模式:periodic, revision")

	// 性能分析器 通过 HTTP
	fs.BoolVar(&cfg.ec.EnablePprof, "enable-pprof", false, `通过HTTP服务器启用运行时分析数据.地址位于客户端URL +/debug/pprof/`)

	// additional metrics
	fs.StringVar(&cfg.ec.Metrics, "metrics", cfg.ec.Metrics, `设置导出的指标的详细程度,指定"扩展"以包括直方图指标(extensive,basic)`)

	// experimental distributed tracing
	fs.BoolVar(&cfg.ec.ExperimentalEnableDistributedTracing, "experimental-enable-distributed-tracing", false, "Enable experimental distributed  tracing using OpenTelemetry Tracing.")
	fs.StringVar(&cfg.ec.ExperimentalDistributedTracingAddress, "experimental-distributed-tracing-address", embed.ExperimentalDistributedTracingAddress, "Address for distributed tracing used for OpenTelemetry Tracing (if enabled with experimental-enable-distributed-tracing flag).")
	fs.StringVar(&cfg.ec.ExperimentalDistributedTracingServiceName, "experimental-distributed-tracing-service-name", embed.ExperimentalDistributedTracingServiceName, "Configures service name for distributed tracing to be used to define service name for OpenTelemetry Tracing (if enabled with experimental-enable-distributed-tracing flag). 'etcd' is the default service name. Use the same service name for all instances of etcd.")
	fs.StringVar(&cfg.ec.ExperimentalDistributedTracingServiceInstanceID, "experimental-distributed-tracing-instance-id", "", "Configures service instance ID for distributed tracing to be used to define service instance ID key for OpenTelemetry Tracing (if enabled with experimental-enable-distributed-tracing flag). There is no default value set. This ID必须是unique per etcd instance.")

	// auth
	fs.StringVar(&cfg.ec.AuthToken, "auth-token", cfg.ec.AuthToken, "指定验证令牌的具体选项. ('simple' or 'jwt')")
	fs.UintVar(&cfg.ec.BcryptCost, "bcrypt-cost", cfg.ec.BcryptCost, "为散列身份验证密码指定bcrypt算法的成本/强度.有效值介于4和31之间.")
	fs.UintVar(&cfg.ec.AuthTokenTTL, "auth-token-ttl", cfg.ec.AuthTokenTTL, "token过期时间")

	// gateway
	fs.BoolVar(&cfg.ec.EnableGRPCGateway, "enable-grpc-gateway", cfg.ec.EnableGRPCGateway, "Enable GRPC gateway.")

	// experimental
	fs.BoolVar(&cfg.ec.ExperimentalInitialCorruptCheck, "experimental-initial-corrupt-check", cfg.ec.ExperimentalInitialCorruptCheck, "Enable to check data corruption before serving any client/peer traffic.")
	fs.DurationVar(&cfg.ec.ExperimentalCorruptCheckTime, "experimental-corrupt-check-time", cfg.ec.ExperimentalCorruptCheckTime, "Duration of time between cluster corruption check passes.")

	fs.BoolVar(&cfg.ec.ExperimentalEnableLeaseCheckpoint, "experimental-enable-lease-checkpoint", true, "允许leader定期向其他成员发送检查点,以防止leader变化时剩余TTL重置")
	// TODO: delete in v3.7
	fs.BoolVar(&cfg.ec.ExperimentalEnableLeaseCheckpointPersist, "experimental-enable-lease-checkpoint-persist", true, "启用持续的剩余TTL,以防止长期租赁的无限期自动续约.在v3.6中始终启用.应使用该功能以确保从启用该功能的v3.5集群顺利升级.需要启用experimental-enable-lease-checkpoint.")
	fs.IntVar(&cfg.ec.ExperimentalCompactionBatchLimit, "experimental-compaction-batch-limit", cfg.ec.ExperimentalCompactionBatchLimit, "Sets the maximum revisions deleted in each compaction batch.")
	fs.DurationVar(&cfg.ec.ExperimentalWatchProgressNotifyInterval, "experimental-watch-progress-notify-interval", cfg.ec.ExperimentalWatchProgressNotifyInterval, "Duration of periodic watch progress notifications.")
	fs.DurationVar(&cfg.ec.ExperimentalDowngradeCheckTime, "experimental-downgrade-check-time", cfg.ec.ExperimentalDowngradeCheckTime, "两次降级状态检查之间的时间间隔.")
	fs.DurationVar(&cfg.ec.ExperimentalWarningApplyDuration, "experimental-warning-apply-duration", cfg.ec.ExperimentalWarningApplyDuration, "时间长度.如果应用请求的时间超过这个值.就会产生一个警告.")
	fs.BoolVar(&cfg.ec.ExperimentalMemoryMlock, "experimental-memory-mlock", cfg.ec.ExperimentalMemoryMlock, "启用强制执行etcd页面(特别是bbolt)留在RAM中.")
	fs.BoolVar(&cfg.ec.ExperimentalTxnModeWriteWithSharedBuffer, "experimental-txn-mode-write-with-shared-buffer", true, "启用写事务在其只读检查操作中使用共享缓冲区.")
	fs.UintVar(&cfg.ec.ExperimentalBootstrapDefragThresholdMegabytes, "experimental-bootstrap-defrag-threshold-megabytes", 0, "Enable the defrag during etcd etcd bootstrap on condition that it will free at least the provided threshold of disk space. Needs to be set to non-zero value to take effect.")

	// 非安全
	fs.BoolVar(&cfg.ec.UnsafeNoFsync, "unsafe-no-fsync", false, "禁用fsync,不安全,会导致数据丢失.")
	fs.BoolVar(&cfg.ec.ForceNewCluster, "force-new-cluster", false, "强制创建新的单成员群集.它提交配置更改,强制删除集群中的所有现有成员并添加自身.需要将其设置为还原备份.")

	// ignored
	for _, f := range cfg.ignored {
		fs.Var(&flags.IgnoredFlag{Name: f}, f, "")
	}
	return cfg
}

// OK
func (cfg *config) parse(arguments []string) error {
	perr := cfg.cf.flagSet.Parse(arguments)
	switch perr {
	case nil:
	case flag.ErrHelp:
		fmt.Println(flagsline)
		os.Exit(0)
	default:
		os.Exit(2)
	}
	if len(cfg.cf.flagSet.Args()) != 0 {
		return fmt.Errorf("'%s'不是一个有效的标志 ", cfg.cf.flagSet.Arg(0))
	}

	if cfg.printVersion {
		fmt.Printf("etcd Version: %s\n", version.Version)
		fmt.Printf("Git SHA: %s\n", version.GitSHA)
		fmt.Printf("Go Version: %s\n", runtime.Version())
		fmt.Printf("Go OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
		os.Exit(0)
	}

	var err error

	// 这个env变量必须被单独解析,因为我们需要根据配置文件是否被设置,来决定是使用还是忽略env变量.
	if cfg.configFile == "" {
		cfg.configFile = os.Getenv(flags.FlagToEnv("ETCD", "config-file")) // ETCD_CONFIG_FILE
	}

	if cfg.configFile != "" {
		err = cfg.configFromFile(cfg.configFile)
		if lg := cfg.ec.GetLogger(); lg != nil {
			lg.Info("加载的etcd配置,其他配置的命令行标志和环境变量将被忽略,如果提供了", zap.String("path", cfg.configFile))
		}
	} else {
		err = cfg.configFromCmdLine()
	}
	if runtime.GOOS == "windows" {
		fmt.Println(os.RemoveAll(fmt.Sprintf("E:\\etcd_cn\\%s.etcd", cfg.ec.Name)))
	} else {
		fmt.Println(os.RemoveAll(fmt.Sprintf("/Users/liushuo/Desktop/source_code/etcd_cn/%s.etcd", cfg.ec.Name)))
	}
	return err
}

// OK
func (cfg *config) configFromCmdLine() error {
	// 用户指定的记录器尚未设置,在标志解析过程中使用此记录器
	lg, err := zap.NewProduction()
	if err != nil {
		return err
	}
	err = flags.SetFlagsFromEnv(lg, "ETCD", cfg.cf.flagSet) // 解析给定flagset中的所有注册标志,如果它们还没有被设置,则尝试从环境变量中设置其值.
	if err != nil {
		return err
	}

	if rafthttp.ConnReadTimeout < rafthttp.DefaultConnReadTimeout {
		rafthttp.ConnReadTimeout = rafthttp.DefaultConnReadTimeout
		lg.Info(fmt.Sprintf("raft-read-timeout : %v", rafthttp.DefaultConnReadTimeout))
	}
	if rafthttp.ConnWriteTimeout < rafthttp.DefaultConnWriteTimeout {
		rafthttp.ConnWriteTimeout = rafthttp.DefaultConnWriteTimeout
		lg.Info(fmt.Sprintf("raft-write-timeout increased to minimum value: %v", rafthttp.DefaultConnWriteTimeout))
	}
	// 集群节点之间通信监听的URL;如果指定的IP是0.0.0.0,那么etcd 会监昕所有网卡的指定端口
	cfg.ec.LPUrls = flags.UniqueURLsFromFlag(cfg.cf.flagSet, "listen-peer-urls")
	cfg.ec.APUrls = flags.UniqueURLsFromFlag(cfg.cf.flagSet, "initial-advertise-peer-urls")
	cfg.ec.LCUrls = flags.UniqueURLsFromFlag(cfg.cf.flagSet, "listen-client-urls")
	cfg.ec.ACUrls = flags.UniqueURLsFromFlag(cfg.cf.flagSet, "advertise-client-urls")
	cfg.ec.ListenMetricsUrls = flags.UniqueURLsFromFlag(cfg.cf.flagSet, "listen-metrics-urls")

	cfg.ec.CORS = flags.UniqueURLsMapFromFlag(cfg.cf.flagSet, "cors")
	cfg.ec.HostWhitelist = flags.UniqueStringsMapFromFlag(cfg.cf.flagSet, "host-whitelist")

	cfg.ec.CipherSuites = flags.StringsFromFlag(cfg.cf.flagSet, "cipher-suites")

	cfg.ec.LogOutputs = flags.UniqueStringsFromFlag(cfg.cf.flagSet, "log-outputs")

	cfg.ec.ClusterState = cfg.cf.clusterState.String()
	cfg.cp.Fallback = cfg.cf.fallback.String() // proxy
	cfg.cp.Proxy = cfg.cf.proxy.String()       // off

	// 如果设置了lcurls,则禁用默认的 advertise-client-urls
	fmt.Println(`flags.IsSet(cfg.cf.flagSet, "listen-client-urls")`, flags.IsSet(cfg.cf.flagSet, "listen-client-urls"))
	fmt.Println(`flags.IsSet(cfg.cf.flagSet, "advertise-client-urls")`, flags.IsSet(cfg.cf.flagSet, "advertise-client-urls"))
	missingAC := flags.IsSet(cfg.cf.flagSet, "listen-client-urls") && !flags.IsSet(cfg.cf.flagSet, "advertise-client-urls")
	// todo 没看懂
	if !cfg.mayBeProxy() && missingAC {
		cfg.ec.ACUrls = nil
	}

	// 如果设置了discovery则禁用默认初始集群
	if (cfg.ec.Durl != "" || cfg.ec.DNSCluster != "" || cfg.ec.DNSClusterServiceName != "") && !flags.IsSet(cfg.cf.flagSet, "initial-cluster") {
		cfg.ec.InitialCluster = ""
	}

	return cfg.validate() // √
}

// OK
func (cfg *config) configFromFile(path string) error {
	eCfg, err := embed.ConfigFromFile(path)
	if err != nil {
		return err
	}
	cfg.ec = *eCfg

	// 加载额外的配置信息
	b, rerr := ioutil.ReadFile(path)
	if rerr != nil {
		return rerr
	}
	if yerr := yaml.Unmarshal(b, &cfg.cp); yerr != nil {
		return yerr
	}

	if cfg.cp.FallbackJSON != "" {
		if err := cfg.cf.fallback.Set(cfg.cp.FallbackJSON); err != nil {
			log.Fatalf("设置时出现意外错误 discovery-fallback flag: %v", err)
		}
		cfg.cp.Fallback = cfg.cf.fallback.String()
	}

	if cfg.cp.ProxyJSON != "" {
		if err := cfg.cf.proxy.Set(cfg.cp.ProxyJSON); err != nil {
			log.Fatalf("设置时出现意外错误 proxyFlag: %v", err)
		}
		cfg.cp.Proxy = cfg.cf.proxy.String()
	}
	return nil
}

func (cfg *config) mayBeProxy() bool {
	mayFallbackToProxy := cfg.ec.Durl != "" && cfg.cp.Fallback == fallbackFlagProxy
	return cfg.cp.Proxy != proxyFlagOff || mayFallbackToProxy
}

func (cfg *config) validate() error {
	err := cfg.ec.Validate()
	// TODO(yichengq): 通过 discovery service case加入,请检查这一点.
	if err == embed.ErrUnsetAdvertiseClientURLsFlag && cfg.mayBeProxy() {
		return nil
	}
	return err
}

// 是否开启代理模式
func (cfg config) isProxy() bool               { return cfg.cf.proxy.String() != proxyFlagOff }
func (cfg config) isReadonlyProxy() bool       { return cfg.cf.proxy.String() == proxyFlagReadonly }
func (cfg config) shouldFallbackToProxy() bool { return cfg.cf.fallback.String() == fallbackFlagProxy }
