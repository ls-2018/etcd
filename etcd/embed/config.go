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
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/logutil"
	"github.com/ls-2018/etcd_cn/client_sdk/pkg/srv"
	"github.com/ls-2018/etcd_cn/client_sdk/pkg/tlsutil"
	"github.com/ls-2018/etcd_cn/client_sdk/pkg/transport"
	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/etcd/config"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v3compactor"
	"github.com/ls-2018/etcd_cn/pkg/flags"
	"github.com/ls-2018/etcd_cn/pkg/netutil"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"sigs.k8s.io/yaml"
)

const (
	// 设置new为初始静态或DNS引导期间出现的所有成员.如果将此选项设置为existing.则etcd将尝试加入现有群集.
	ClusterStateFlagNew      = "new"
	ClusterStateFlagExisting = "existing"

	DefaultName                  = "default"
	DefaultMaxSnapshots          = 5
	DefaultMaxWALs               = 5
	DefaultMaxTxnOps             = uint(128)
	DefaultWarningApplyDuration  = 100 * time.Millisecond
	DefaultMaxRequestBytes       = 1.5 * 1024 * 1024
	DefaultGRPCKeepAliveMinTime  = 5 * time.Second
	DefaultGRPCKeepAliveInterval = 2 * time.Hour
	DefaultGRPCKeepAliveTimeout  = 20 * time.Second
	DefaultDowngradeCheckTime    = 5 * time.Second

	DefaultListenPeerURLs   = "http://localhost:2380"
	DefaultListenClientURLs = "http://localhost:2379"

	DefaultLogOutput = "default"
	JournalLogOutput = "systemd/journal"
	StdErrLogOutput  = "stderr"
	StdOutLogOutput  = "stdout"

	// DefaultLogRotationConfig 是用于日志轮换的默认配置. 默认情况下,日志轮换是禁用的.
	// MaxSize    = 100 // MB
	// MaxAge     = 0 // days (no limit)
	// MaxBackups = 0 // no limit
	// LocalTime  = false // use computers local time, UTC by default
	// Compress   = false // compress the rotated log in gzip format
	DefaultLogRotationConfig = `{"maxsize": 100, "maxage": 0, "maxbackups": 0, "localtime": false, "compress": false}`

	// ExperimentalDistributedTracingAddress is the default collector address.
	ExperimentalDistributedTracingAddress = "localhost:4317"
	// ExperimentalDistributedTracingServiceName is the default etcd service name.
	ExperimentalDistributedTracingServiceName = "etcd"

	// DefaultStrictReconfigCheck 拒绝可能导致仲裁丢失的重新配置请求
	DefaultStrictReconfigCheck = true

	// maxElectionMs specifies the maximum value of election timeout.
	// More details are listed in ../Documentation/tuning.md#time-parameters.
	maxElectionMs = 50000
	// backend freelist map type
	freelistArrayType = "array"
)

var (
	ErrConflictBootstrapFlags = fmt.Errorf("multiple discovery or bootstrap flags are set. " +
		"Choose one of \"initial-cluster\", \"discovery\" or \"discovery-srv\"")
	ErrUnsetAdvertiseClientURLsFlag = fmt.Errorf("--advertise-client-urls is required when --listen-client-urls is set explicitly")
	ErrLogRotationInvalidLogOutput  = fmt.Errorf("--log-outputs requires a single file path when --log-rotate-config-json is defined")

	DefaultInitialAdvertisePeerURLs = "http://localhost:2380"
	DefaultAdvertiseClientURLs      = "http://localhost:2379"

	// netutil.GetDefaultHost()
	defaultHostname   string
	defaultHostStatus error

	// indirection for testing
	getCluster = srv.GetCluster
)

var (
	// CompactorModePeriodic
	// 周期性压缩 eg. 1h
	CompactorModePeriodic = v3compactor.ModePeriodic

	// CompactorModeRevision  "AutoCompactionRetention" is "1000",
	// 当前版本为6000时,它将日志压缩到5000版本.
	// 如果有足够多的日志,这将每5分钟运行一次.
	CompactorModeRevision = v3compactor.ModeRevision
)

func init() {
	defaultHostname, defaultHostStatus = netutil.GetDefaultHost()
	fmt.Println("defaultHostname", defaultHostname)
	fmt.Println("defaultHostStatus", defaultHostStatus)
	// defaultHostname 172.17.0.2
	// defaultHostStatus <nil>
}

// Config 保存配置etcd的参数etcd.
type Config struct {
	Name string `json:"name"`     // 节点的名字
	Dir  string `json:"data-dir"` // 数据目录
	// 独立设置wal目录.etcd会将WAL文件写入  --wal-dir而不是--data-dir. 独立的wal路径.有助于避免日志记录和其他IO操作之间的竞争.
	WalDir string `json:"wal-dir"` // 专用wal目录的路径.

	SnapshotCount uint64 `json:"snapshot-count"` // 触发一次磁盘快照的提交事务的次数

	// SnapshotCatchUpEntries 是在压缩raft存储条目后,慢的follower要追赶的条目数.我们预计follower与leader之间有毫秒级的延迟.
	// 最大吞吐量大约为10K.保持一个5K的条目就足够帮助follower赶上了.
	SnapshotCatchUpEntries uint64

	MaxSnapFiles uint `json:"max-snapshots"` // 最大快照数
	MaxWalFiles  uint `json:"max-wals"`      // 要保留的最大wal文件数(0表示不受限制). 5

	// TickMs是心脏跳动间隔的毫秒数.
	// TODO:将tickMs和心跳tick解耦(目前的心跳tick=1)
	// 使tick成为集群范围内的配置.
	TickMs     uint `json:"heartbeat-interval"` // 定时器触发间隔  100ms
	ElectionMs uint `json:"election-timeout"`   // 选举权检查周期   1s

	// InitialElectionTickAdvance is true, then local member fast-forwards
	// election ticks to speed up "initial" leader election trigger. This
	// benefits the case of larger election ticks. For instance, cross
	// datacenter deployment may require longer election timeout of 10-second.
	// If true, local node does not need wait up to 10-second. Instead,
	// forwards its election ticks to 8-second, and have only 2-second left
	// before leader election.
	//
	// Major assumptions are that:
	//  - cluster has no active leader thus advancing ticks enables faster
	//    leader election, or
	//  - cluster already has an established leader, and rejoining follower
	//    is likely to receive heartbeats from the leader after tick advance
	//    and before election timeout.
	//
	// However, when network from leader to rejoining follower is congested,
	// and the follower does not receive leader heartbeat within left election
	// ticks, disruptive election has to happen thus affecting cluster
	// availabilities.
	//
	// Disabling this would slow down initial bootstrap process for cross
	// datacenter deployments. Make your own tradeoffs by configuring
	// --initial-election-tick-advance at the cost of slow initial bootstrap.
	//
	// If single-node, it advances ticks regardless.
	//
	// See https://github.com/etcd-io/etcd/issues/9333 for more detail.
	// todo 是否在开机时快进初始选举点.以加快选举速度.
	InitialElectionTickAdvance bool `json:"initial-election-tick-advance"` // 是否提前初始化选举时钟启动,以便更快的选举

	BoltBackendBatchInterval time.Duration `json:"backend-batch-interval"`      // BackendBatchInterval是提交后端事务前的最长时间
	BoltBackendBatchLimit    int           `json:"backend-batch-limit"`         // BackendBatchLimit是提交后端事务前的最大操作数
	BackendFreelistType      string        `json:"backend-bbolt-freelist-type"` // BackendFreelistType指定boltdb后端使用的freelist的类型(array and map是支持的类型).
	QuotaBackendBytes        int64         `json:"quota-backend-bytes"`         // 当后端大小超过给定配额时(0默认为低空间配额).引发警报.
	MaxTxnOps                uint          `json:"max-txn-ops"`                 // 事务中允许的最大操作数.
	MaxRequestBytes          uint          `json:"max-request-bytes"`           // 服务器将接受的最大客户端请求大小(字节).

	LPUrls []url.URL // 和etcd  server 成员之间通信的地址.用于监听其他etcd member的url
	LCUrls []url.URL // 这个参数是etcd服务器自己监听时用的,也就是说,监听本机上的哪个网卡,哪个端口

	APUrls []url.URL // 就是客户端(etcd server 等)跟etcd服务进行交互时请求的url
	ACUrls []url.URL // 就是客户端(etcdctl/curl等)跟etcd服务进行交互时请求的url

	ClientTLSInfo transport.TLSInfo // 与 etcdctl 交互的客户端证书信息
	ClientAutoTLS bool

	PeerTLSInfo transport.TLSInfo
	PeerAutoTLS bool // 节点之间使用生成的证书通信;默认false
	// SelfSignedCertValidity 客户端证书和同级证书的有效期,单位为年 ;etcd自动生成的 如果指定了ClientAutoTLS and PeerAutoTLS,
	SelfSignedCertValidity uint `json:"self-signed-cert-validity"`

	// CipherSuites is a list of supported TLS cipher suites between
	// client/etcd and peers. If empty, Go auto-populates the list.
	// Note that cipher suites are prioritized in the given order.
	CipherSuites []string `json:"cipher-suites"`

	ClusterState          string `json:"initial-cluster-state"`
	DNSCluster            string `json:"discovery-srv"`         // DNS srv域用于引导群集.
	DNSClusterServiceName string `json:"discovery-srv-name"`    // 使用DNS引导时查询的DNS srv名称的后缀.
	Dproxy                string `json:"discovery-proxy"`       // 用于流量到发现服务的HTTP代理
	Durl                  string `json:"discovery"`             // 用于引导群集的发现URL.
	InitialCluster        string `json:"initial-cluster"`       // 集群中所有节点的信息.  default=http://localhost:2380
	InitialClusterToken   string `json:"initial-cluster-token"` // 此配置可使重新创建集群.即使配置和之前一样.也会再次生成新的集群和节点 uuid;否则会导致多个集群之间的冲突.造成未知的错误.
	StrictReconfigCheck   bool   `json:"strict-reconfig-check"` // 严格配置变更检查

	EnableV2 bool `json:"enable-v2"`
	// AutoCompactionMode 基于时间保留模式  时间、修订版本
	AutoCompactionMode string `json:"auto-compaction-mode"`

	//--auto-compaction-mode=revision --auto-compaction-retention=1000 每5分钟自动压缩"latest revision" - 1000;
	//--auto-compaction-mode=periodic --auto-compaction-retention=12h 每1小时自动压缩并保留12小时窗口.

	AutoCompactionRetention string `json:"auto-compaction-retention"`

	// GRPCKeepAliveMinTime  客户端在ping服务器之前应等待的最短持续时间间隔.
	GRPCKeepAliveMinTime time.Duration `json:"grpc-keepalive-min-time"`

	// GRPCKeepAliveInterval 服务器到客户端ping的频率持续时间.以检查连接是否处于活动状态(0表示禁用).
	GRPCKeepAliveInterval time.Duration `json:"grpc-keepalive-interval"`
	// GRPCKeepAliveTimeout 关闭非响应连接之前的额外持续等待时间(0表示禁用).20s
	GRPCKeepAliveTimeout time.Duration `json:"grpc-keepalive-timeout"`

	// SocketOpts are socket options passed to listener config.
	SocketOpts transport.SocketOpts

	// PreVote  为真.以启用Raft预投票.如果启用.Raft会运行一个额外的选举阶段.以检查它是否会获得足够的票数来赢得选举.从而最大限度地减少干扰.
	PreVote bool `json:"pre-vote"` // 默认false

	CORS map[string]struct{}

	// 列出可接受的来自HTTP客户端请求的主机名.客户端来源策略可以防止对不安全的etcd服务器的 "DNS重定向 "攻击.
	// 也就是说.任何网站可以简单地创建一个授权的DNS名称.并将DNS指向 "localhost"(或任何其他地址).
	// 然后.所有监听 "localhost "的etcd的HTTP端点都变得可以访问.从而容易受到DNS重定向攻击.
	HostWhitelist map[string]struct{}

	// UserHandlers 是用来注册用户处理程序的,只用于将etcd嵌入到其他应用程序中.
	// map key 是处理程序的路径,你必须确保它不能与etcd的路径冲突.
	UserHandlers map[string]http.Handler `json:"-"`
	// ServiceRegister is for registering users' gRPC services. A simple usage example:
	//	cfg := embed.NewConfig()
	//	cfg.ServerRegister = func(s *grpc.Server) {
	//		pb.RegisterFooServer(s, &fooServer{})
	//		pb.RegisterBarServer(s, &barServer{})
	//	}
	//	embed.StartEtcd(cfg)
	ServiceRegister func(*grpc.Server) `json:"-"`

	AuthToken  string `json:"auth-token"`  // 认证格式  simple、jwt
	BcryptCost uint   `json:"bcrypt-cost"` // 为散列身份验证密码指定bcrypt算法的成本/强度.有效值介于4和31之间.默认值:10

	AuthTokenTTL uint `json:"auth-token-ttl"` // token 有效期

	ExperimentalInitialCorruptCheck bool          `json:"experimental-initial-corrupt-check"` // 数据毁坏检测功能
	ExperimentalCorruptCheckTime    time.Duration `json:"experimental-corrupt-check-time"`    // 数据毁坏检测功能
	// ExperimentalEnableV2V3 configures URLs that expose deprecated V2 API working on V3 store.
	// Deprecated in v3.5.
	// TODO: Delete in v3.6 (https://github.com/etcd-io/etcd/issues/12913)
	ExperimentalEnableV2V3            string `json:"experimental-enable-v2v3"`
	ExperimentalEnableLeaseCheckpoint bool   `json:"experimental-enable-lease-checkpoint"` // 允许leader定期向其他成员发送检查点,以防止leader变化时剩余TTL重置.
	// ExperimentalEnableLeaseCheckpointPersist
	// 启用持续的剩余TTL,以防止长期租约的无限期自动续约.在v3.6中始终启用.应该用于确保从启用该功能的v3.5集群顺利升级.
	// 需要启用 experimental-enable-lease-checkpoint
	// Deprecated in v3.6.
	// TODO: Delete in v3.7
	ExperimentalEnableLeaseCheckpointPersist bool          `json:"experimental-enable-lease-checkpoint-persist"`
	ExperimentalCompactionBatchLimit         int           `json:"experimental-compaction-batch-limit"`
	ExperimentalWatchProgressNotifyInterval  time.Duration `json:"experimental-watch-progress-notify-interval"`
	// ExperimentalWarningApplyDuration 是时间长度.如果应用请求的时间超过这个值.就会产生一个警告.
	ExperimentalWarningApplyDuration time.Duration `json:"experimental-warning-apply-duration"`
	// ExperimentalBootstrapDefragThresholdMegabytes is the minimum number of megabytes needed to be freed for etcd etcd to
	// consider running defrag during bootstrap. Needs to be set to non-zero value to take effect.
	ExperimentalBootstrapDefragThresholdMegabytes uint `json:"experimental-bootstrap-defrag-threshold-megabytes"`

	// ForceNewCluster starts a new cluster even if previously started; unsafe.
	ForceNewCluster bool `json:"force-new-cluster"`

	EnablePprof           bool   `json:"enable-pprof"`
	Metrics               string `json:"metrics"` // basic  ;extensive
	ListenMetricsUrls     []url.URL
	ListenMetricsUrlsJSON string `json:"listen-metrics-urls"`

	// ExperimentalEnableDistributedTracing 表示是否启用了使用OpenTelemetry的实验性追踪.
	ExperimentalEnableDistributedTracing bool `json:"experimental-enable-distributed-tracing"`
	// ExperimentalDistributedTracingAddress is the address of the OpenTelemetry Collector.
	// Can only be set if ExperimentalEnableDistributedTracing is true.
	ExperimentalDistributedTracingAddress string `json:"experimental-distributed-tracing-address"`
	// ExperimentalDistributedTracingServiceName is the name of the service.
	// Can only be used if ExperimentalEnableDistributedTracing is true.
	ExperimentalDistributedTracingServiceName string `json:"experimental-distributed-tracing-service-name"`
	// ExperimentalDistributedTracingServiceInstanceID is the ID key of the service.
	// This ID必须是unique, as helps to distinguish instances of the same service
	// that exist at the same time.
	// Can only be used if ExperimentalEnableDistributedTracing is true.
	ExperimentalDistributedTracingServiceInstanceID string `json:"experimental-distributed-tracing-instance-id"`

	// Logger 使用哪种logger
	Logger string `json:"logger"`
	// LogLevel 日志等级 debug, info, warn, error, panic, or fatal. Default 'info'.
	LogLevel string `json:"log-level"`
	// LogOutputs is either:
	//  - "default" as os.Stderr,
	//  - "stderr" as os.Stderr,
	//  - "stdout" as os.Stdout,
	//  - file path to append etcd logs to.
	// 当 logger是zap时,它可以是多个.
	LogOutputs []string `json:"log-outputs"`
	// EnableLogRotation 启用单个日志输出文件目标的日志旋转.
	EnableLogRotation bool `json:"enable-log-rotation"`
	// LogRotationConfigJSON is a passthrough allowing a log rotation JSON config to be passed directly.
	LogRotationConfigJSON string `json:"log-rotation-config-json"`
	// ZapLoggerBuilder 用于给自己构造一个zap logger
	ZapLoggerBuilder func(*Config) error

	// logger logs etcd-side operations. The default is nil,
	// and "setupLogging"必须是called before starting etcd.
	// Do not set logger directly.
	loggerMu *sync.RWMutex
	logger   *zap.Logger
	// EnableGRPCGateway 启用grpc网关,将 http 转换成 grpc / true
	EnableGRPCGateway bool `json:"enable-grpc-gateway"`

	// UnsafeNoFsync 禁用所有fsync的使用.设置这个是不安全的,会导致数据丢失.
	UnsafeNoFsync bool `json:"unsafe-no-fsync"` // 默认false
	// 两次降级状态检查之间的时间间隔.
	ExperimentalDowngradeCheckTime time.Duration `json:"experimental-downgrade-check-time"`

	// ExperimentalMemoryMlock 启用对etcd拥有的内存页的锁定. 该设置改善了以下环境中的etcd尾部延迟.
	//   - 内存压力可能会导致将页面交换到磁盘上
	//   - 磁盘延迟可能是不稳定的
	// 目前,所有的etcd内存都被锁住了,但在将来,这个标志可以改进为只锁住bbolt的使用区域.
	ExperimentalMemoryMlock bool `json:"experimental-memory-mlock"`

	// ExperimentalTxnModeWriteWithSharedBuffer 使得写事务在其只读检查操作中使用一个共享缓冲区.
	ExperimentalTxnModeWriteWithSharedBuffer bool `json:"experimental-txn-mode-write-with-shared-buffer"`

	// V2Deprecation describes phase of API & Storage V2 support
	V2Deprecation config.V2DeprecationEnum `json:"v2-deprecation"`
}

// configYAML holds the config suitable for yaml parsing
type configYAML struct {
	Config
	configJSON
}

// configJSON 有文件选项,被翻译成配置选项
type configJSON struct {
	LPUrlsJSON string `json:"listen-peer-urls"` // 集群节点之间通信监听的URL;如果指定的IP是0.0.0.0,那么etcd 会监昕所有网卡的指定端口
	LCUrlsJSON string `json:"listen-client-urls"`
	APUrlsJSON string `json:"initial-advertise-peer-urls"`
	ACUrlsJSON string `json:"advertise-client-urls"`

	CORSJSON          string `json:"cors"`
	HostWhitelistJSON string `json:"host-whitelist"`

	ClientSecurityJSON securityConfig `json:"client-transport-security"`
	PeerSecurityJSON   securityConfig `json:"peer-transport-security"`
}

type securityConfig struct {
	CertFile       string `json:"cert-file"`
	KeyFile        string `json:"key-file"`
	ClientCertFile string `json:"client-cert-file"`
	ClientKeyFile  string `json:"client-key-file"`
	CertAuth       bool   `json:"client-cert-auth"`
	TrustedCAFile  string `json:"trusted-ca-file"`
	AutoTLS        bool   `json:"auto-tls"`
}

// NewConfig 创建一个用默认值填充的新配置.
func NewConfig() *Config {
	lpurl, _ := url.Parse(DefaultListenPeerURLs)           // "http://localhost:2380"
	apurl, _ := url.Parse(DefaultInitialAdvertisePeerURLs) // "http://localhost:2380"
	lcurl, _ := url.Parse(DefaultListenClientURLs)         // "http://localhost:2379"
	acurl, _ := url.Parse(DefaultAdvertiseClientURLs)      // "http://localhost:2379"
	cfg := &Config{
		MaxSnapFiles: DefaultMaxSnapshots, // 最大快照数
		MaxWalFiles:  DefaultMaxWALs,      // wal文件的最大保留数量(0不受限制).

		Name: DefaultName, // 节点的名字

		SnapshotCount:          etcdserver.DefaultSnapshotCount,          // 快照数量
		SnapshotCatchUpEntries: etcdserver.DefaultSnapshotCatchUpEntries, // 触发快照到磁盘的已提交事务数.

		MaxTxnOps:                        DefaultMaxTxnOps,            // 事务中允许的最大操作数. 128
		MaxRequestBytes:                  DefaultMaxRequestBytes,      // 最大请求体, 1.5M
		ExperimentalWarningApplyDuration: DefaultWarningApplyDuration, // 是时间长度.如果应用请求的时间超过这个值.就会产生一个警告. 100ms

		GRPCKeepAliveMinTime:  DefaultGRPCKeepAliveMinTime,  // 客户端在ping服务器之前应等待的最短持续时间间隔. 5s
		GRPCKeepAliveInterval: DefaultGRPCKeepAliveInterval, // 服务器到客户端ping的探活周期.以检查连接是否处于活动状态(0表示禁用).2h
		GRPCKeepAliveTimeout:  DefaultGRPCKeepAliveTimeout,  // 关闭非响应连接之前的额外持续等待时间(0表示禁用).20s

		SocketOpts: transport.SocketOpts{}, // 套接字配置

		TickMs:                     100,  // 心跳间隔100ms
		ElectionMs:                 1000, // 选举超时 1s
		InitialElectionTickAdvance: true,

		LPUrls: []url.URL{*lpurl}, // "http://localhost:2380"
		LCUrls: []url.URL{*lcurl}, // "http://localhost:2380"
		APUrls: []url.URL{*apurl}, // "http://localhost:2379"
		ACUrls: []url.URL{*acurl}, // "http://localhost:2379"

		// 设置new为初始静态或DNS引导期间出现的所有成员.如果将此选项设置为existing.则etcd将尝试加入现有群集.
		ClusterState:        ClusterStateFlagNew, // 状态标志、默认new
		InitialClusterToken: "etcd-cluster",
		StrictReconfigCheck: DefaultStrictReconfigCheck, // 拒绝可能导致仲裁丢失的重新配置请求
		Metrics:             "basic",                    // 基本的

		CORS:          map[string]struct{}{"*": {}}, // 跨域请求
		HostWhitelist: map[string]struct{}{"*": {}}, // 主机白名单

		AuthToken:    "simple",                 // 认证格式  simple、jwt
		BcryptCost:   uint(bcrypt.DefaultCost), // 为散列身份验证密码指定bcrypt算法的成本/强度
		AuthTokenTTL: 300,                      // token 有效期

		PreVote: true, // Raft会运行一个额外的选举阶段.以检查它是否会获得足够的票数来赢得选举.从而最大限度地减少干扰.

		loggerMu:              new(sync.RWMutex),
		logger:                nil,
		Logger:                "zap",
		LogOutputs:            []string{DefaultLogOutput}, // os.Stderr
		LogLevel:              logutil.DefaultLogLevel,    // info
		EnableLogRotation:     false,                      // 默认不允许日志旋转
		LogRotationConfigJSON: DefaultLogRotationConfig,   // 是用于日志轮换的默认配置. 默认情况下,日志轮换是禁用的.
		EnableGRPCGateway:     true,                       // 将http->grpc
		// 实验性
		ExperimentalDowngradeCheckTime:           DefaultDowngradeCheckTime, // 两次降级状态检查之间的时间间隔.
		ExperimentalMemoryMlock:                  false,                     // 内存页锁定
		ExperimentalTxnModeWriteWithSharedBuffer: true,                      // 启用写事务在其只读检查操作中使用共享缓冲区.

		V2Deprecation: config.V2_DEPR_DEFAULT, // not-yet
	}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	return cfg
}

// ConfigFromFile OK
func ConfigFromFile(path string) (*Config, error) {
	cfg := &configYAML{Config: *NewConfig()}
	if err := cfg.configFromFile(path); err != nil { // ✅
		return nil, err
	}
	return &cfg.Config, nil
}

// OK
func (cfg *configYAML) configFromFile(path string) error {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	defaultInitialCluster := cfg.InitialCluster

	err = yaml.Unmarshal(b, cfg)
	if err != nil {
		return err
	}

	if cfg.LPUrlsJSON != "" {
		u, err := types.NewURLs(strings.Split(cfg.LPUrlsJSON, ","))
		if err != nil {
			fmt.Fprintf(os.Stderr, "设置时出现意外错误 listen-peer-urls: %v\n", err)
			os.Exit(1)
		}
		cfg.LPUrls = []url.URL(u)
	}

	if cfg.LCUrlsJSON != "" {
		u, err := types.NewURLs(strings.Split(cfg.LCUrlsJSON, ","))
		if err != nil {
			fmt.Fprintf(os.Stderr, "设置时出现意外错误 listen-client-urls: %v\n", err)
			os.Exit(1)
		}
		cfg.LCUrls = []url.URL(u)
	}

	if cfg.APUrlsJSON != "" {
		u, err := types.NewURLs(strings.Split(cfg.APUrlsJSON, ","))
		if err != nil {
			fmt.Fprintf(os.Stderr, "设置时出现意外错误 initial-advertise-peer-urls: %v\n", err)
			os.Exit(1)
		}
		cfg.APUrls = []url.URL(u)
	}

	if cfg.ACUrlsJSON != "" {
		u, err := types.NewURLs(strings.Split(cfg.ACUrlsJSON, ","))
		if err != nil {
			fmt.Fprintf(os.Stderr, "设置时出现意外错误 advertise-peer-urls: %v\n", err)
			os.Exit(1)
		}
		cfg.ACUrls = []url.URL(u)
	}

	if cfg.ListenMetricsUrlsJSON != "" {
		u, err := types.NewURLs(strings.Split(cfg.ListenMetricsUrlsJSON, ","))
		if err != nil {
			fmt.Fprintf(os.Stderr, "设置时出现意外错误 listen-metrics-urls: %v\n", err)
			os.Exit(1)
		}
		cfg.ListenMetricsUrls = []url.URL(u)
	}

	if cfg.CORSJSON != "" {
		uv := flags.NewUniqueURLsWithExceptions(cfg.CORSJSON, "*")
		cfg.CORS = uv.Values
	}

	if cfg.HostWhitelistJSON != "" {
		uv := flags.NewUniqueStringsValue(cfg.HostWhitelistJSON)
		cfg.HostWhitelist = uv.Values
	}

	// 如果设置了discovery flag则清除由InitialClusterFromName设置的默认初始集群
	if (cfg.Durl != "" || cfg.DNSCluster != "") && cfg.InitialCluster == defaultInitialCluster {
		cfg.InitialCluster = ""
	}
	if cfg.ClusterState == "" {
		cfg.ClusterState = ClusterStateFlagNew
	}

	copySecurityDetails := func(tls *transport.TLSInfo, ysc *securityConfig) {
		tls.CertFile = ysc.CertFile
		tls.KeyFile = ysc.KeyFile
		tls.ClientCertFile = ysc.ClientCertFile
		tls.ClientKeyFile = ysc.ClientKeyFile
		tls.ClientCertAuth = ysc.CertAuth
		tls.TrustedCAFile = ysc.TrustedCAFile
	}
	copySecurityDetails(&cfg.ClientTLSInfo, &cfg.ClientSecurityJSON)
	copySecurityDetails(&cfg.PeerTLSInfo, &cfg.PeerSecurityJSON)
	cfg.ClientAutoTLS = cfg.ClientSecurityJSON.AutoTLS
	cfg.PeerAutoTLS = cfg.PeerSecurityJSON.AutoTLS
	if cfg.SelfSignedCertValidity == 0 {
		cfg.SelfSignedCertValidity = 1
	}
	return cfg.Validate() // ✅
}

// 更新密码套件
func updateCipherSuites(tls *transport.TLSInfo, ss []string) error {
	if len(tls.CipherSuites) > 0 && len(ss) > 0 {
		return fmt.Errorf("TLSInfo.CipherSuites已经指定(given %v)", ss)
	}
	if len(ss) > 0 {
		cs := make([]uint16, len(ss))
		for i, s := range ss {
			var ok bool
			cs[i], ok = tlsutil.GetCipherSuite(s)
			if !ok {
				return fmt.Errorf("unexpected TLS cipher suite %q", s)
			}
		}
		tls.CipherSuites = cs
	}
	return nil
}

// Validate 确保 '*embed.Config' 字段是正确配置的.
func (cfg *Config) Validate() error {
	if err := cfg.setupLogging(); err != nil { // ✅
		return err
	}
	if err := checkBindURLs(cfg.LPUrls); err != nil {
		return err
	}
	if err := checkBindURLs(cfg.LCUrls); err != nil {
		return err
	}
	if err := checkBindURLs(cfg.ListenMetricsUrls); err != nil {
		return err
	}
	if err := checkHostURLs(cfg.APUrls); err != nil {
		addrs := cfg.getAPURLs()
		return fmt.Errorf(`--initial-advertise-peer-urls %q 必须是 "host:port" (%v)`, strings.Join(addrs, ","), err)
	}
	if err := checkHostURLs(cfg.ACUrls); err != nil {
		addrs := cfg.getACURLs()
		return fmt.Errorf(`--advertise-client-urls %q 必须是 "host:port" (%v)`, strings.Join(addrs, ","), err)
	}
	// 检查是否有冲突的标志通过.
	nSet := 0
	for _, v := range []bool{cfg.Durl != "", cfg.InitialCluster != "", cfg.DNSCluster != ""} {
		if v {
			nSet++
		}
	}

	if cfg.ClusterState != ClusterStateFlagNew && cfg.ClusterState != ClusterStateFlagExisting {
		return fmt.Errorf("意料之外的集群状态 %q", cfg.ClusterState)
	}

	if nSet > 1 {
		return ErrConflictBootstrapFlags
	}

	if cfg.TickMs == 0 {
		return fmt.Errorf("--heartbeat-interval必须是>0 (set to %dms)", cfg.TickMs)
	}
	if cfg.ElectionMs == 0 {
		return fmt.Errorf("--election-timeout必须是>0 (set to %dms)", cfg.ElectionMs)
	}
	if 5*cfg.TickMs > cfg.ElectionMs {
		return fmt.Errorf("--election-timeout[%vms] 必须是5倍 --heartbeat-interval[%vms]", cfg.ElectionMs, cfg.TickMs)
	}
	if cfg.ElectionMs > maxElectionMs {
		return fmt.Errorf("--election-timeout[%vms] 时间太长,应该小于 %vms", cfg.ElectionMs, maxElectionMs)
	}

	// 最后检查一下,因为在etcdmain中代理可能会使这个问题得到解决.
	if cfg.LCUrls != nil && cfg.ACUrls == nil {
		return ErrUnsetAdvertiseClientURLsFlag
	}

	switch cfg.AutoCompactionMode {
	case "":
	case CompactorModeRevision, CompactorModePeriodic:
	default:
		return fmt.Errorf("未知的 auto-compaction-mode %q", cfg.AutoCompactionMode)
	}
	// false,false 不会走
	if !cfg.ExperimentalEnableLeaseCheckpointPersist && cfg.ExperimentalEnableLeaseCheckpoint {
		cfg.logger.Warn("检测到启用了Checkpoint而没有持久性.考虑启用experimental-enable-le-checkpoint-persist")
	}
	if !cfg.ExperimentalEnableLeaseCheckpoint && !cfg.ExperimentalEnableLeaseCheckpointPersist {
		// falsefalse  默认走这里
		return nil
	} else if cfg.ExperimentalEnableLeaseCheckpoint && cfg.ExperimentalEnableLeaseCheckpointPersist {
		return nil
	}
	return fmt.Errorf("  experimental-enable-lease-checkpoint-persist   experimental-enable-lease-checkpoint 需要同时开启")
}

// PeerURLsMapAndToken 设置一个初始的peer URLsMap 和token,用于启动或发现.
func (cfg *Config) PeerURLsMapAndToken(which string) (urlsmap types.URLsMap, token string, err error) {
	token = cfg.InitialClusterToken
	switch {
	// todo 以下手动注释掉,一般不会使用以下的
	//case cfg.Durl != "": // 用于引导群集的发现URL
	//	urlsmap = types.URLsMap{}
	//	// 如果使用discovery,根据advertised peer URLs 生成一个临时的集群
	//	urlsmap[cfg.Name] = cfg.APUrls
	//	token = cfg.Durl
	//
	//case cfg.DNSCluster != "": // DNS srv域用于引导群集.
	//	clusterStrs, cerr := cfg.GetDNSClusterNames()
	//	lg := cfg.logger
	//	if cerr != nil {
	//		lg.Warn("如法解析 SRV discovery", zap.Error(cerr))
	//	}
	//	if len(clusterStrs) == 0 {
	//		return nil, "", cerr
	//	}
	//	for _, s := range clusterStrs {
	//		lg.Info("got bootstrap from DNS for etcd-etcd", zap.String("node", s))
	//	}
	//	clusterStr := strings.Join(clusterStrs, ",")
	//	if strings.Contains(clusterStr, "https://") && cfg.PeerTLSInfo.TrustedCAFile == "" {
	//		cfg.PeerTLSInfo.ServerName = cfg.DNSCluster
	//	}
	//	urlsmap, err = types.NewURLsMap(clusterStr)
	//	// only etcd member must belong to the discovered cluster.
	//	// proxy does not need to belong to the discovered cluster.
	//	if which == "etcd" {
	//		if _, ok := urlsmap[cfg.Name]; !ok {
	//			return nil, "", fmt.Errorf("cannot find local etcd member %q in SRV records", cfg.Name)
	//		}
	//	}

	default:
		// 我们是静态配置的,
		// infra1=http://127.0.0.1:12380,infra2=http://127.0.0.1:22380,infra3=http://127.0.0.1:32380
		urlsmap, err = types.NewURLsMap(cfg.InitialCluster) // 仅仅是类型转换
	}
	return urlsmap, token, err
}

// GetDNSClusterNames 使用DNS SRV记录来获取集群启动的初始节点列表.这个函数将返回一个或多个节点的列表,以及在执行服务发现时遇到的任何错误.
// Note: Because this checks multiple sets of SRV records, discovery should only be considered to have
// failed if the returned node list is empty.
func (cfg *Config) GetDNSClusterNames() ([]string, error) {
	var (
		clusterStrs       []string
		cerr              error
		serviceNameSuffix string
	)
	if cfg.DNSClusterServiceName != "" {
		serviceNameSuffix = "-" + cfg.DNSClusterServiceName
	}

	lg := cfg.GetLogger()

	// Use both etcd-etcd-ssl and etcd-etcd for discovery.
	// Combine the results if both are available.
	clusterStrs, cerr = getCluster("https", "etcd-etcd-ssl"+serviceNameSuffix, cfg.Name, cfg.DNSCluster, cfg.APUrls)
	if cerr != nil {
		clusterStrs = make([]string, 0)
	}
	lg.Info(
		"get cluster for etcd-etcd-ssl SRV",
		zap.String("service-scheme", "https"),
		zap.String("service-name", "etcd-etcd-ssl"+serviceNameSuffix),
		zap.String("etcd-name", cfg.Name),
		zap.String("discovery-srv", cfg.DNSCluster),
		zap.Strings("advertise-peer-urls", cfg.getAPURLs()),
		zap.Strings("found-cluster", clusterStrs),
		zap.Error(cerr),
	)

	defaultHTTPClusterStrs, httpCerr := getCluster("http", "etcd-etcd"+serviceNameSuffix, cfg.Name, cfg.DNSCluster, cfg.APUrls)
	if httpCerr == nil {
		clusterStrs = append(clusterStrs, defaultHTTPClusterStrs...)
	}
	lg.Info(
		"get cluster for etcd-etcd SRV",
		zap.String("service-scheme", "http"),
		zap.String("service-name", "etcd-etcd"+serviceNameSuffix),
		zap.String("etcd-name", cfg.Name),
		zap.String("discovery-srv", cfg.DNSCluster),
		zap.Strings("advertise-peer-urls", cfg.getAPURLs()),
		zap.Strings("found-cluster", clusterStrs),
		zap.Error(httpCerr),
	)

	return clusterStrs, multierr.Combine(cerr, httpCerr)
}

// 初始化集群节点列表  default=http://localhost:2380
func (cfg Config) InitialClusterFromName(name string) (ret string) {
	if len(cfg.APUrls) == 0 {
		return ""
	}
	n := name
	if name == "" {
		n = DefaultName
	}
	for i := range cfg.APUrls {
		ret = ret + "," + n + "=" + cfg.APUrls[i].String()
	}
	return ret[1:]
}

func (cfg Config) IsNewCluster() bool { return cfg.ClusterState == ClusterStateFlagNew }

// ElectionTicks 返回选举权检查对应多少次tick触发次数
func (cfg Config) ElectionTicks() int {
	return int(cfg.ElectionMs / cfg.TickMs)
}

func (cfg Config) V2DeprecationEffective() config.V2DeprecationEnum {
	if cfg.V2Deprecation == "" {
		return config.V2_DEPR_DEFAULT
	}
	return cfg.V2Deprecation
}

func (cfg Config) defaultPeerHost() bool {
	return len(cfg.APUrls) == 1 && cfg.APUrls[0].String() == DefaultInitialAdvertisePeerURLs
}

func (cfg Config) defaultClientHost() bool {
	return len(cfg.ACUrls) == 1 && cfg.ACUrls[0].String() == DefaultAdvertiseClientURLs
}

// ClientSelfCert etcd LCUrls客户端自签
func (cfg *Config) ClientSelfCert() (err error) {
	if !cfg.ClientAutoTLS {
		return nil
	}
	if !cfg.ClientTLSInfo.Empty() {
		cfg.logger.Warn("忽略客户端自动TLS,因为已经给出了证书")
		return nil
	}
	chosts := make([]string, len(cfg.LCUrls))
	for i, u := range cfg.LCUrls {
		chosts[i] = u.Host
	}
	cfg.ClientTLSInfo, err = transport.SelfCert(cfg.logger, filepath.Join(cfg.Dir, "fixtures", "client"), chosts, cfg.SelfSignedCertValidity)
	if err != nil {
		return err
	}
	return updateCipherSuites(&cfg.ClientTLSInfo, cfg.CipherSuites)
}

// PeerSelfCert etcd LPUrls客户端自签
func (cfg *Config) PeerSelfCert() (err error) {
	if !cfg.PeerAutoTLS {
		return nil
	}
	if !cfg.PeerTLSInfo.Empty() {
		cfg.logger.Warn("如果证书给出  则忽略peer自动TLS")
		return nil
	}
	phosts := make([]string, len(cfg.LPUrls))
	for i, u := range cfg.LPUrls {
		phosts[i] = u.Host
	}
	cfg.PeerTLSInfo, err = transport.SelfCert(cfg.logger, filepath.Join(cfg.Dir, "fixtures", "peer"), phosts, cfg.SelfSignedCertValidity) // ?年
	if err != nil {
		return err
	}
	return updateCipherSuites(&cfg.PeerTLSInfo, cfg.CipherSuites)
}

// UpdateDefaultClusterFromName 更新集群通信地址
func (cfg *Config) UpdateDefaultClusterFromName(defaultInitialCluster string) (string, error) {
	// default=http://localhost:2380
	if defaultHostname == "" || defaultHostStatus != nil {
		// 当 指定名称时,更新'initial-cluster'(例如,'etcd --name=abc').
		if cfg.Name != DefaultName && cfg.InitialCluster == defaultInitialCluster {
			cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
		}
		return "", defaultHostStatus
	}

	used := false
	pip, pport := cfg.LPUrls[0].Hostname(), cfg.LPUrls[0].Port()
	if cfg.defaultPeerHost() && pip == "0.0.0.0" {
		cfg.APUrls[0] = url.URL{Scheme: cfg.APUrls[0].Scheme, Host: fmt.Sprintf("%s:%s", defaultHostname, pport)}
		used = true
	}
	// update 'initial-cluster' when only the name is specified (e.g. 'etcd --name=abc')
	if cfg.Name != DefaultName && cfg.InitialCluster == defaultInitialCluster {
		cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	}

	cip, cport := cfg.LCUrls[0].Hostname(), cfg.LCUrls[0].Port()
	if cfg.defaultClientHost() && cip == "0.0.0.0" {
		cfg.ACUrls[0] = url.URL{Scheme: cfg.ACUrls[0].Scheme, Host: fmt.Sprintf("%s:%s", defaultHostname, cport)}
		used = true
	}
	dhost := defaultHostname
	if !used {
		dhost = ""
	}
	return dhost, defaultHostStatus
}

// checkBindURLs 如果任何URL使用域名,则返回错误.
func checkBindURLs(urls []url.URL) error {
	for _, url := range urls {
		if url.Scheme == "unix" || url.Scheme == "unixs" {
			continue
		}
		host, _, err := net.SplitHostPort(url.Host)
		if err != nil {
			return err
		}
		if host == "localhost" {
			// special case for local address
			// TODO: support /etc/hosts ?
			continue
		}
		if net.ParseIP(host) == nil {
			return fmt.Errorf("expected IP in URL for binding (%s)", url.String())
		}
	}
	return nil
}

func checkHostURLs(urls []url.URL) error {
	for _, url := range urls {
		host, _, err := net.SplitHostPort(url.Host)
		if err != nil {
			return err
		}
		if host == "" {
			return fmt.Errorf("unexpected empty host (%s)", url.String())
		}
	}
	return nil
}

func (cfg *Config) getAPURLs() (ss []string) {
	ss = make([]string, len(cfg.APUrls))
	for i := range cfg.APUrls {
		ss[i] = cfg.APUrls[i].String()
	}
	return ss
}

func (cfg *Config) getLPURLs() (ss []string) {
	ss = make([]string, len(cfg.LPUrls))
	for i := range cfg.LPUrls {
		ss[i] = cfg.LPUrls[i].String()
	}
	return ss
}

func (cfg *Config) getACURLs() (ss []string) {
	ss = make([]string, len(cfg.ACUrls))
	for i := range cfg.ACUrls {
		ss[i] = cfg.ACUrls[i].String()
	}
	return ss
}

func (cfg *Config) getLCURLs() (ss []string) {
	ss = make([]string, len(cfg.LCUrls))
	for i := range cfg.LCUrls {
		ss[i] = cfg.LCUrls[i].String()
	}
	return ss
}

func (cfg *Config) getMetricsURLs() (ss []string) {
	ss = make([]string, len(cfg.ListenMetricsUrls))
	for i := range cfg.ListenMetricsUrls {
		ss[i] = cfg.ListenMetricsUrls[i].String()
	}
	return ss
}

// 返回boltdb存储的数据类型
func parseBackendFreelistType(freelistType string) bolt.FreelistType {
	if freelistType == freelistArrayType {
		return bolt.FreelistArrayType
	}

	return bolt.FreelistMapType
}
