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

package config

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/transport"
	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/etcd/datadir"
	"github.com/ls-2018/etcd_cn/pkg/netutil"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// ServerConfig 持有从命令行或发现中获取的etcd的配置.
type ServerConfig struct {
	Name                   string
	DiscoveryURL           string // 节点发现
	DiscoveryProxy         string // discovery代理
	ClientURLs             types.URLs
	PeerURLs               types.URLs
	DataDir                string
	DedicatedWALDir        string // 配置将使etcd把WAL写到WALDir 而不是dataDir/member/wal.
	SnapshotCount          uint64 // 触发一次磁盘快照的提交事务的次数
	SnapshotCatchUpEntries uint64 // 是slow follower在raft存储条目落后追赶的条目数量.我们希望follower与leader有一毫秒级的延迟.最大的吞吐量是10K左右.保持5K的条目就足以帮助follower赶上.
	MaxSnapFiles           uint
	MaxWALFiles            uint
	BackendBatchInterval   time.Duration     // 提交后端事务前的最长时间
	BackendBatchLimit      int               // 提交后端事务前的最大操作量
	BackendFreelistType    bolt.FreelistType // boltdb存储的类型
	InitialPeerURLsMap     types.URLsMap     // 节点 --- 【 通信地址】可能绑定了多块网卡
	InitialClusterToken    string
	NewCluster             bool
	PeerTLSInfo            transport.TLSInfo
	CORS                   map[string]struct{}
	HostWhitelist          map[string]struct{} // 列出了客户端请求中可接受的主机名.如果etcd是不安全的(没有TLS),etcd只接受其Host头值存在于此白名单的请求.

	TickMs        uint // tick计时器触发间隔
	ElectionTicks int  // 返回选举权检查对应多少次tick触发次数

	// InitialElectionTickAdvance 是否提前初始化选举时钟启动,以便更快的选举
	InitialElectionTickAdvance bool

	BootstrapTimeout time.Duration // 引导超时

	AutoCompactionRetention time.Duration
	AutoCompactionMode      string

	CompactionBatchLimit int
	QuotaBackendBytes    int64 // bolt.db 存储上限 【字节】
	MaxTxnOps            uint  // 事务中允许的最大操作数

	// MaxRequestBytes raft发送的最大数据量
	MaxRequestBytes uint

	WarningApplyDuration time.Duration

	StrictReconfigCheck   bool   // 严格配置变更检查
	ClientCertAuthEnabled bool   // 验证客户端证书是不是服务器CA签署的
	AuthToken             string // 认证格式  simple、jwt
	BcryptCost            uint   // 为散列身份验证密码指定bcrypt算法的成本/强度默认10
	TokenTTL              uint

	InitialCorruptCheck bool // 数据毁坏检测功能,运行之后,在开始服务之前
	CorruptCheckTime    time.Duration

	PreVote bool // PreVote 是否启用PreVote

	// SocketOpts are socket options passed to listener config.
	SocketOpts transport.SocketOpts

	// Logger logs etcd-side operations.
	Logger *zap.Logger

	ForceNewCluster bool

	EnableLeaseCheckpoint bool // 允许leader定期向其他成员发送检查点,以防止leader变化时剩余TTL重置.
	// LeaseCheckpointInterval time.Duration is the wait duration between lease checkpoints.
	LeaseCheckpointInterval time.Duration
	// LeaseCheckpointPersist enables persisting remainingTTL to prevent indefinite auto-renewal of long lived leases. Always enabled in v3.6. Should be used to ensure smooth upgrade from v3.5 clusters with this feature enabled.
	LeaseCheckpointPersist bool

	EnableGRPCGateway bool // 启用grpc网关,将 http 转换成 grpc / true

	// ExperimentalEnableDistributedTracing 使用OpenTelemetry协议实现分布式跟踪.
	ExperimentalEnableDistributedTracing bool // 默认false
	// ExperimentalTracerOptions are options for OpenTelemetry gRPC interceptor.
	ExperimentalTracerOptions []otelgrpc.Option

	WatchProgressNotifyInterval time.Duration

	// UnsafeNoFsync 禁用所有fsync的使用.设置这个是不安全的,会导致数据丢失.
	UnsafeNoFsync bool `json:"unsafe-no-fsync"`

	DowngradeCheckTime time.Duration

	// ExperimentalMemoryMlock enables mlocking of etcd owned memory pages.
	// The setting improves etcd tail latency in environments were:
	//   - memory pressure might lead to swapping pages to disk
	//   - disk latency might be unstable
	// Currently all etcd memory gets mlocked, but in future the flag can
	// be refined to mlock in-use area of bbolt only.
	ExperimentalMemoryMlock bool `json:"experimental-memory-mlock"`

	// ExperimentalTxnModeWriteWithSharedBuffer enable write transaction to use
	// a shared buffer in its readonly check operations.
	ExperimentalTxnModeWriteWithSharedBuffer bool `json:"experimental-txn-mode-write-with-shared-buffer"`

	// ExperimentalBootstrapDefragThresholdMegabytes 是指在启动过程中 etcd考虑运行碎片整理所需释放的最小兆字节数.需要设置为非零值才能生效.
	ExperimentalBootstrapDefragThresholdMegabytes uint `json:"experimental-bootstrap-defrag-threshold-megabytes"`

	// V2Deprecation defines a phase of v2store deprecation process.
	V2Deprecation V2DeprecationEnum `json:"v2-deprecation"`
}

// VerifyBootstrap 检查初始配置的引导情况,并对不应该发生的事情返回一个错误.
func (c *ServerConfig) VerifyBootstrap() error {
	if err := c.hasLocalMember(); err != nil { // initial-cluster 集群至少包含本机节点
		return err
	}
	// 主要就是验证  这两个参数  --initial-advertise-peer-urls" and "--initial-cluster
	if err := c.advertiseMatchesCluster(); err != nil {
		return err
	}
	// 检查所有ip:port 有没有重复的,有就返回 true
	if CheckDuplicateURL(c.InitialPeerURLsMap) {
		return fmt.Errorf("初始集群有重复的网址%s", c.InitialPeerURLsMap)
	}
	if c.InitialPeerURLsMap.String() == "" && c.DiscoveryURL == "" {
		return fmt.Errorf("初始集群未设置,没有发现discovery的URL")
	}
	return nil
}

// VerifyJoinExisting  检查加入现有集群的初始配置,并对不应该发生的事情返回一个错误.
func (c *ServerConfig) VerifyJoinExisting() error {
	if err := c.hasLocalMember(); err != nil {
		return err
	}
	if CheckDuplicateURL(c.InitialPeerURLsMap) {
		return fmt.Errorf("初始集群 %s 有重复的地址", c.InitialPeerURLsMap)
	}
	if c.DiscoveryURL != "" {
		return fmt.Errorf("discovery URL 不应该设置,当加入一个存在的初始集群")
	}
	return nil
}

// hasLocalMember 集群至少包含本机节点
func (c *ServerConfig) hasLocalMember() error {
	if urls := c.InitialPeerURLsMap[c.Name]; urls == nil {
		return fmt.Errorf("不能再集群配置中发现本机 %q", c.Name)
	}
	return nil
}

// advertiseMatchesCluster 确认peer URL与集群cluster peer中的URL一致.
func (c *ServerConfig) advertiseMatchesCluster() error {
	urls, apurls := c.InitialPeerURLsMap[c.Name], c.PeerURLs.StringSlice()
	urls.Sort()
	sort.Strings(apurls)
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	ok, err := netutil.URLStringsEqual(ctx, c.Logger, apurls, urls.StringSlice())
	if ok {
		return nil
	}

	initMap, apMap := make(map[string]struct{}), make(map[string]struct{})
	for _, url := range c.PeerURLs {
		apMap[url.String()] = struct{}{}
	}
	for _, url := range c.InitialPeerURLsMap[c.Name] {
		initMap[url.String()] = struct{}{}
	}

	var missing []string
	for url := range initMap {
		if _, ok := apMap[url]; !ok {
			missing = append(missing, url)
		}
	}
	if len(missing) > 0 {
		for i := range missing {
			missing[i] = c.Name + "=" + missing[i]
		}
		mstr := strings.Join(missing, ",")
		apStr := strings.Join(apurls, ",")
		return fmt.Errorf("--initial-cluster 有 %s但丢失了--initial-advertise-peer-urls=%s (%v)", mstr, apStr, err)
	}

	for url := range apMap {
		if _, ok := initMap[url]; !ok {
			missing = append(missing, url)
		}
	}
	if len(missing) > 0 {
		mstr := strings.Join(missing, ",")
		umap := types.URLsMap(map[string]types.URLs{c.Name: c.PeerURLs})
		return fmt.Errorf("--initial-advertise-peer-urls 有 %s但丢失了--initial-cluster=%s", mstr, umap.String())
	}

	// resolved URLs from "--initial-advertise-peer-urls" and "--initial-cluster" did not match or failed
	apStr := strings.Join(apurls, ",")
	umap := types.URLsMap(map[string]types.URLs{c.Name: c.PeerURLs})
	return fmt.Errorf("无法解决 %s 匹配--initial-cluster=%s 的问题(%v)", apStr, umap.String(), err)
}

// MemberDir default.etcd/member
func (c *ServerConfig) MemberDir() string {
	return datadir.ToMemberDir(c.DataDir)
}

// WALDir default.etcd/member/wal
func (c *ServerConfig) WALDir() string {
	if c.DedicatedWALDir != "" { // ""
		return c.DedicatedWALDir
	}
	return datadir.ToWalDir(c.DataDir)
}

// SnapDir default.etcd/member/snap
func (c *ServerConfig) SnapDir() string {
	return datadir.ToSnapDir(c.DataDir)
}

func (c *ServerConfig) ShouldDiscover() bool { return c.DiscoveryURL != "" }

// ReqTimeout 返回请求完成的超时时间
func (c *ServerConfig) ReqTimeout() time.Duration {
	// 5用于队列等待,计算和磁盘IO延迟+ 2倍选举超时
	return 5*time.Second + 2*time.Duration(c.ElectionTicks*int(c.TickMs))*time.Millisecond
}

// ElectionTimeout 选举超时
func (c *ServerConfig) ElectionTimeout() time.Duration {
	return time.Duration(c.ElectionTicks*int(c.TickMs)) * time.Millisecond
}

func (c *ServerConfig) PeerDialTimeout() time.Duration {
	return time.Second + time.Duration(c.ElectionTicks*int(c.TickMs))*time.Millisecond
}

// CheckDuplicateURL 检查所有ip:port 有没有重复的,有就返回 true
func CheckDuplicateURL(urlsmap types.URLsMap) bool {
	um := make(map[string]bool)
	for _, urls := range urlsmap {
		for _, url := range urls {
			u := url.String()
			if um[u] {
				return true
			}
			um[u] = true
		}
	}
	return false
}

// BootstrapTimeoutEffective 有效的Bootstrap超时
func (c *ServerConfig) BootstrapTimeoutEffective() time.Duration {
	if c.BootstrapTimeout != 0 {
		return c.BootstrapTimeout
	}
	return time.Second
}

// BackendPath default.etcd/member/snap/db
func (c *ServerConfig) BackendPath() string { return datadir.ToBackendFileName(c.DataDir) }
