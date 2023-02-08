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

package etcdserver

import (
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ls-2018/etcd_cn/raft"

	"github.com/coreos/go-semver/semver"
	humanize "github.com/dustin/go-humanize"
	"github.com/ls-2018/etcd_cn/etcd/config"
	"go.uber.org/zap"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/fileutil"
	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/etcd/auth"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/membership"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/rafthttp"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/snap"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2discovery"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2http/httptypes"
	stats "github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2stats"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2store"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v3alarm"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v3compactor"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/cindex"
	"github.com/ls-2018/etcd_cn/etcd/lease"
	"github.com/ls-2018/etcd_cn/etcd/mvcc"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"
	"github.com/ls-2018/etcd_cn/etcd/wal"
	"github.com/ls-2018/etcd_cn/offical/api/v3/version"
	pb "github.com/ls-2018/etcd_cn/offical/etcdserverpb"
	"github.com/ls-2018/etcd_cn/pkg/idutil"
	"github.com/ls-2018/etcd_cn/pkg/pbutil"
	"github.com/ls-2018/etcd_cn/pkg/runtime"
	"github.com/ls-2018/etcd_cn/pkg/schedule"
	"github.com/ls-2018/etcd_cn/pkg/traceutil"
	"github.com/ls-2018/etcd_cn/pkg/wait"
	"github.com/ls-2018/etcd_cn/raft/raftpb"
)

const (
	DefaultSnapshotCount = 100000

	// DefaultSnapshotCatchUpEntries is the number of entries for a slow follower
	// to catch-up after compacting the raft storage entries.
	// We expect the follower has a millisecond level latency with the leader.
	// The max throughput is around 10K. Keep a 5K entries is enough for helping
	// follower to catch up.
	DefaultSnapshotCatchUpEntries uint64 = 5000

	StoreClusterPrefix = "/0"
	StoreKeysPrefix    = "/1"

	// HealthInterval is the minimum time the cluster should backend healthy
	// before accepting add member requests.
	HealthInterval = 5 * time.Second

	purgeFileInterval = 30 * time.Second

	// max number of in-flight snapshot messages etcdserver allows to have
	// This number is more than enough for most clusters with 5 machines.
	maxInFlightMsgSnap = 16

	releaseDelayAfterSnapshot = 30 * time.Second

	// maxPendingRevokes is the maximum number of outstanding expired lease revocations.
	maxPendingRevokes = 16

	recommendedMaxRequestBytes = 10 * 1024 * 1024 // 10M

	readyPercent = 0.9

	DowngradeEnabledPath = "/downgrade/enabled"
)

var (
	// monitorVersionInterval should backend smaller than the timeout
	// on the connection. Or we will not backend able to reuse the connection
	// (since it will timeout).
	monitorVersionInterval = rafthttp.ConnWriteTimeout - time.Second

	recommendedMaxRequestBytesString = humanize.Bytes(uint64(recommendedMaxRequestBytes))
	storeMemberAttributeRegexp       = regexp.MustCompile(path.Join(membership.StoreMembersPrefix, "[[:xdigit:]]{1,16}", "attributes"))
)

func init() {
	rand.Seed(time.Now().UnixNano())

	expvar.Publish(
		"file_descriptor_limit",
		expvar.Func(
			func() interface{} {
				n, _ := runtime.FDLimit()
				return n
			},
		),
	)
}

type Response struct {
	Term    uint64
	Index   uint64
	Event   *v2store.Event
	Watcher v2store.Watcher
	Err     error
}

type ServerV2 interface {
	Server
	Leader() types.ID
	// Do takes a V2 request and attempts to fulfill it, returning a Response.
	Do(ctx context.Context, r pb.Request) (Response, error)
	stats.Stats
	ClientCertAuthEnabled() bool
}

type ServerV3 interface {
	Server
	RaftStatusGetter
}

func (s *EtcdServer) ClientCertAuthEnabled() bool { return s.Cfg.ClientCertAuthEnabled }

type Server interface {
	AddMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error)          // http  添加节点
	RemoveMember(ctx context.Context, id uint64) ([]*membership.Member, error)                    // http  移除节点
	UpdateMember(ctx context.Context, updateMemb membership.Member) ([]*membership.Member, error) // http  更新节点
	PromoteMember(ctx context.Context, id uint64) ([]*membership.Member, error)                   // http  提升节点
	ClusterVersion() *semver.Version                                                              //
	Cluster() api.Cluster                                                                         // 返回内部集群cluster 结构体
	Alarms() []*pb.AlarmMember                                                                    //
	LeaderChangedNotify() <-chan struct{}                                                         // 领导者变更通知
	// 1. 当领导层发生变化时,返回的通道将被关闭.
	// 2. 因此,每一个任期都需要获得新的通道.
	// 3. 用户可能会因为使用这个API而失去一些连续的频道变化.
}

// EtcdServer 整个etcd节点的功能的入口,包含etcd节点运行过程中需要的大部分成员.
type EtcdServer struct {
	inflightSnapshots int64  // 当前正在发送的snapshot数量
	appliedIndex      uint64 // 已经apply到状态机的日志index
	committedIndex    uint64 // 已经提交的日志index,也就是leader确认多数成员已经同步了的日志index
	term              uint64
	lead              uint64
	consistIndex      cindex.ConsistentIndexer // 已经持久化到kvstore的index
	r                 raftNode                 // 重要的数据结果,存储了raft的状态机信息.
	readych           chan struct{}            // 启动成功并注册了自己到cluster,关闭这个通道.
	Cfg               config.ServerConfig      // 配置项
	lgMu              *sync.RWMutex
	lg                *zap.Logger
	w                 wait.Wait     // 为了同步调用情况下让调用者阻塞等待调用结果的.
	readMu            sync.RWMutex  // 下面3个结果都是为了实现linearizable 读使用的
	readwaitc         chan struct{} // 通过向readwaitC发送一个空结构体来通知etcd服务器它正在等待读取
	readNotifier      *notifier     // 在没有错误时通知read goroutine 可以处理请求

	stop            chan struct{}           // 停止通道
	stopping        chan struct{}           // 停止时关闭这个通道
	done            chan struct{}           // etcd的start函数中的循环退出,会关闭这个通道
	leaderChanged   chan struct{}           // leader变换后 通知linearizable read loop   drop掉旧的读请求
	leaderChangedMu sync.RWMutex            //
	errorc          chan error              // 错误通道,用以传入不可恢复的错误,关闭raft状态机.
	id              types.ID                // etcd实例id
	attributes      membership.Attributes   // etcd实例属性
	cluster         *membership.RaftCluster // 集群信息
	v2store         v2store.Store           // v2的kv存储
	snapshotter     *snap.Snapshotter       // 用以snapshot
	applyV2         ApplierV2               // v2的applier,用于将commited index apply到raft状态机
	applyV3         applierV3               // v3的applier,用于将commited index apply到raft状态机
	applyV3Base     applierV3               // 剥去了鉴权和配额功能的applyV3
	applyV3Internal applierV3Internal       // v3的内部applier
	applyWait       wait.WaitTime           // apply的等待队列,等待某个index的日志apply完成
	kv              mvcc.WatchableKV        // v3用的kv存储
	lessor          lease.Lessor            // v3用,作用是实现过期时间
	backendLock     sync.Mutex              // 守护后端存储的锁,改变后端存储和获取后端存储是使用
	backend         backend.Backend         // 后端存储  bolt.db
	beHooks         *backendHooks           // 存储钩子
	authStore       auth.AuthStore          // 存储鉴权数据
	alarmStore      *v3alarm.AlarmStore     // 存储告警数据
	stats           *stats.ServerStats      // 当前节点状态
	lstats          *stats.LeaderStats      // leader状态
	SyncTicker      *time.Ticker            // v2用,实现ttl数据过期的
	compactor       v3compactor.Compactor   // 压缩数据的周期任务
	peerRt          http.RoundTripper       // 用于发送远程请求
	reqIDGen        *idutil.Generator       // 用于生成请求id
	// wgMu blocks concurrent waitgroup mutation while etcd stopping
	wgMu sync.RWMutex
	// wg is used to wait for the goroutines that depends on the etcd state
	// to exit when stopping the etcd.
	wg                  sync.WaitGroup
	ctx                 context.Context // 用于由etcd发起的请求这些请求可能需要在etcd关机时被后端取消.
	cancel              context.CancelFunc
	leadTimeMu          sync.RWMutex
	leadElectedTime     time.Time
	firstCommitInTermMu sync.RWMutex
	firstCommitInTermC  chan struct{} // 任期内的第一次commit时创建的
	*AccessController
}

// 后端存储钩子
type backendHooks struct {
	indexer   cindex.ConsistentIndexer // 一致性存储的索引
	lg        *zap.Logger
	confState raftpb.ConfState // 集群当前的配置信息
	// first write changes it to 'dirty'. false by default, so
	// not initialized `confState` is meaningless.
	confStateDirty bool
	confStateLock  sync.Mutex
}

func (bh *backendHooks) OnPreCommitUnsafe(tx backend.BatchTx) {
	bh.indexer.UnsafeSave(tx)
	bh.confStateLock.Lock()
	defer bh.confStateLock.Unlock()
	if bh.confStateDirty {
		membership.MustUnsafeSaveConfStateToBackend(bh.lg, tx, &bh.confState)
		// save bh.confState
		bh.confStateDirty = false
	}
}

func (bh *backendHooks) SetConfState(confState *raftpb.ConfState) {
	bh.confStateLock.Lock()
	defer bh.confStateLock.Unlock()
	bh.confState = *confState
	bh.confStateDirty = true
}

type Temp struct {
	Bepath   string
	W        *wal.WAL
	N        raft.RaftNodeInterFace
	S        *raft.MemoryStorage
	ID       types.ID
	CL       *membership.RaftCluster
	Remotes  []*membership.Member
	Snapshot *raftpb.Snapshot
	Prt      http.RoundTripper
	SS       *snap.Snapshotter
	ST       v2store.Store
	CI       cindex.ConsistentIndexer
	BeExist  bool
	BeHooks  *backendHooks
	BE       backend.Backend
}

func MySelfStartRaft(cfg config.ServerConfig) (temp *Temp, err error) {
	temp = &Temp{}
	temp.ST = v2store.New(StoreClusterPrefix, StoreKeysPrefix) // 创建了一个store结构体   /0 /1

	if cfg.MaxRequestBytes > recommendedMaxRequestBytes { // 10M
		cfg.Logger.Warn(
			"超过了建议的请求限度",
			zap.Uint("max-request-bytes", cfg.MaxRequestBytes),
			zap.String("max-request-size", humanize.Bytes(uint64(cfg.MaxRequestBytes))),
			zap.Int("recommended-request-bytes", recommendedMaxRequestBytes),
			zap.String("recommended-request-size", recommendedMaxRequestBytesString),
		)
	}
	// 存在也可以
	if terr := fileutil.TouchDirAll(cfg.DataDir); terr != nil {
		return nil, fmt.Errorf("无法访问数据目录: %v", terr)
	}

	haveWAL := wal.Exist(cfg.WALDir()) // default.etcd/member/wal
	// default.etcd/member/snap
	if err = fileutil.TouchDirAll(cfg.SnapDir()); err != nil {
		cfg.Logger.Fatal(
			"创建快照目录失败",
			zap.String("path", cfg.SnapDir()),
			zap.Error(err),
		)
	}
	// 移除格式匹配的文件
	if err = fileutil.RemoveMatchFile(cfg.Logger, cfg.SnapDir(), func(fileName string) bool {
		return strings.HasPrefix(fileName, "tmp")
	}); err != nil {
		cfg.Logger.Error(
			"删除快照目录下的临时文件",
			zap.String("path", cfg.SnapDir()),
			zap.Error(err),
		)
	}
	// 创建快照struct
	temp.SS = snap.New(cfg.Logger, cfg.SnapDir())

	temp.Bepath = cfg.BackendPath() // default.etcd/member/snap/db
	temp.BeExist = fileutil.Exist(temp.Bepath)

	temp.CI = cindex.NewConsistentIndex(nil) // pointer
	temp.BeHooks = &backendHooks{lg: cfg.Logger, indexer: temp.CI}
	temp.BE = openBackend(cfg, temp.BeHooks)
	temp.CI.SetBackend(temp.BE)
	cindex.CreateMetaBucket(temp.BE.BatchTx())

	// 启动时,判断要不要进行碎片整理
	if cfg.ExperimentalBootstrapDefragThresholdMegabytes != 0 {
		err := maybeDefragBackend(cfg, temp.BE)
		if err != nil {
			return nil, err
		}
	}

	defer func() {
		if err != nil {
			temp.BE.Close()
		}
	}()
	// 服务端的
	temp.Prt, err = rafthttp.NewRoundTripper(cfg.PeerTLSInfo, cfg.PeerDialTimeout())
	if err != nil {
		return nil, err
	}

	switch {
	case !haveWAL && !cfg.NewCluster: // false true   重新加入的成员
		if err = cfg.VerifyJoinExisting(); err != nil {
			return nil, err
		}
		temp.CL, err = membership.NewClusterFromURLsMap(cfg.Logger, cfg.InitialClusterToken, cfg.InitialPeerURLsMap)
		if err != nil {
			return nil, err
		}
		existingCluster, gerr := GetClusterFromRemotePeers(cfg.Logger, getRemotePeerURLs(temp.CL, cfg.Name), temp.Prt)
		if gerr != nil {
			return nil, fmt.Errorf("cannot fetch cluster info from peer urls: %v", gerr)
		}
		if err = membership.ValidateClusterAndAssignIDs(cfg.Logger, temp.CL, existingCluster); err != nil {
			return nil, fmt.Errorf("error validating peerURLs %s: %v", existingCluster, err)
		}
		if !isCompatibleWithCluster(cfg.Logger, temp.CL, temp.CL.MemberByName(cfg.Name).ID, temp.Prt) {
			return nil, fmt.Errorf("incompatible with current running cluster")
		}

		temp.Remotes = existingCluster.Members()
		temp.CL.SetID(types.ID(0), existingCluster.ID())
		temp.CL.SetStore(temp.ST)
		temp.CL.SetBackend(temp.BE)
		temp.ID, temp.N, temp.S, temp.W = startNode(cfg, temp.CL, nil)
		temp.CL.SetID(temp.ID, existingCluster.ID())

	case !haveWAL && cfg.NewCluster: // false true   初始新成员
		if err = cfg.VerifyBootstrap(); err != nil { // 验证peer 通信地址、--initial-advertise-peer-urls" and "--initial-cluster
			return nil, err
		}
		// 创建RaftCluster
		temp.CL, err = membership.NewClusterFromURLsMap(cfg.Logger, cfg.InitialClusterToken, cfg.InitialPeerURLsMap)
		if err != nil {
			return nil, err
		}
		m := temp.CL.MemberByName(cfg.Name) // 返回本节点的信息
		if isMemberBootstrapped(cfg.Logger, temp.CL, cfg.Name, temp.Prt, cfg.BootstrapTimeoutEffective()) {
			return nil, fmt.Errorf("成员 %s 已经引导过", m.ID)
		}
		// TODO 是否使用discovery 发现其他节点
		if cfg.ShouldDiscover() {
			var str string
			str, err = v2discovery.JoinCluster(cfg.Logger, cfg.DiscoveryURL, cfg.DiscoveryProxy, m.ID, cfg.InitialPeerURLsMap.String())
			if err != nil {
				return nil, &DiscoveryError{Op: "join", Err: err}
			}
			var urlsmap types.URLsMap
			urlsmap, err = types.NewURLsMap(str)
			if err != nil {
				return nil, err
			}
			if config.CheckDuplicateURL(urlsmap) {
				return nil, fmt.Errorf("discovery cluster %s has duplicate url", urlsmap)
			}
			if temp.CL, err = membership.NewClusterFromURLsMap(cfg.Logger, cfg.InitialClusterToken, urlsmap); err != nil {
				return nil, err
			}
		}
		temp.CL.SetStore(temp.ST) // 结构体
		temp.CL.SetBackend(temp.BE)
		// 启动节点
		temp.ID, temp.N, temp.S, temp.W = startNode(cfg, temp.CL, temp.CL.MemberIDs()) // ✅✈️ 🚗🚴🏻😁
		temp.CL.SetID(temp.ID, temp.CL.ID())

	case haveWAL:
		if err = fileutil.IsDirWriteable(cfg.MemberDir()); err != nil {
			return nil, fmt.Errorf("cannot write to member directory: %v", err)
		}

		if err = fileutil.IsDirWriteable(cfg.WALDir()); err != nil {
			return nil, fmt.Errorf("cannot write to WAL directory: %v", err)
		}

		if cfg.ShouldDiscover() {
			cfg.Logger.Warn(
				"discovery token is ignored since cluster already initialized; valid logs are found",
				zap.String("wal-dir", cfg.WALDir()),
			)
		}

		// Find a snapshot to start/restart a raft node
		walSnaps, err := wal.ValidSnapshotEntries(cfg.Logger, cfg.WALDir())
		if err != nil {
			return nil, err
		}
		// snapshot files can backend orphaned if etcd crashes after writing them but before writing the corresponding
		// wal log entries
		temp.Snapshot, err = temp.SS.LoadNewestAvailable(walSnaps)
		if err != nil && err != snap.ErrNoSnapshot {
			return nil, err
		}

		if temp.Snapshot != nil {
			if err = temp.ST.Recovery(temp.Snapshot.Data); err != nil {
				cfg.Logger.Panic("failed to recover from snapshot", zap.Error(err))
			}

			if err = assertNoV2StoreContent(cfg.Logger, temp.ST, cfg.V2Deprecation); err != nil {
				cfg.Logger.Error("illegal v2store content", zap.Error(err))
				return nil, err
			}

			cfg.Logger.Info(
				"recovered v2 store from snapshot",
				zap.Uint64("snapshot-index", temp.Snapshot.Metadata.Index),
				zap.String("snapshot-size", humanize.Bytes(uint64(temp.Snapshot.Size()))),
			)

			if temp.BE, err = recoverSnapshotBackend(cfg, temp.BE, *temp.Snapshot, temp.BeExist, temp.BeHooks); err != nil {
				cfg.Logger.Panic("failed to recover v3 backend from snapshot", zap.Error(err))
			}
			// A snapshot db may have already been recovered, and the old db should have
			// already been closed in this case, so we should set the backend again.
			temp.CI.SetBackend(temp.BE)
			s1, s2 := temp.BE.Size(), temp.BE.SizeInUse()
			cfg.Logger.Info(
				"recovered v3 backend from snapshot",
				zap.Int64("backend-size-bytes", s1),
				zap.String("backend-size", humanize.Bytes(uint64(s1))),
				zap.Int64("backend-size-in-use-bytes", s2),
				zap.String("backend-size-in-use", humanize.Bytes(uint64(s2))),
			)
		} else {
			cfg.Logger.Info("No snapshot found. Recovering WAL from scratch!")
		}

		if !cfg.ForceNewCluster {
			temp.ID, temp.CL, temp.N, temp.S, temp.W = restartNode(cfg, temp.Snapshot)
		} else {
			temp.ID, temp.CL, temp.N, temp.S, temp.W = restartAsStandaloneNode(cfg, temp.Snapshot)
		}

		temp.CL.SetStore(temp.ST)
		temp.CL.SetBackend(temp.BE)
		temp.CL.Recover(api.UpdateCapability)
		if temp.CL.Version() != nil && !temp.CL.Version().LessThan(semver.Version{Major: 3}) && !temp.BeExist {
			os.RemoveAll(temp.Bepath)
			return nil, fmt.Errorf("database file (%v) of the backend is missing", temp.Bepath)
		}

	default:
		return nil, fmt.Errorf("不支持的引导配置")
	}

	if terr := fileutil.TouchDirAll(cfg.MemberDir()); terr != nil {
		return nil, fmt.Errorf("不能访问成员目录: %v", terr)
	}

	return
}

// NewServer 根据提供的配置创建一个新的EtcdServer.在EtcdServer的生命周期内,该配置被认为是静态的.
func NewServer(cfg config.ServerConfig) (srv *EtcdServer, err error) {
	temp := &Temp{}
	temp, err = MySelfStartRaft(cfg) // 逻辑时钟初始化
	serverStats := stats.NewServerStats(cfg.Name, temp.ID.String())
	leaderStats := stats.NewLeaderStats(cfg.Logger, temp.ID.String())

	heartbeat := time.Duration(cfg.TickMs) * time.Millisecond
	srv = &EtcdServer{
		readych:     make(chan struct{}),
		Cfg:         cfg,
		lgMu:        new(sync.RWMutex),
		lg:          cfg.Logger,
		errorc:      make(chan error, 1),
		v2store:     temp.ST,
		snapshotter: temp.SS,
		r: *newRaftNode(
			raftNodeConfig{
				lg:                cfg.Logger,
				isIDRemoved:       func(id uint64) bool { return temp.CL.IsIDRemoved(types.ID(id)) },
				RaftNodeInterFace: temp.N,
				heartbeat:         heartbeat,
				raftStorage:       temp.S,
				storage:           NewStorage(temp.W, temp.SS),
			},
		),
		id:                 temp.ID,
		attributes:         membership.Attributes{Name: cfg.Name, ClientURLs: cfg.ClientURLs.StringSlice()},
		cluster:            temp.CL,
		stats:              serverStats,
		lstats:             leaderStats,
		SyncTicker:         time.NewTicker(500 * time.Millisecond),
		peerRt:             temp.Prt,
		reqIDGen:           idutil.NewGenerator(uint16(temp.ID), time.Now()),
		AccessController:   &AccessController{CORS: cfg.CORS, HostWhitelist: cfg.HostWhitelist},
		consistIndex:       temp.CI,
		firstCommitInTermC: make(chan struct{}),
	}
	srv.applyV2 = NewApplierV2(cfg.Logger, srv.v2store, srv.cluster)

	srv.backend = temp.BE
	srv.beHooks = temp.BeHooks
	// 可能为了确保发生leader选举时,lease不会过期,最小ttl应该比选举时间长,看代码
	minTTL := time.Duration((3*cfg.ElectionTicks)/2) * heartbeat
	// 默认的情况下应该是2s,

	// 始终在KV之前恢复出租人.当我们恢复mvcc.KV时,它将把钥匙重新连接到它的租约上.如果我们先恢复mvcc.KV,它将在恢复前把钥匙附加到错误的出租人上.
	srv.lessor = lease.NewLessor(srv.Logger(), srv.backend, srv.cluster, lease.LessorConfig{
		MinLeaseTTL:                int64(math.Ceil(minTTL.Seconds())),
		CheckpointInterval:         cfg.LeaseCheckpointInterval,
		CheckpointPersist:          cfg.LeaseCheckpointPersist,
		ExpiredLeasesRetryInterval: srv.Cfg.ReqTimeout(),
	})

	tp, err := auth.NewTokenProvider(cfg.Logger, cfg.AuthToken, // 认证格式  simple、jwt
		func(index uint64) <-chan struct{} {
			return srv.applyWait.Wait(index)
		},
		time.Duration(cfg.TokenTTL)*time.Second,
	)
	if err != nil {
		cfg.Logger.Warn("创建令牌提供程序失败", zap.Error(err))
		return nil, err
	}
	// watch | kv ...
	srv.kv = mvcc.New(srv.Logger(), srv.backend, srv.lessor, mvcc.StoreConfig{CompactionBatchLimit: cfg.CompactionBatchLimit})

	kvindex := temp.CI.ConsistentIndex()
	srv.lg.Debug("恢复consistentIndex", zap.Uint64("index", kvindex))
	if temp.BeExist {
		// TODO: remove kvindex != 0 checking when we do not expect users to upgrade
		// etcd from pre-3.0 release.
		if temp.Snapshot != nil && kvindex < temp.Snapshot.Metadata.Index {
			if kvindex != 0 {
				return nil, fmt.Errorf("database file (%v index %d) does not match with snapshot (index %d)", temp.Bepath, kvindex, temp.Snapshot.Metadata.Index)
			}
			cfg.Logger.Warn(
				"consistent index was never saved",
				zap.Uint64("snapshot-index", temp.Snapshot.Metadata.Index),
			)
		}
	}

	srv.authStore = auth.NewAuthStore(srv.Logger(), srv.backend, tp, int(cfg.BcryptCost)) // BcryptCost 为散列身份验证密码指定bcrypt算法的成本/强度默认10

	newSrv := srv // since srv == nil in defer if srv is returned as nil
	defer func() {
		// closing backend without first closing kv can cause
		// resumed compactions to fail with closed tx errors
		if err != nil {
			newSrv.kv.Close()
		}
	}()
	if num := cfg.AutoCompactionRetention; num != 0 {
		srv.compactor, err = v3compactor.New(cfg.Logger, cfg.AutoCompactionMode, num, srv.kv, srv)
		if err != nil {
			return nil, err
		}
		srv.compactor.Run()
	}

	srv.applyV3Base = srv.newApplierV3Backend()
	srv.applyV3Internal = srv.newApplierV3Internal()
	// 启动时重置所有警报
	if err = srv.restoreAlarms(); err != nil {
		return nil, err
	}

	if srv.Cfg.EnableLeaseCheckpoint {
		// 通过设置checkpointer使能租期检查点功能.
		srv.lessor.SetCheckpointer(func(ctx context.Context, cp *pb.LeaseCheckpointRequest) {
			// 定期批量地将 Lease 剩余的 TTL 基于 Raft Log 同步给 Follower 节点,Follower 节点收到 CheckPoint 请求后,
			// 更新内存数据结构 LeaseMap 的剩余 TTL 信息.
			srv.raftRequestOnce(ctx, pb.InternalRaftRequest{LeaseCheckpoint: cp})
		})
	}

	// TODO: move transport initialization near the definition of remote
	tr := &rafthttp.Transport{
		Logger:      cfg.Logger,
		TLSInfo:     cfg.PeerTLSInfo,
		DialTimeout: cfg.PeerDialTimeout(),
		ID:          temp.ID,
		URLs:        cfg.PeerURLs,
		ClusterID:   temp.CL.ID(),
		Raft:        srv,
		Snapshotter: temp.SS,
		ServerStats: serverStats,
		LeaderStats: leaderStats,
		ErrorC:      srv.errorc,
	}
	if err = tr.Start(); err != nil {
		return nil, err
	}
	// add all remotes into transport
	for _, m := range temp.Remotes {
		if m.ID != temp.ID {
			tr.AddRemote(m.ID, m.PeerURLs)
		}
	}
	for _, m := range temp.CL.Members() {
		if m.ID != temp.ID {
			tr.AddPeer(m.ID, m.PeerURLs)
		}
	}
	srv.r.transport = tr

	return srv, nil
}

// assertNoV2StoreContent -> depending on the deprecation stage, warns or report an error
// if the v2store contains custom content.
func assertNoV2StoreContent(lg *zap.Logger, st v2store.Store, deprecationStage config.V2DeprecationEnum) error {
	metaOnly, err := membership.IsMetaStoreOnly(st)
	if err != nil {
		return err
	}
	if metaOnly {
		return nil
	}
	if deprecationStage.IsAtLeast(config.V2_DEPR_1_WRITE_ONLY) {
		return fmt.Errorf("detected disallowed custom content in v2store for stage --v2-deprecation=%s", deprecationStage)
	}
	lg.Warn("detected custom v2store content. Etcd v3.5 is the last version allowing to access it using API v2. Please remove the content.")
	return nil
}

func (s *EtcdServer) Logger() *zap.Logger {
	s.lgMu.RLock()
	l := s.lg
	s.lgMu.RUnlock()
	return l
}

func tickToDur(ticks int, tickMs uint) string {
	return fmt.Sprintf("%v", time.Duration(ticks)*time.Duration(tickMs)*time.Millisecond)
}

func (s *EtcdServer) adjustTicks() {
	lg := s.Logger()
	clusterN := len(s.cluster.Members())

	// single-node fresh start, or single-node recovers from snapshot
	if clusterN == 1 {
		ticks := s.Cfg.ElectionTicks - 1
		lg.Info(
			"started as single-node; fast-forwarding election ticks",
			zap.String("local-member-id", s.ID().String()),
			zap.Int("forward-ticks", ticks),
			zap.String("forward-duration", tickToDur(ticks, s.Cfg.TickMs)),
			zap.Int("election-ticks", s.Cfg.ElectionTicks),
			zap.String("election-timeout", tickToDur(s.Cfg.ElectionTicks, s.Cfg.TickMs)),
		)
		s.r.advanceTicks(ticks)
		return
	}

	if !s.Cfg.InitialElectionTickAdvance {
		lg.Info("skipping initial election tick advance", zap.Int("election-ticks", s.Cfg.ElectionTicks))
		return
	}
	lg.Info("starting initial election tick advance", zap.Int("election-ticks", s.Cfg.ElectionTicks))

	// retry up to "rafthttp.ConnReadTimeout", which is 5-sec
	// until peer connection reports; otherwise:
	// 1. all connections failed, or
	// 2. no active peers, or
	// 3. restarted single-node with no snapshot
	// then, do nothing, because advancing ticks would have no effect
	waitTime := rafthttp.ConnReadTimeout
	itv := 50 * time.Millisecond
	for i := int64(0); i < int64(waitTime/itv); i++ {
		select {
		case <-time.After(itv):
		case <-s.stopping:
			return
		}

		peerN := s.r.transport.ActivePeers()
		if peerN > 1 {
			// multi-node received peer connection reports
			// adjust ticks, in case slow leader message receive
			ticks := s.Cfg.ElectionTicks - 2

			lg.Info(
				"initialized peer connections; fast-forwarding election ticks",
				zap.String("local-member-id", s.ID().String()),
				zap.Int("forward-ticks", ticks),
				zap.String("forward-duration", tickToDur(ticks, s.Cfg.TickMs)),
				zap.Int("election-ticks", s.Cfg.ElectionTicks),
				zap.String("election-timeout", tickToDur(s.Cfg.ElectionTicks, s.Cfg.TickMs)),
				zap.Int("active-remote-members", peerN),
			)

			s.r.advanceTicks(ticks)
			return
		}
	}
}

func (s *EtcdServer) Start() {
	s.start()
	s.GoAttach(func() { s.adjustTicks() })
	s.GoAttach(func() { s.publish(s.Cfg.ReqTimeout()) })
	s.GoAttach(s.purgeFile)
	s.GoAttach(s.monitorVersions)
	s.GoAttach(s.linearizableReadLoop)
	s.GoAttach(s.monitorKVHash)
	s.GoAttach(s.monitorDowngrade)
}

func (s *EtcdServer) start() {
	lg := s.Logger()

	if s.Cfg.SnapshotCount == 0 { // 触发一次磁盘快照的提交事务的次数
		lg.Info("更新快照数量为默认值",
			zap.Uint64("given-snapshot-count", s.Cfg.SnapshotCount), // 触发一次磁盘快照的提交事务的次数
			zap.Uint64("updated-snapshot-count", DefaultSnapshotCount),
		)
		s.Cfg.SnapshotCount = DefaultSnapshotCount // 触发一次磁盘快照的提交事务的次数
	}
	if s.Cfg.SnapshotCatchUpEntries == 0 {
		lg.Info("将快照追赶条目更新为默认条目",
			zap.Uint64("given-snapshot-catchup-entries", s.Cfg.SnapshotCatchUpEntries),
			zap.Uint64("updated-snapshot-catchup-entries", DefaultSnapshotCatchUpEntries),
		)
		s.Cfg.SnapshotCatchUpEntries = DefaultSnapshotCatchUpEntries
	}

	s.w = wait.New()
	s.applyWait = wait.NewTimeList()
	s.done = make(chan struct{})
	s.stop = make(chan struct{})
	s.stopping = make(chan struct{}, 1)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.readwaitc = make(chan struct{}, 1)
	s.readNotifier = newNotifier()
	s.leaderChanged = make(chan struct{})
	if s.ClusterVersion() != nil {
		lg.Info("启动etcd", zap.String("local-member-id", s.ID().String()),
			zap.String("local-etcd-version", version.Version),
			zap.String("cluster-id", s.Cluster().ID().String()),
			zap.String("cluster-version", version.Cluster(s.ClusterVersion().String())),
		)
	} else {
		lg.Info("启动etcd", zap.String("local-member-id", s.ID().String()),
			zap.String("local-etcd-version", version.Version), zap.String("cluster-version", "to_be_decided"))
	}

	go s.run()
}

func (s *EtcdServer) purgeFile() {
	lg := s.Logger()
	var dberrc, serrc, werrc <-chan error
	var dbdonec, sdonec, wdonec <-chan struct{}
	if s.Cfg.MaxSnapFiles > 0 {
		dbdonec, dberrc = fileutil.PurgeFileWithDoneNotify(lg, s.Cfg.SnapDir(), "snap.db", s.Cfg.MaxSnapFiles, purgeFileInterval, s.stopping)
		sdonec, serrc = fileutil.PurgeFileWithDoneNotify(lg, s.Cfg.SnapDir(), "snap", s.Cfg.MaxSnapFiles, purgeFileInterval, s.stopping)
	}
	if s.Cfg.MaxWALFiles > 0 {
		wdonec, werrc = fileutil.PurgeFileWithDoneNotify(lg, s.Cfg.WALDir(), "wal", s.Cfg.MaxWALFiles, purgeFileInterval, s.stopping)
	}

	select {
	case e := <-dberrc:
		lg.Fatal("failed to purge snap db file", zap.Error(e))
	case e := <-serrc:
		lg.Fatal("failed to purge snap file", zap.Error(e))
	case e := <-werrc:
		lg.Fatal("failed to purge wal file", zap.Error(e))
	case <-s.stopping:
		if dbdonec != nil {
			<-dbdonec
		}
		if sdonec != nil {
			<-sdonec
		}
		if wdonec != nil {
			<-wdonec
		}
		return
	}
}

type ServerPeer interface {
	ServerV2
	RaftHandler() http.Handler
	LeaseHandler() http.Handler
}

func (s *EtcdServer) RaftHandler() http.Handler {
	return s.r.transport.Handler()
}

type ServerPeerV2 interface {
	ServerPeer
	HashKVHandler() http.Handler
	DowngradeEnabledHandler() http.Handler
}

func (s *EtcdServer) ReportUnreachable(id uint64) {
	s.r.ReportUnreachable(id)
}

// ReportSnapshot reports snapshot sent status to the raft state machine,
// and clears the used snapshot from the snapshot store.
func (s *EtcdServer) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	s.r.ReportSnapshot(id, status)
}

type etcdProgress struct {
	confState raftpb.ConfState
	snapi     uint64
	appliedt  uint64
	appliedi  uint64
}

// raftReadyHandler contains a set of EtcdServer operations to backend called by raftNode,
// and helps decouple state machine logic from Raft algorithms.
// TODO: add a state machine interface to apply the commit entries and do snapshot/recover
type raftReadyHandler struct {
	getLead              func() (lead uint64)
	updateLead           func(lead uint64)
	updateLeadership     func(newLeader bool)
	updateCommittedIndex func(uint64)
}

func (s *EtcdServer) run() {
	lg := s.Logger()

	sn, err := s.r.raftStorage.Snapshot()
	if err != nil {
		lg.Panic("从Raft存储获取快照失败", zap.Error(err))
	}

	// asynchronously accept apply packets, dispatch progress in-order
	sched := schedule.NewFIFOScheduler()

	var (
		smu   sync.RWMutex
		syncC <-chan time.Time
	)
	setSyncC := func(ch <-chan time.Time) {
		smu.Lock()
		syncC = ch
		smu.Unlock()
	}
	getSyncC := func() (ch <-chan time.Time) {
		smu.RLock()
		ch = syncC
		smu.RUnlock()
		return
	}
	rh := &raftReadyHandler{
		getLead:    func() (lead uint64) { return s.getLead() },
		updateLead: func(lead uint64) { s.setLead(lead) },
		updateLeadership: func(newLeader bool) {
			if !s.isLeader() {
				// 自己不是leader了
				if s.lessor != nil {
					s.lessor.Demote() // 持久化所有租约
				}
				if s.compactor != nil {
					s.compactor.Pause()
				}
				setSyncC(nil)
			} else {
				if newLeader {
					t := time.Now()
					s.leadTimeMu.Lock()
					s.leadElectedTime = t
					s.leadTimeMu.Unlock()
				}
				setSyncC(s.SyncTicker.C)
				if s.compactor != nil {
					s.compactor.Resume()
				}
			}
			if newLeader {
				s.leaderChangedMu.Lock()
				lc := s.leaderChanged
				s.leaderChanged = make(chan struct{})
				close(lc)
				s.leaderChangedMu.Unlock()
			}
			// TODO: remove the nil checking
			// current test utility does not provide the stats
			if s.stats != nil {
				s.stats.BecomeLeader()
			}
		},
		updateCommittedIndex: func(ci uint64) {
			cci := s.getCommittedIndex()
			if ci > cci {
				s.setCommittedIndex(ci)
			}
		},
	}
	s.r.start(rh)

	ep := etcdProgress{
		confState: sn.Metadata.ConfState,
		snapi:     sn.Metadata.Index,
		appliedt:  sn.Metadata.Term,
		appliedi:  sn.Metadata.Index,
	}

	defer func() {
		s.wgMu.Lock() // block concurrent waitgroup adds in GoAttach while stopping
		close(s.stopping)
		s.wgMu.Unlock()
		s.cancel()
		sched.Stop()

		// wait for gouroutines before closing raft so wal stays open
		s.wg.Wait()

		s.SyncTicker.Stop()

		// must stop raft after scheduler-- etcdserver can leak rafthttp pipelines
		// by adding a peer after raft stops the transport
		s.r.stop()

		s.Cleanup()

		close(s.done)
	}()
	var expiredLeaseC <-chan []*lease.Lease // 返回一个用于接收过期租约的CHAN.
	if s.lessor != nil {                    // v3用,作用是实现过期时间
		expiredLeaseC = s.lessor.ExpiredLeasesC()
	}

	for {
		select {
		case ap := <-s.r.apply():
			// 集群启动时,会先apply两条消息
			// index1:EntryConfChange {"Type":0,"NodeID":10276657743932975437,"Context":"{\"id\":10276657743932975437,\"peerURLs\":[\"http://localhost:2380\"],\"name\":\"default\"}","ID":0}
			// index2:EntryNormal nil    用于任期内第一次commit
			// index3:EntryNormal {"ID":7587861549007417858,"Method":"PUT","Path":"/0/members/8e9e05c52164694d/attributes","Val":"{\"name\":\"default\",\"clientURLs\":[\"http://localhost:2379\"]}","Dir":false,"PrevValue":"","PrevIndex":0,"Expiration":0,"Wait":false,"Since":0,"Recursive":false,"Sorted":false,"Quorum":false,"Time":0,"Stream":false}
			// 读取 放入applyc的消息
			f := func(context.Context) {
				s.applyAll(&ep, &ap)
			}
			sched.Schedule(f)
		case leases := <-expiredLeaseC:
			s.GoAttach(func() {
				// 通过并行化增加过期租约删除过程的吞吐量
				c := make(chan struct{}, maxPendingRevokes) // 控制每一批  并发数为16
				for _, lease := range leases {
					select {
					case c <- struct{}{}:
					case <-s.stopping:
						return
					}
					lid := lease.ID
					s.GoAttach(func() {
						ctx := s.authStore.WithRoot(s.ctx)
						_, lerr := s.LeaseRevoke(ctx, &pb.LeaseRevokeRequest{ID: int64(lid)})
						if lerr == nil {
						} else {
							lg.Warn("移除租约失败", zap.String("lease-id", fmt.Sprintf("%016x", lid)), zap.Error(lerr))
						}
						<-c
					})
				}
			})
		case err := <-s.errorc:
			lg.Warn("etcd error", zap.Error(err))
			lg.Warn("本机使用的data-dir必须移除")
			return
		case <-getSyncC():
			if s.v2store.HasTTLKeys() {
				s.sync(s.Cfg.ReqTimeout())
			}
		case <-s.stop:
			return
		}
	}
}

// Cleanup removes allocated objects by EtcdServer.NewServer in
// situation that EtcdServer::Start was not called (that takes care of cleanup).
func (s *EtcdServer) Cleanup() {
	// kv, lessor and backend can backend nil if running without v3 enabled
	// or running unit tests.
	if s.lessor != nil {
		s.lessor.Stop()
	}
	if s.kv != nil {
		s.kv.Close()
	}
	if s.authStore != nil {
		s.authStore.Close()
	}
	if s.backend != nil {
		s.backend.Close()
	}
	if s.compactor != nil {
		s.compactor.Stop()
	}
}

func (s *EtcdServer) applyAll(ep *etcdProgress, apply *apply) {
	s.applySnapshot(ep, apply) // 从持久化的内存存储中恢复出快照
	s.applyEntries(ep, apply)

	s.applyWait.Trigger(ep.appliedi)

	// wait for the raft routine to finish the disk writes before triggering a
	// snapshot. or applied index might backend greater than the last index in raft
	// storage, since the raft routine might backend slower than apply routine.
	<-apply.notifyc

	s.triggerSnapshot(ep)
	select {
	// snapshot requested via send()
	case m := <-s.r.msgSnapC:
		merged := s.createMergedSnapshotMessage(m, ep.appliedt, ep.appliedi, ep.confState)
		s.sendMergedSnap(merged)
	default:
	}
}

func (s *EtcdServer) applySnapshot(ep *etcdProgress, apply *apply) {
	if raft.IsEmptySnap(apply.snapshot) {
		return
	}

	lg := s.Logger()
	lg.Info("开始应用快照",
		zap.Uint64("current-snapshot-index", ep.snapi),
		zap.Uint64("current-applied-index", ep.appliedi),
		zap.Uint64("incoming-leader-snapshot-index", apply.snapshot.Metadata.Index),
		zap.Uint64("incoming-leader-snapshot-term", apply.snapshot.Metadata.Term),
	)
	defer func() {
		lg.Info("已应用快照",
			zap.Uint64("current-snapshot-index", ep.snapi),
			zap.Uint64("current-applied-index", ep.appliedi),
			zap.Uint64("incoming-leader-snapshot-index", apply.snapshot.Metadata.Index),
			zap.Uint64("incoming-leader-snapshot-term", apply.snapshot.Metadata.Term),
		)
	}()

	if apply.snapshot.Metadata.Index <= ep.appliedi {
		lg.Panic("意外得到 来自过时索引的领导者快照",
			zap.Uint64("current-snapshot-index", ep.snapi),
			zap.Uint64("current-applied-index", ep.appliedi),
			zap.Uint64("incoming-leader-snapshot-index", apply.snapshot.Metadata.Index),
			zap.Uint64("incoming-leader-snapshot-term", apply.snapshot.Metadata.Term),
		)
	}

	// 等待raftnode持久化快找到硬盘上
	<-apply.notifyc

	newbe, err := openSnapshotBackend(s.Cfg, s.snapshotter, apply.snapshot, s.beHooks)
	if err != nil {
		lg.Panic("failed to open snapshot backend", zap.Error(err))
	}

	// always recover lessor before kv. When we recover the mvcc.KV it will reattach keys to its leases.
	// If we recover mvcc.KV first, it will attach the keys to the wrong lessor before it recovers.
	if s.lessor != nil {
		lg.Info("restoring lease store")

		s.lessor.Recover(newbe, func() lease.TxnDelete { return s.kv.Write(traceutil.TODO()) })

		lg.Info("restored lease store")
	}

	lg.Info("restoring mvcc store")

	if err := s.kv.Restore(newbe); err != nil {
		lg.Panic("failed to restore mvcc store", zap.Error(err))
	}

	s.consistIndex.SetBackend(newbe)
	lg.Info("restored mvcc store", zap.Uint64("consistent-index", s.consistIndex.ConsistentIndex()))

	// Closing old backend might block until all the txns
	// on the backend are finished.
	// We do not want to wait on closing the old backend.
	s.backendLock.Lock()
	oldbe := s.backend
	go func() {
		lg.Info("closing old backend file")
		defer func() {
			lg.Info("closed old backend file")
		}()
		if err := oldbe.Close(); err != nil {
			lg.Panic("failed to close old backend", zap.Error(err))
		}
	}()

	s.backend = newbe
	s.backendLock.Unlock()

	lg.Info("restoring alarm store")

	if err := s.restoreAlarms(); err != nil {
		lg.Panic("failed to restore alarm store", zap.Error(err))
	}

	lg.Info("restored alarm store")

	if s.authStore != nil {
		lg.Info("restoring auth store")

		s.authStore.Recover(newbe)

		lg.Info("restored auth store")
	}

	lg.Info("restoring v2 store")
	if err := s.v2store.Recovery(apply.snapshot.Data); err != nil {
		lg.Panic("failed to restore v2 store", zap.Error(err))
	}

	if err := assertNoV2StoreContent(lg, s.v2store, s.Cfg.V2Deprecation); err != nil {
		lg.Panic("illegal v2store content", zap.Error(err))
	}

	lg.Info("restored v2 store")

	s.cluster.SetBackend(newbe)

	lg.Info("restoring cluster configuration")

	s.cluster.Recover(api.UpdateCapability)

	lg.Info("restored cluster configuration")
	lg.Info("removing old peers from network")

	// recover raft transport
	s.r.transport.RemoveAllPeers()

	lg.Info("removed old peers from network")
	lg.Info("adding peers from new cluster configuration")

	for _, m := range s.cluster.Members() {
		if m.ID == s.ID() {
			continue
		}
		s.r.transport.AddPeer(m.ID, m.PeerURLs)
	}

	lg.Info("added peers from new cluster configuration")

	ep.appliedt = apply.snapshot.Metadata.Term
	ep.appliedi = apply.snapshot.Metadata.Index
	ep.snapi = ep.appliedi
	ep.confState = apply.snapshot.Metadata.ConfState
}

func (s *EtcdServer) applyEntries(ep *etcdProgress, apply *apply) {
	if len(apply.entries) == 0 {
		return
	}
	firsti := apply.entries[0].Index
	if firsti > ep.appliedi+1 {
		lg := s.Logger()
		lg.Panic("意外的 已提交索引",
			zap.Uint64("current-applied-index", ep.appliedi),
			zap.Uint64("first-committed-entry-index", firsti),
		)
	}
	var ents []raftpb.Entry
	if ep.appliedi+1-firsti < uint64(len(apply.entries)) {
		ents = apply.entries[ep.appliedi+1-firsti:]
	}
	if len(ents) == 0 {
		return
	}
	var shouldstop bool
	if ep.appliedt, ep.appliedi, shouldstop = s.apply(ents, &ep.confState); shouldstop {
		go s.stopWithDelay(10*100*time.Millisecond, fmt.Errorf(""))
	}
}

func (s *EtcdServer) triggerSnapshot(ep *etcdProgress) {
	if ep.appliedi-ep.snapi <= s.Cfg.SnapshotCount { // 触发一次磁盘快照的提交事务的次数
		return
	}

	lg := s.Logger()
	lg.Info("触发打快照",
		zap.String("local-member-id", s.ID().String()),
		zap.Uint64("local-member-applied-index", ep.appliedi),
		zap.Uint64("local-member-snapshot-index", ep.snapi),
		zap.Uint64("local-member-snapshot-count", s.Cfg.SnapshotCount),
	)

	s.snapshot(ep.appliedi, ep.confState)
	ep.snapi = ep.appliedi
}

func (s *EtcdServer) hasMultipleVotingMembers() bool {
	return s.cluster != nil && len(s.cluster.VotingMemberIDs()) > 1
}

func (s *EtcdServer) isLeader() bool {
	return uint64(s.ID()) == s.Lead()
}

// MoveLeader leader转移
func (s *EtcdServer) MoveLeader(ctx context.Context, lead, transferee uint64) error {
	if !s.cluster.IsMemberExist(types.ID(transferee)) || s.cluster.Member(types.ID(transferee)).IsLearner {
		return ErrBadLeaderTransferee
	}

	now := time.Now()
	interval := time.Duration(s.Cfg.TickMs) * time.Millisecond

	lg := s.Logger()
	lg.Info(
		"开始leader转移",
		zap.String("local-member-id", s.ID().String()),
		zap.String("current-leader-member-id", types.ID(lead).String()),
		zap.String("transferee-member-id", types.ID(transferee).String()),
	)

	s.r.TransferLeadership(ctx, lead, transferee) // 开始leader转移
	for s.Lead() != transferee {
		select {
		case <-ctx.Done(): // time out
			return ErrTimeoutLeaderTransfer
		case <-time.After(interval):
		}
	}

	// 耗尽所有请求,或者驱逐掉所有就leader的消息
	lg.Info(
		"leader转移完成",
		zap.String("local-member-id", s.ID().String()),
		zap.String("old-leader-member-id", types.ID(lead).String()),
		zap.String("new-leader-member-id", types.ID(transferee).String()),
		zap.Duration("took", time.Since(now)),
	)
	return nil
}

func (s *EtcdServer) TransferLeadership() error {
	lg := s.Logger()
	if !s.isLeader() {
		lg.Info(
			"skipped leadership transfer; local etcd is not leader",
			zap.String("local-member-id", s.ID().String()),
			zap.String("current-leader-member-id", types.ID(s.Lead()).String()),
		)
		return nil
	}

	if !s.hasMultipleVotingMembers() {
		lg.Info(
			"skipped leadership transfer for single voting member cluster",
			zap.String("local-member-id", s.ID().String()),
			zap.String("current-leader-member-id", types.ID(s.Lead()).String()),
		)
		return nil
	}

	transferee, ok := longestConnected(s.r.transport, s.cluster.VotingMemberIDs())
	if !ok {
		return ErrUnhealthy
	}

	tm := s.Cfg.ReqTimeout()
	ctx, cancel := context.WithTimeout(s.ctx, tm)
	err := s.MoveLeader(ctx, s.Lead(), uint64(transferee))
	cancel()
	return err
}

// HardStop 在不与集群中其他成员协调的情况下停止etcd.
func (s *EtcdServer) HardStop() {
	select {
	case s.stop <- struct{}{}:
	case <-s.done:
		return
	}
	<-s.done
}

// Stop 优雅停止本节点, 如果是leader要等leader转移
func (s *EtcdServer) Stop() {
	lg := s.Logger()
	if err := s.TransferLeadership(); err != nil {
		lg.Warn("leader转移失败", zap.String("local-member-id", s.ID().String()), zap.Error(err))
	}
	s.HardStop()
}

// ReadyNotify  当etcd 准备好服务请求后,会关闭ready ch
func (s *EtcdServer) ReadyNotify() <-chan struct{} { return s.readych }

func (s *EtcdServer) stopWithDelay(d time.Duration, err error) {
	select {
	case <-time.After(d):
	case <-s.done:
	}
	select {
	case s.errorc <- err:
	default:
	}
}

// StopNotify 当etcd停止时、会往此channel发送 empty struct
// when the etcd is stopped.
func (s *EtcdServer) StopNotify() <-chan struct{} { return s.done }

// StoppingNotify returns a channel that receives a empty struct
// when the etcd is being stopped.
func (s *EtcdServer) StoppingNotify() <-chan struct{} { return s.stopping }

func (s *EtcdServer) SelfStats() []byte { return s.stats.JSON() }

func (s *EtcdServer) LeaderStats() []byte {
	lead := s.getLead()
	if lead != uint64(s.id) {
		return nil
	}
	return s.lstats.JSON()
}

func (s *EtcdServer) StoreStats() []byte { return s.v2store.JsonStats() }

// 检查节点操作的权限
func (s *EtcdServer) checkMembershipOperationPermission(ctx context.Context) error {
	_ = auth.NewAuthStore
	if s.authStore == nil {
		// 在普通的etcd进程中,s.authStore永远不会为零.这个分支是为了处理server_test.go中的情况
		return nil
	}

	// 请注意,这个权限检查是在API层完成的,所以TOCTOU问题可能会在这样的时间表中引起:
	// 更新用户A的会员资格------撤销A的根角色------在状态机层应用会员资格的改变
	// 然而,会员资格的改变和角色管理都需要根权限.所以管理员的谨慎操作可以防止这个问题.
	authInfo, err := s.AuthInfoFromCtx(ctx)
	if err != nil {
		return err
	}

	return s.AuthStore().IsAdminPermitted(authInfo)
}

// 检查learner是否追上了leader
// 注意:如果在集群中没有找到成员,或者成员不是学习者,它将返回nil.
// 这两个条件将在后面的应用阶段之前进行后台检查.
func (s *EtcdServer) isLearnerReady(id uint64) error {
	rs := s.raftStatus()
	if rs.Progress == nil {
		return ErrNotLeader
	}

	var learnerMatch uint64
	isFound := false
	leaderID := rs.ID
	for memberID, progress := range rs.Progress {
		if id == memberID {
			learnerMatch = progress.Match
			isFound = true
			break
		}
	}

	if isFound {
		leaderMatch := rs.Progress[leaderID].Match
		// learner的进度还没有赶上领导者
		if float64(learnerMatch) < float64(leaderMatch)*readyPercent {
			return ErrLearnerNotReady
		}
	}
	return nil
}

func (s *EtcdServer) mayRemoveMember(id types.ID) error {
	if !s.Cfg.StrictReconfigCheck { // 严格配置变更检查
		return nil
	}

	lg := s.Logger()
	isLearner := s.cluster.IsMemberExist(id) && s.cluster.Member(id).IsLearner
	// no need to check quorum when removing non-voting member
	if isLearner {
		return nil
	}

	if !s.cluster.IsReadyToRemoveVotingMember(uint64(id)) {
		lg.Warn(
			"rejecting member remove request; not enough healthy members",
			zap.String("local-member-id", s.ID().String()),
			zap.String("requested-member-remove-id", id.String()),
			zap.Error(ErrNotEnoughStartedMembers),
		)
		return ErrNotEnoughStartedMembers
	}

	// downed member is safe to remove since it's not part of the active quorum
	if t := s.r.transport.ActiveSince(id); id != s.ID() && t.IsZero() {
		return nil
	}

	// protect quorum if some members are down
	m := s.cluster.VotingMembers()
	active := numConnectedSince(s.r.transport, time.Now().Add(-HealthInterval), s.ID(), m)
	if (active - 1) < 1+((len(m)-1)/2) {
		lg.Warn(
			"rejecting member remove request; local member has not been connected to all peers, reconfigure breaks active quorum",
			zap.String("local-member-id", s.ID().String()),
			zap.String("requested-member-remove", id.String()),
			zap.Int("active-peers", active),
			zap.Error(ErrUnhealthy),
		)
		return ErrUnhealthy
	}

	return nil
}

// FirstCommitInTermNotify
// 任期内第一次commit会往这个channel发个信号,这是新leader回答只读请求所必需的
// Leader不能响应任何只读请求,只要线性语义是必需的
func (s *EtcdServer) FirstCommitInTermNotify() <-chan struct{} {
	s.firstCommitInTermMu.RLock()
	defer s.firstCommitInTermMu.RUnlock()
	return s.firstCommitInTermC
}

type confChangeResponse struct {
	membs []*membership.Member
	err   error
}

// configureAndSendRaft 通过raft发送配置变更,然后等待后端应用到etcd.它将阻塞,直到更改执行或出现错误.
func (s *EtcdServer) configureAndSendRaft(ctx context.Context, cc raftpb.ConfChangeV1) ([]*membership.Member, error) {
	lg := s.Logger()
	cc.ID = s.reqIDGen.Next()
	ch := s.w.Register(cc.ID)

	start := time.Now()
	if err := s.r.ProposeConfChange(ctx, cc); err != nil {
		s.w.Trigger(cc.ID, nil)
		return nil, err
	}

	select {
	case x := <-ch:
		if x == nil {
			lg.Panic("配置失败")
		}
		resp := x.(*confChangeResponse)
		lg.Info(
			"通过raft应用配置更改",
			zap.String("local-member-id", s.ID().String()),
			zap.String("raft-conf-change", cc.Type.String()),
			zap.String("raft-conf-change-node-id", types.ID(cc.NodeID).String()),
		)
		return resp.membs, resp.err

	case <-ctx.Done():
		s.w.Trigger(cc.ID, nil) // GC wait
		return nil, s.parseProposeCtxErr(ctx.Err(), start)

	case <-s.stopping:
		return nil, ErrStopped
	}
}

// sync proposes a SYNC request and is non-blocking.
// This makes no guarantee that the request will backend proposed or performed.
// The request will backend canceled after the given timeout.
func (s *EtcdServer) sync(timeout time.Duration) {
	req := pb.Request{
		Method: "SYNC",
		ID:     s.reqIDGen.Next(),
		Time:   time.Now().UnixNano(),
	}
	data := pbutil.MustMarshal(&req)
	// There is no promise that node has leader when do SYNC request,
	// so it uses goroutine to propose.
	ctx, cancel := context.WithTimeout(s.ctx, timeout)
	s.GoAttach(func() {
		s.r.Propose(ctx, data)
		cancel()
	})
}

// publish registers etcd information into the cluster. The information
// is the JSON representation of this etcd's member struct, updated with the
// static clientURLs of the etcd.
// The function keeps attempting to register until it succeeds,
// or its etcd is stopped.
//
// Use v2 store to encode member attributes, and apply through Raft
// but does not go through v2 API endpoint, which means even with v2
// client handler disabled (e.g. --enable-v2=false), cluster can still
// process publish requests through rafthttp
// TODO: Remove in 3.6 (start using publishV3)
func (s *EtcdServer) publish(timeout time.Duration) {
	lg := s.Logger()
	b, err := json.Marshal(s.attributes)
	if err != nil {
		lg.Panic("failed to marshal JSON", zap.Error(err))
		return
	}
	req := pb.Request{
		Method: "PUT",
		Path:   membership.MemberAttributesStorePath(s.id),
		Val:    string(b),
	}

	for {
		ctx, cancel := context.WithTimeout(s.ctx, timeout)
		_, err := s.Do(ctx, req)
		cancel()
		switch err {
		case nil:
			close(s.readych)
			lg.Info(
				"published local member to cluster through raft",
				zap.String("local-member-id", s.ID().String()),
				zap.String("local-member-attributes", fmt.Sprintf("%+v", s.attributes)),
				zap.String("request-path", req.Path),
				zap.String("cluster-id", s.cluster.ID().String()),
				zap.Duration("publish-timeout", timeout),
			)
			return

		case ErrStopped:
			lg.Warn(
				"stopped publish because etcd is stopped",
				zap.String("local-member-id", s.ID().String()),
				zap.String("local-member-attributes", fmt.Sprintf("%+v", s.attributes)),
				zap.Duration("publish-timeout", timeout),
				zap.Error(err),
			)
			return

		default:
			lg.Warn(
				"通过raft发布本机信息失败",
				zap.String("local-member-id", s.ID().String()),
				zap.String("local-member-attributes", fmt.Sprintf("%+v", s.attributes)),
				zap.String("request-path", req.Path),
				zap.Duration("publish-timeout", timeout),
				zap.Error(err),
			)
		}
	}
}

func (s *EtcdServer) sendMergedSnap(merged snap.Message) {
	atomic.AddInt64(&s.inflightSnapshots, 1)

	lg := s.Logger()
	fields := []zap.Field{
		zap.String("from", s.ID().String()),
		zap.String("to", types.ID(merged.To).String()),
		zap.Int64("bytes", merged.TotalSize),
		zap.String("size", humanize.Bytes(uint64(merged.TotalSize))),
	}

	now := time.Now()
	s.r.transport.SendSnapshot(merged)
	lg.Info("sending merged snapshot", fields...)

	s.GoAttach(func() {
		select {
		case ok := <-merged.CloseNotify():
			// delay releasing inflight snapshot for another 30 seconds to
			// block log compaction.
			// If the follower still fails to catch up, it is probably just too slow
			// to catch up. We cannot avoid the snapshot cycle anyway.
			if ok {
				select {
				case <-time.After(releaseDelayAfterSnapshot):
				case <-s.stopping:
				}
			}

			atomic.AddInt64(&s.inflightSnapshots, -1)

			lg.Info("sent merged snapshot", append(fields, zap.Duration("took", time.Since(now)))...)

		case <-s.stopping:
			lg.Warn("canceled sending merged snapshot; etcd stopping", fields...)
			return
		}
	})
}

// apply 从raft 获取到committed ---> applying
func (s *EtcdServer) apply(es []raftpb.Entry, confState *raftpb.ConfState) (appliedt uint64, appliedi uint64, shouldStop bool) {
	// confState 当前快照中的 集群配置
	s.lg.Debug("开始应用日志", zap.Int("num-entries", len(es)))
	for i := range es {
		e := es[i]
		s.lg.Debug("开始应用日志", zap.Uint64("index", e.Index), zap.Uint64("term", e.Term), zap.Stringer("type", e.Type))
		switch e.Type {
		case raftpb.EntryNormal:
			s.applyEntryNormal(&e)
			s.setAppliedIndex(e.Index)
			s.setTerm(e.Term)

		case raftpb.EntryConfChange:
			shouldApplyV3 := membership.ApplyV2storeOnly    // false
			if e.Index > s.consistIndex.ConsistentIndex() { // 查找 bolt.db meta 库里的 	consistent_index、term
				s.consistIndex.SetConsistentIndex(e.Index, e.Term) // 更新内存里的
				shouldApplyV3 = membership.ApplyBoth               // true
			}

			var cc raftpb.ConfChangeV1
			_ = cc.Unmarshal
			pbutil.MustUnmarshal(&cc, e.Data)
			// ConfChangeAddNode {"id":10276657743932975437,"peerURLs":["http://localhost:2380"],"name":"default"}
			removedSelf, err := s.applyConfChange(cc, confState, shouldApplyV3)
			s.setAppliedIndex(e.Index)
			s.setTerm(e.Term)
			shouldStop = shouldStop || removedSelf
			s.w.Trigger(cc.ID, &confChangeResponse{s.cluster.Members(), err})

		default:
			lg := s.Logger()
			lg.Panic(
				"未知的日志类型;必须是 EntryNormal 或 EntryConfChange",
				zap.String("type", e.Type.String()),
			)
		}
		appliedi, appliedt = e.Index, e.Term
	}
	return appliedt, appliedi, shouldStop
}

// TODO: non-blocking snapshot
func (s *EtcdServer) snapshot(snapi uint64, confState raftpb.ConfState) {
	clone := s.v2store.Clone()
	// commit kv to write metadata (for example: consistent index) to disk.
	//
	// This guarantees that Backend's consistent_index is >= index of last snapshot.
	//
	// KV().commit() updates the consistent index in backend.
	// All operations that update consistent index必须是called sequentially
	// from applyAll function.
	// So KV().Commit() cannot run in parallel with apply. It has to backend called outside
	// the go routine created below.
	s.KV().Commit()

	s.GoAttach(func() {
		lg := s.Logger()

		d, err := clone.SaveNoCopy()
		// TODO: current store will never fail to do a snapshot
		// what should we do if the store might fail?
		if err != nil {
			lg.Panic("failed to save v2 store", zap.Error(err))
		}
		snap, err := s.r.raftStorage.CreateSnapshot(snapi, &confState, d)
		if err != nil {
			// the snapshot was done asynchronously with the progress of raft.
			// raft might have already got a newer snapshot.
			if err == raft.ErrSnapOutOfDate {
				return
			}
			lg.Panic("failed to create snapshot", zap.Error(err))
		}
		// SaveSnap saves the snapshot to file and appends the corresponding WAL entry.
		if err = s.r.storage.SaveSnap(snap); err != nil {
			lg.Panic("failed to save snapshot", zap.Error(err))
		}
		if err = s.r.storage.Release(snap); err != nil {
			lg.Panic("failed to release wal", zap.Error(err))
		}

		lg.Info(
			"saved snapshot",
			zap.Uint64("snapshot-index", snap.Metadata.Index),
		)

		// When sending a snapshot, etcd will pause compaction.
		// After receives a snapshot, the slow follower needs to get all the entries right after
		// the snapshot sent to catch up. If we do not pause compaction, the log entries right after
		// the snapshot sent might already backend compacted. It happens when the snapshot takes long time
		// to send and save. Pausing compaction avoids triggering a snapshot sending cycle.
		if atomic.LoadInt64(&s.inflightSnapshots) != 0 {
			lg.Info("skip compaction since there is an inflight snapshot")
			return
		}

		// keep some in memory log entries for slow followers.
		compacti := uint64(1)
		if snapi > s.Cfg.SnapshotCatchUpEntries {
			compacti = snapi - s.Cfg.SnapshotCatchUpEntries // 保留一定数量的日志,为了follower可以追赶
		}

		err = s.r.raftStorage.Compact(compacti)
		if err != nil {
			// the compaction was done asynchronously with the progress of raft.
			// raft log might already been compact.
			if err == raft.ErrCompacted {
				return
			}
			lg.Panic("failed to compact", zap.Error(err))
		}
		lg.Info(
			"compacted Raft logs",
			zap.Uint64("compact-index", compacti),
		)
	})
}

// CutPeer drops messages to the specified peer.
func (s *EtcdServer) CutPeer(id types.ID) {
	tr, ok := s.r.transport.(*rafthttp.Transport)
	if ok {
		tr.CutPeer(id)
	}
}

// MendPeer recovers the message dropping behavior of the given peer.
func (s *EtcdServer) MendPeer(id types.ID) {
	tr, ok := s.r.transport.(*rafthttp.Transport)
	if ok {
		tr.MendPeer(id)
	}
}

func (s *EtcdServer) PauseSending() { s.r.pauseSending() }

func (s *EtcdServer) ResumeSending() { s.r.resumeSending() }

// monitorVersions checks the member's version every monitorVersionInterval.
// It updates the cluster version if all members agrees on a higher one.
// It prints out log if there is a member with a higher version than the
// local version.
func (s *EtcdServer) monitorVersions() {
	for {
		select {
		case <-s.FirstCommitInTermNotify():
		case <-time.After(monitorVersionInterval):
		case <-s.stopping:
			return
		}

		if s.Leader() != s.ID() {
			continue
		}

		v := decideClusterVersion(s.Logger(), getVersions(s.Logger(), s.cluster, s.id, s.peerRt))
		if v != nil {
			// only keep major.minor version for comparison
			v = &semver.Version{
				Major: v.Major,
				Minor: v.Minor,
			}
		}

		// if the current version is nil:
		// 1. use the decided version if possible
		// 2. or use the min cluster version
		if s.cluster.Version() == nil {
			verStr := version.MinClusterVersion
			if v != nil {
				verStr = v.String()
			}
			s.GoAttach(func() { s.updateClusterVersionV2(verStr) })
			continue
		}

		if v != nil && membership.IsValidVersionChange(s.cluster.Version(), v) {
			s.GoAttach(func() { s.updateClusterVersionV2(v.String()) })
		}
	}
}

func (s *EtcdServer) updateClusterVersionV2(ver string) {
	lg := s.Logger()
	if s.cluster.Version() == nil {
		lg.Info("使用v2 API 设置初始集群版本", zap.String("cluster-version", version.Cluster(ver)))
	} else {
		lg.Info("使用v2 API 更新初始集群版本", zap.String("from", version.Cluster(s.cluster.Version().String())), zap.String("to", version.Cluster(ver)))
	}

	req := pb.Request{
		Method: "PUT",
		Path:   membership.StoreClusterVersionKey(), // /0/version
		Val:    ver,
	}

	ctx, cancel := context.WithTimeout(s.ctx, s.Cfg.ReqTimeout())
	fmt.Println("start", time.Now())
	_, err := s.Do(ctx, req)
	fmt.Println("end", time.Now())
	cancel()

	switch err {
	case nil:
		lg.Info("集群版本已更新", zap.String("cluster-version", version.Cluster(ver)))
		return

	case ErrStopped:
		lg.Warn("终止集群版本更新;etcd被停止了", zap.Error(err))
		return

	default:
		lg.Warn("集群版本更新失败", zap.Error(err))
	}
}

func (s *EtcdServer) monitorDowngrade() {
	t := s.Cfg.DowngradeCheckTime
	if t == 0 {
		return
	}
	lg := s.Logger()
	for {
		select {
		case <-time.After(t):
		case <-s.stopping:
			return
		}

		if !s.isLeader() {
			continue
		}

		d := s.cluster.DowngradeInfo()
		if !d.Enabled {
			continue
		}

		targetVersion := d.TargetVersion
		v := semver.Must(semver.NewVersion(targetVersion))
		if isMatchedVersions(s.Logger(), v, getVersions(s.Logger(), s.cluster, s.id, s.peerRt)) {
			lg.Info("the cluster has been downgraded", zap.String("cluster-version", targetVersion))
			ctx, cancel := context.WithTimeout(context.Background(), s.Cfg.ReqTimeout())
			if _, err := s.downgradeCancel(ctx); err != nil {
				lg.Warn("failed to cancel downgrade", zap.Error(err))
			}
			cancel()
		}
	}
}

func (s *EtcdServer) parseProposeCtxErr(err error, start time.Time) error {
	switch err {
	case context.Canceled:
		return ErrCanceled

	case context.DeadlineExceeded:
		s.leadTimeMu.RLock()
		curLeadElected := s.leadElectedTime
		s.leadTimeMu.RUnlock()
		prevLeadLost := curLeadElected.Add(-2 * time.Duration(s.Cfg.ElectionTicks) * time.Duration(s.Cfg.TickMs) * time.Millisecond)
		if start.After(prevLeadLost) && start.Before(curLeadElected) {
			return ErrTimeoutDueToLeaderFail
		}
		lead := types.ID(s.getLead())
		switch lead {
		case types.ID(raft.None):
			// 当前没有leader
		case s.ID(): // leader是自己
			if !isConnectedToQuorumSince(s.r.transport, start, s.ID(), s.cluster.Members()) { // 检查是否与大多数节点建立连接
				return ErrTimeoutDueToConnectionLost
			}
		default:
			// 检查是否自给定时间以后,与该节点建立连接
			if !isConnectedSince(s.r.transport, start, lead) {
				return ErrTimeoutDueToConnectionLost
			}
		}
		return ErrTimeout

	default:
		return err
	}
}

func (s *EtcdServer) Backend() backend.Backend {
	s.backendLock.Lock()
	defer s.backendLock.Unlock()
	return s.backend
}

func (s *EtcdServer) AuthStore() auth.AuthStore { return s.authStore }

// 启动时重置所有警报
func (s *EtcdServer) restoreAlarms() error {
	s.applyV3 = s.newApplierV3()
	as, err := v3alarm.NewAlarmStore(s.lg, s)
	if err != nil {
		return err
	}
	s.alarmStore = as
	// 警报只有这两种类型
	if len(as.Get(pb.AlarmType_NOSPACE)) > 0 {
		s.applyV3 = newApplierV3Capped(s.applyV3)
	}
	if len(as.Get(pb.AlarmType_CORRUPT)) > 0 {
		s.applyV3 = newApplierV3Corrupt(s.applyV3)
	}
	return nil
}

// ----------------------------------------- OVER  --------------------------------------------------------------

// GoAttach 启动一个协程干活
func (s *EtcdServer) GoAttach(f func()) {
	s.wgMu.RLock() // stopping 关闭 是加锁操作
	defer s.wgMu.RUnlock()
	select {
	case <-s.stopping:
		lg := s.Logger()
		lg.Warn("etcd 已停止; 跳过 GoAttach")
		return
	default:
	}

	// 现在可以安全添加因为等待组的等待还没有开始.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		f()
	}()
}

// applyEntryNormal 将日志应用到raft内部
func (s *EtcdServer) applyEntryNormal(e *raftpb.Entry) {
	shouldApplyV3 := membership.ApplyV2storeOnly
	index := s.consistIndex.ConsistentIndex()
	if e.Index > index {
		// 设置当前entry的一致性索引
		s.consistIndex.SetConsistentIndex(e.Index, e.Term)
		shouldApplyV3 = membership.ApplyBoth // v2store 、bolt.db 都存储数据
	}
	s.lg.Debug("应用日志", zap.Uint64("consistent-index", index),
		zap.Uint64("entry-index", e.Index),
		zap.Bool("should-applyV3", bool(shouldApplyV3)))

	// 当leader确认时raft状态机可能会产生noop条目. 提前跳过它以避免将来出现一些潜在的错误.
	if len(e.Data) == 0 {
		s.notifyAboutFirstCommitInTerm() // 被任期内 第一次commit更新channel
		// 当本地成员是leader 并完成了上一任期的所有条目时促进follower.
		if s.isLeader() {
			// 成为leader时,初始化租约管理器
			s.lessor.Promote(s.Cfg.ElectionTimeout())
		}
		return
	}
	// e.Data 是由 pb.InternalRaftRequest、 序列化得到的
	var raftReq pb.InternalRaftRequest
	if pbutil.MaybeUnmarshal(&raftReq, e.Data) {
	} else {
		// 如果不能不能反序列化
		// {"ID":7587861231285799684,"Method":"PUT","Path":"/0/version","Val":"3.5.0","Dir":false,"PrevValue":"","PrevIndex":0,"Expiration":0,"Wait":false,"Since":0,"Recursive":false,"Sorted":false,"Quorum":false,"Time":0,"Stream":false}
		var r pb.Request
		rp := &r
		pbutil.MustUnmarshal(rp, e.Data)
		s.w.Trigger(r.ID, s.applyV2Request((*RequestV2)(rp), shouldApplyV3))
		fmt.Println("pbutil.MustUnmarshal return")
		return
	}
	// 如果能
	//{"header":{"ID":7587861231285799685},"put":{"key":"YQ==","value":"Yg=="}}
	if raftReq.V2 != nil {
		req := (*RequestV2)(raftReq.V2)
		s.w.Trigger(req.ID, s.applyV2Request(req, shouldApplyV3))
		return
	}

	id := raftReq.ID
	if id == 0 {
		id = raftReq.Header.ID
	}

	var ar *applyResult
	needResult := s.w.IsRegistered(id)
	if needResult || !noSideEffect(&raftReq) {
		if !needResult && raftReq.Txn != nil {
			removeNeedlessRangeReqs(raftReq.Txn)
		}
		ar = s.applyV3.Apply(&raftReq, shouldApplyV3)
	}

	if !shouldApplyV3 { //  是否存储到bolt.db
		return
	}

	if ar == nil {
		return
	}

	if ar.err != ErrNoSpace || len(s.alarmStore.Get(pb.AlarmType_NOSPACE)) > 0 {
		s.w.Trigger(id, ar)
		return
	}

	lg := s.Logger()
	lg.Warn("消息超过了后端配额；发出警报", zap.Int64("quota-size-bytes", s.Cfg.QuotaBackendBytes),
		zap.String("quota-size", humanize.Bytes(uint64(s.Cfg.QuotaBackendBytes))),
		zap.Error(ar.err),
	)

	s.GoAttach(func() {
		a := &pb.AlarmRequest{
			MemberID: uint64(s.ID()),
			Action:   pb.AlarmRequest_ACTIVATE, // 日志应用时, 激活警报
			Alarm:    pb.AlarmType_NOSPACE,
		}
		s.raftRequest(s.ctx, pb.InternalRaftRequest{Alarm: a})
		s.w.Trigger(id, ar)
	})
}

// 通知关于任期内的第一次commit
func (s *EtcdServer) notifyAboutFirstCommitInTerm() {
	newNotifier := make(chan struct{})
	s.firstCommitInTermMu.Lock()
	notifierToClose := s.firstCommitInTermC
	// 同于响应只读请求的
	s.firstCommitInTermC = newNotifier
	s.firstCommitInTermMu.Unlock()
	close(notifierToClose)
}

// IsLearner 当前节点是不是 raft learner
func (s *EtcdServer) IsLearner() bool {
	return s.cluster.IsLocalMemberLearner()
}

// IsMemberExist returns if the member with the given id exists in cluster.
func (s *EtcdServer) IsMemberExist(id types.ID) bool {
	return s.cluster.IsMemberExist(id)
}

// raftStatus 返回当前节点的raft状态
func (s *EtcdServer) raftStatus() raft.Status {
	return s.r.RaftNodeInterFace.Status()
}

// 碎片整理
func maybeDefragBackend(cfg config.ServerConfig, be backend.Backend) error {
	size := be.Size()
	sizeInUse := be.SizeInUse()
	freeableMemory := uint(size - sizeInUse) // 剩余
	thresholdBytes := cfg.ExperimentalBootstrapDefragThresholdMegabytes * 1024 * 1024
	if freeableMemory < thresholdBytes {
		cfg.Logger.Info("跳过碎片整理",
			zap.Int64("current-db-size-bytes", size),
			zap.String("current-db-size", humanize.Bytes(uint64(size))),
			zap.Int64("current-db-size-in-use-bytes", sizeInUse),
			zap.String("current-db-size-in-use", humanize.Bytes(uint64(sizeInUse))),
			zap.Uint("experimental-bootstrap-defrag-threshold-bytes", thresholdBytes),
			zap.String("experimental-bootstrap-defrag-threshold", humanize.Bytes(uint64(thresholdBytes))),
		)
		return nil
	}
	return be.Defrag()
}

// applyConfChange 将一个confChange作用到当前raft,它必须已经committed
func (s *EtcdServer) applyConfChange(cc raftpb.ConfChangeV1, confState *raftpb.ConfState, shouldApplyV3 membership.ShouldApplyV3) (bool, error) {
	if err := s.cluster.ValidateConfigurationChange(cc); err != nil {
		cc.NodeID = raft.None // 这种,不会处理的
		s.r.ApplyConfChange(cc)
		return false, err
	}

	lg := s.Logger()
	*confState = *s.r.ApplyConfChange(cc) // 生效之后的配置
	s.beHooks.SetConfState(confState)
	switch cc.Type {
	// 集群里记录的quorum.JointConfig与peer信息已经更新
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		confChangeContext := new(membership.ConfigChangeContext)
		if err := json.Unmarshal([]byte(cc.Context), confChangeContext); err != nil {
			lg.Panic("发序列化成员失败", zap.Error(err))
		}
		if cc.NodeID != uint64(confChangeContext.Member.ID) {
			lg.Panic("得到不同的成员ID",
				zap.String("member-id-from-config-change-entry", types.ID(cc.NodeID).String()),
				zap.String("member-id-from-message", confChangeContext.Member.ID.String()),
			)
		}
		if confChangeContext.IsPromote { // 是否角色提升
			s.cluster.PromoteMember(confChangeContext.Member.ID, shouldApplyV3)
		} else {
			s.cluster.AddMember(&confChangeContext.Member, shouldApplyV3) // 添加节点  /0/members/8e9e05c52164694d
			if confChangeContext.Member.ID != s.id {                      // 不是本实例
				s.r.transport.AddPeer(confChangeContext.Member.ID, confChangeContext.PeerURLs)
			}
		}

	case raftpb.ConfChangeRemoveNode:
		id := types.ID(cc.NodeID)
		s.cluster.RemoveMember(id, shouldApplyV3) // ✅
		if id == s.id {
			return true, nil
		}
		s.r.transport.RemovePeer(id)

	case raftpb.ConfChangeUpdateNode:
		m := new(membership.Member)
		if err := json.Unmarshal([]byte(cc.Context), m); err != nil {
			lg.Panic("反序列化失败", zap.Error(err))
		}
		if cc.NodeID != uint64(m.ID) {
			lg.Panic("得到了一个不同的ID",
				zap.String("member-id-from-config-change-entry", types.ID(cc.NodeID).String()),
				zap.String("member-id-from-message", m.ID.String()),
			)
		}
		s.cluster.UpdateRaftAttributes(m.ID, m.RaftAttributes, shouldApplyV3)
		if m.ID != s.id {
			s.r.transport.UpdatePeer(m.ID, m.PeerURLs)
		}
	}
	return false, nil
}

// Alarms 获取所有的警报,
func (s *EtcdServer) Alarms() []*pb.AlarmMember {
	return s.alarmStore.Get(pb.AlarmType_NONE)
}

func (s *EtcdServer) setCommittedIndex(v uint64) {
	atomic.StoreUint64(&s.committedIndex, v)
}

func (s *EtcdServer) getCommittedIndex() uint64 {
	return atomic.LoadUint64(&s.committedIndex)
}

func (s *EtcdServer) setAppliedIndex(v uint64) {
	atomic.StoreUint64(&s.appliedIndex, v)
}

func (s *EtcdServer) getAppliedIndex() uint64 {
	return atomic.LoadUint64(&s.appliedIndex)
}

func (s *EtcdServer) setTerm(v uint64) {
	atomic.StoreUint64(&s.term, v)
}

func (s *EtcdServer) getTerm() uint64 {
	return atomic.LoadUint64(&s.term)
}

func (s *EtcdServer) setLead(v uint64) {
	atomic.StoreUint64(&s.lead, v)
}

func (s *EtcdServer) getLead() uint64 {
	return atomic.LoadUint64(&s.lead)
}

func (s *EtcdServer) IsIDRemoved(id uint64) bool { return s.cluster.IsIDRemoved(types.ID(id)) }

func (s *EtcdServer) Cluster() api.Cluster { return s.cluster }

func (s *EtcdServer) LeaderChangedNotify() <-chan struct{} {
	s.leaderChangedMu.RLock()
	defer s.leaderChangedMu.RUnlock()
	return s.leaderChanged
}

func (s *EtcdServer) KV() mvcc.WatchableKV { return s.kv }

// Process 接收一个raft信息并将其应用于etcd的raft状态机,使用ctx的超时.
func (s *EtcdServer) Process(ctx context.Context, m raftpb.Message) error {
	lg := s.Logger()
	// 判断该消息的来源有没有被删除
	if s.cluster.IsIDRemoved(types.ID(m.From)) {
		lg.Warn("拒绝来自被删除的成员的raft的信息",
			zap.String("local-member-id", s.ID().String()),
			zap.String("removed-member-id", types.ID(m.From).String()),
		)
		return httptypes.NewHTTPError(http.StatusForbidden, "无法处理来自被删除成员的信息")
	}
	// 操作日志【复制、配置变更 req】
	if m.Type == raftpb.MsgApp {
		s.stats.RecvAppendReq(types.ID(m.From).String(), m.Size())
	}
	var _ raft.RaftNodeInterFace = raftNode{}
	//_ = raftNode{}.Step
	return s.r.Step(ctx, m)
}

func (s *EtcdServer) ClusterVersion() *semver.Version {
	if s.cluster == nil {
		return nil
	}
	return s.cluster.Version()
}
