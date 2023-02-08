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

package lease

import (
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/ls-2018/etcd_cn/etcd/lease/leasepb"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/buckets"
	pb "github.com/ls-2018/etcd_cn/offical/etcdserverpb"
	"go.uber.org/zap"
)

const (
	NoLease     = LeaseID(0) // 是一个特殊的LeaseID,表示没有租约.
	MaxLeaseTTL = 9000000000
)

var v3_6 = semver.Version{Major: 3, Minor: 6}

var (
	forever                          = time.Time{}
	leaseRevokeRate                  = 1000            // 每秒撤销租约的最大数量；可为测试配置
	leaseCheckpointRate              = 1000            // 每秒记录在共识日志中的最大租约快照数量；可对测试进行配置
	defaultLeaseCheckpointInterval   = 5 * time.Minute // 租约快照的默认时间间隔
	maxLeaseCheckpointBatchSize      = 1000            // 租约快照的最大数量,以批处理为一个单一的共识日志条目
	defaultExpiredleaseRetryInterval = 3 * time.Second // 检查过期租约是否被撤销的默认时间间隔.
	ErrNotPrimary                    = errors.New("不是主 lessor")
	ErrLeaseNotFound                 = errors.New("lease没有发现")
	ErrLeaseExists                   = errors.New("lease已存在")
	ErrLeaseTTLTooLarge              = errors.New("过大的TTL")
)

type TxnDelete interface {
	DeleteRange(key, end []byte) (n, rev int64)
	End()
}

type RangeDeleter func() TxnDelete

// Checkpointer 允许对租约剩余ttl的检查点到 wal日志.这里定义是为了避免与mvcc的循环依赖.
type Checkpointer func(ctx context.Context, lc *pb.LeaseCheckpointRequest)

type LeaseID int64

// Lessor 创建、移除、更新租约
type Lessor interface {
	// SetRangeDeleter lets the lessor create TxnDeletes to the store.
	// Lessor deletes the items in the revoked or expired lease by creating
	// new TxnDeletes.
	SetRangeDeleter(rd RangeDeleter)
	SetCheckpointer(cp Checkpointer)
	Grant(id LeaseID, ttl int64) (*Lease, error)     // 创建一个制定了过期时间的租约
	Revoke(id LeaseID) error                         // 移除租约
	Checkpoint(id LeaseID, remainingTTL int64) error // 更新租约的剩余时间到其他节点
	Attach(id LeaseID, items []LeaseItem) error      //
	GetLease(item LeaseItem) LeaseID                 // 返回给定项目的LeaseID.如果没有找到租约,则返回NoLease值.
	Detach(id LeaseID, items []LeaseItem) error      // 将租约从key上移除
	Promote(extend time.Duration)                    // 推动lessor成为主lessor.主lessor管理租约的到期和续期.新晋升的lessor更新所有租约的ttl 以延长先前的ttl
	Demote()                                         // leader变更,触发
	Renew(id LeaseID) (int64, error)                 // 重新计算过期时间
	Lookup(id LeaseID) *Lease
	Leases() []*Lease                // 获取当前节点上的所有租约
	ExpiredLeasesC() <-chan []*Lease // 返回一个用于接收过期租约的CHAN.
	Recover(b backend.Backend, rd RangeDeleter)
	Stop()
}

type lessor struct {
	mu                   sync.RWMutex
	demotec              chan struct{}         // 当lessor成为主时,会被设置.当被降级时,会被关闭
	leaseMap             map[LeaseID]*Lease    // 存储了所有的租约
	leaseExpiredNotifier *ExpiredNotifier      // 租约到期管理
	leaseCheckpointHeap  LeaseQueue            // 记录检查点,租约ID
	itemMap              map[LeaseItem]LeaseID // key 关联到了哪个租约
	rd                   RangeDeleter          // 租约过期时,使用范围删除
	cp                   Checkpointer          // 当一个租约的最后期限应该被持久化,以保持跨领袖选举和重启的剩余TTL,出租人将通过Checkpointer对租约进行检查.
	b                    backend.Backend       // 持久化租约到bolt.db.
	minLeaseTTL          int64                 // 是可授予租约的最小租期TTL.任何缩短TTL的请求都被扩展到最小TTL.
	expiredC             chan []*Lease         // 发送一批已经过期的租约
	// stopC is a channel whose closure indicates that the lessor should be stopped.
	stopC                     chan struct{}
	doneC                     chan struct{} // close表明lessor被停止.
	lg                        *zap.Logger
	checkpointInterval        time.Duration // 租约快照的默认时间间隔
	expiredLeaseRetryInterval time.Duration // 检查过期租约是否被撤销的默认时间间隔
	checkpointPersist         bool          // lessor是否应始终保持剩余的TTL（在v3.6中始终启用）.
	cluster                   cluster       // 基于集群版本  调整lessor逻辑
}
type Lease struct {
	ID           LeaseID                // 租约ID ,   自增得到的,
	ttl          int64                  // 租约的生存时间,以秒为单位
	remainingTTL int64                  // 剩余生存时间,以秒为单位,如果为零,则视为未设置,应使用完整的tl.
	expiryMu     sync.RWMutex           // 保护并发的访问
	expiry       time.Time              // 是租约到期的时间.当expiry.IsZero()为真时,永久存在.
	mu           sync.RWMutex           // 保护并发的访问 itemSet
	itemSet      map[LeaseItem]struct{} // 哪些租约附加到了key
	revokec      chan struct{}          // 租约被删除、到期 关闭此channel,触发后续逻辑
}

type cluster interface {
	Version() *semver.Version // 是整个集群的最小major.minor版本.
}

type LessorConfig struct {
	MinLeaseTTL                int64         // 是可授予租约的最小租期TTL.任何缩短TTL的请求都被扩展到最小TTL.
	CheckpointInterval         time.Duration // 租约快照的默认时间间隔
	ExpiredLeasesRetryInterval time.Duration // 租约快照的默认时间间隔
	CheckpointPersist          bool          // lessor是否应始终保持剩余的TTL（在v3.6中始终启用）.
}

func NewLessor(lg *zap.Logger, b backend.Backend, cluster cluster, cfg LessorConfig) Lessor {
	return newLessor(lg, b, cluster, cfg)
}

func (le *lessor) shouldPersistCheckpoints() bool {
	cv := le.cluster.Version()
	return le.checkpointPersist || (cv != nil && greaterOrEqual(*cv, v3_6))
}

func greaterOrEqual(first, second semver.Version) bool {
	return !first.LessThan(second)
}

// Promote 当节点成为leader时
func (le *lessor) Promote(extend time.Duration) {
	// extend 是选举超时
	le.mu.Lock()
	defer le.mu.Unlock()

	le.demotec = make(chan struct{})

	// 刷新所有租约的过期时间
	for _, l := range le.leaseMap {
		l.refresh(extend)
		item := &LeaseWithTime{id: l.ID, time: l.expiry}
		le.leaseExpiredNotifier.RegisterOrUpdate(item) // 开始监听租约过期
		le.scheduleCheckpointIfNeeded(l)
	}

	if len(le.leaseMap) < leaseRevokeRate {
		// 没有租约堆积的可能性
		return
	}

	// 如果有重叠,请调整过期时间
	leases := le.unsafeLeases()
	sort.Sort(leasesByExpiry(leases))

	baseWindow := leases[0].Remaining() // 剩余存活时间
	nextWindow := baseWindow + time.Second
	expires := 0 // 到期
	// 失效期限少于总失效率,所以堆积的租约不会消耗整个失效限制
	targetExpiresPerSecond := (3 * leaseRevokeRate) / 4
	for _, l := range leases {
		remaining := l.Remaining()
		if remaining > nextWindow {
			baseWindow = remaining
			nextWindow = baseWindow + time.Second
			expires = 1
			continue
		}
		expires++
		if expires <= targetExpiresPerSecond {
			continue
		}
		rateDelay := float64(time.Second) * (float64(expires) / float64(targetExpiresPerSecond))
		// 如果租期延长n秒,则比基本窗口早n秒的租期只应延长1秒.
		rateDelay -= float64(remaining - baseWindow)
		delay := time.Duration(rateDelay)
		nextWindow = baseWindow + delay
		l.refresh(delay + extend)
		item := &LeaseWithTime{id: l.ID, time: l.expiry}
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
		le.scheduleCheckpointIfNeeded(l)
	}
}

type leasesByExpiry []*Lease

func (le leasesByExpiry) Len() int           { return len(le) }
func (le leasesByExpiry) Less(i, j int) bool { return le[i].Remaining() < le[j].Remaining() }
func (le leasesByExpiry) Swap(i, j int)      { le[i], le[j] = le[j], le[i] }

func (le *lessor) GetLease(item LeaseItem) LeaseID {
	le.mu.RLock()
	id := le.itemMap[item] // 找不到就是永久
	le.mu.RUnlock()
	return id
}

func (le *lessor) Recover(b backend.Backend, rd RangeDeleter) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.b = b
	le.rd = rd
	le.leaseMap = make(map[LeaseID]*Lease)
	le.itemMap = make(map[LeaseItem]LeaseID)
	le.initAndRecover()
}

func (le *lessor) Stop() {
	close(le.stopC)
	<-le.doneC
}

// --------------------------------------------- OVER  -----------------------------------------------------------------

// FakeLessor is a fake implementation of Lessor interface.
// Used for testing only.
type FakeLessor struct{}

func (fl *FakeLessor) SetRangeDeleter(dr RangeDeleter) {}

func (fl *FakeLessor) SetCheckpointer(cp Checkpointer) {}

func (fl *FakeLessor) Grant(id LeaseID, ttl int64) (*Lease, error) { return nil, nil }

func (fl *FakeLessor) Revoke(id LeaseID) error { return nil }

func (fl *FakeLessor) Checkpoint(id LeaseID, remainingTTL int64) error { return nil }

func (fl *FakeLessor) Attach(id LeaseID, items []LeaseItem) error { return nil }

func (fl *FakeLessor) GetLease(item LeaseItem) LeaseID { return 0 }

func (fl *FakeLessor) Detach(id LeaseID, items []LeaseItem) error { return nil }

func (fl *FakeLessor) Promote(extend time.Duration) {}

func (fl *FakeLessor) Demote() {}

func (fl *FakeLessor) Renew(id LeaseID) (int64, error) { return 10, nil }

func (fl *FakeLessor) Lookup(id LeaseID) *Lease { return nil }

func (fl *FakeLessor) Leases() []*Lease { return nil }

func (fl *FakeLessor) ExpiredLeasesC() <-chan []*Lease { return nil }

func (fl *FakeLessor) Recover(b backend.Backend, rd RangeDeleter) {}

func (fl *FakeLessor) Stop() {}

type FakeTxnDelete struct {
	backend.BatchTx
}

func (ftd *FakeTxnDelete) DeleteRange(key, end []byte) (n, rev int64) { return 0, 0 }
func (ftd *FakeTxnDelete) End()                                       { ftd.Unlock() }

// --------------------------------------------- OVER  -----------------------------------------------------------------

// Grant 创建租约
func (le *lessor) Grant(id LeaseID, ttl int64) (*Lease, error) {
	if id == NoLease {
		return nil, ErrLeaseNotFound
	}

	if ttl > MaxLeaseTTL {
		return nil, ErrLeaseTTLTooLarge
	}

	// lessor在高负荷时,应延长租期,以减少续租.
	l := &Lease{
		ID:      id,
		ttl:     ttl,
		itemSet: make(map[LeaseItem]struct{}),
		revokec: make(chan struct{}), // 租约被删除、到期 关闭此channel,触发后续逻辑
	}

	le.mu.Lock()
	defer le.mu.Unlock()

	if _, ok := le.leaseMap[id]; ok {
		return nil, ErrLeaseExists
	}

	if l.ttl < le.minLeaseTTL {
		l.ttl = le.minLeaseTTL
	}

	if le.isPrimary() { // 是否还是主lessor
		l.refresh(0) // 刷新租约的过期时间
	} else {
		l.forever()
	}

	le.leaseMap[id] = l
	l.persistTo(le.b)

	if le.isPrimary() {
		item := &LeaseWithTime{id: l.ID, time: l.expiry}
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
		le.scheduleCheckpointIfNeeded(l)
	}

	return l, nil
}

// expireExists 返回是否有已过期的租约, next 表明它可能在下次尝试中存在.
func (le *lessor) expireExists() (l *Lease, ok bool, next bool) {
	if le.leaseExpiredNotifier.Len() == 0 {
		return nil, false, false
	}

	item := le.leaseExpiredNotifier.Poll() // 获取第一个,不会从堆中剔除
	l = le.leaseMap[item.id]
	if l == nil {
		// 租约已过期或 已经被移除,不需要再次移除
		le.leaseExpiredNotifier.Unregister() // O(log N)  弹出第一个
		return nil, false, true
	}
	now := time.Now()
	if now.Before(item.time) {
		// 判断时间有没有过期
		return l, false, false
	}

	// recheck if revoke is complete after retry interval
	item.time = now.Add(le.expiredLeaseRetryInterval)
	le.leaseExpiredNotifier.RegisterOrUpdate(item)
	return l, true, false
}

// findExpiredLeases 在leaseExpiredNotifier中的小顶堆中删除过期的lease,有数量限制
func (le *lessor) findExpiredLeases(limit int) []*Lease {
	leases := make([]*Lease, 0, 16)

	for {
		l, ok, next := le.expireExists() // 获取一个已过期的 租约,以及之后是否可能仍然存在
		if !ok && !next {
			// 当前没有,以后不存在
			break
		}
		if !ok {
			// 当前没有
			continue
		}
		if next {
			// 以后存在
			continue
		}
		//
		if l.expired() {
			leases = append(leases, l)
			if len(leases) == limit {
				break
			}
		}
	}

	return leases
}

// 查找所有过期的租约,并将其发送到过期的通道中等待撤销.
func (le *lessor) revokeExpiredLeases() {
	var ls []*Lease

	// 每秒撤销租约的最大数量,  500ms调用一次,那么限制应该改为 /2
	revokeLimit := leaseRevokeRate / 2

	le.mu.RLock()
	if le.isPrimary() { // 主
		// 在leaseExpiredNotifier中的小顶堆中删除过期的lease,有数量限制
		ls = le.findExpiredLeases(revokeLimit)
	}
	le.mu.RUnlock()

	if len(ls) != 0 {
		select {
		case <-le.stopC:
			return
		case le.expiredC <- ls:
		default:
			// expiredC的接收器可能正在忙着处理其他东西,下次500ms后再试
		}
	}
}

// ExpiredLeasesC 返回一批已过期的租约
func (le *lessor) ExpiredLeasesC() <-chan []*Lease {
	return le.expiredC
}

// Revoke 从kvindex以及bolt.db中删除
func (le *lessor) Revoke(id LeaseID) error {
	le.mu.Lock()

	l := le.leaseMap[id]
	if l == nil {
		le.mu.Unlock()
		return ErrLeaseNotFound
	}
	defer close(l.revokec)
	le.mu.Unlock()
	// mvcc.newWatchableStore
	if le.rd == nil {
		return nil
	}

	txn := le.rd()

	// 对键进行排序,以便在所有成员中以相同的顺序删除,否则后台的哈希值将是不同的.
	keys := l.Keys() // 返回当前组约绑定到了哪些key
	sort.StringSlice(keys).Sort()
	for _, key := range keys { // 该租约附加到了哪些key上
		fmt.Printf("租约:%d到期  删除key:%s  \n", id, key)
		txn.DeleteRange([]byte(key), nil) // 从内存 kvindex 中 删除
	}

	le.mu.Lock()
	defer le.mu.Unlock()
	delete(le.leaseMap, l.ID)
	// 租约的删除需要与kv的删除在同一个后台事务中.否则,如果 etcdserver 在两者之间发生故障,我们可能会出现不执行撤销或不删除钥匙的结果.
	le.b.BatchTx().UnsafeDelete(buckets.Lease, int64ToBytes(int64(l.ID))) // 删除bolt.db 里的key
	txn.End()
	return nil
}

// Remaining 返回剩余时间
func (l *Lease) Remaining() time.Duration {
	l.expiryMu.RLock()
	defer l.expiryMu.RUnlock()
	if l.expiry.IsZero() {
		return time.Duration(math.MaxInt64)
	}
	return time.Until(l.expiry)
}

type LeaseItem struct {
	Key string
}

func int64ToBytes(n int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(n))
	return bytes
}

// 是否已过期
func (l *Lease) expired() bool {
	return l.Remaining() <= 0
}

// 持久化租约
func (l *Lease) persistTo(b backend.Backend) {
	key := int64ToBytes(int64(l.ID))

	lpb := leasepb.Lease{ID: int64(l.ID), TTL: l.ttl, RemainingTTL: l.remainingTTL}
	val, err := lpb.Marshal()
	if err != nil {
		panic("序列化lease消息失败")
	}

	b.BatchTx().Lock()
	b.BatchTx().UnsafePut(buckets.Lease, key, val)
	b.BatchTx().Unlock()
}

func (l *Lease) TTL() int64 {
	return l.ttl
}

// Keys 返回当前组约绑定到了哪些key
func (l *Lease) Keys() []string {
	l.mu.RLock()
	keys := make([]string, 0, len(l.itemSet))
	for k := range l.itemSet {
		keys = append(keys, k.Key)
	}
	l.mu.RUnlock()
	return keys
}

// getRemainingTTL returns the last checkpointed remaining TTL of the lease.
func (l *Lease) getRemainingTTL() int64 {
	if l.remainingTTL > 0 {
		return l.remainingTTL
	}
	return l.ttl
}

// 创建租约管理器
func newLessor(lg *zap.Logger, b backend.Backend, cluster cluster, cfg LessorConfig) *lessor {
	checkpointInterval := cfg.CheckpointInterval
	expiredLeaseRetryInterval := cfg.ExpiredLeasesRetryInterval
	if checkpointInterval == 0 {
		checkpointInterval = defaultLeaseCheckpointInterval
	}
	if expiredLeaseRetryInterval == 0 {
		expiredLeaseRetryInterval = defaultExpiredleaseRetryInterval
	}
	l := &lessor{
		leaseMap:                  make(map[LeaseID]*Lease),
		itemMap:                   make(map[LeaseItem]LeaseID),
		leaseExpiredNotifier:      newLeaseExpiredNotifier(), // 租约到期移除的队列
		leaseCheckpointHeap:       make(LeaseQueue, 0),
		b:                         b,                         // bolt.db
		minLeaseTTL:               cfg.MinLeaseTTL,           // 是可授予租约的最小租期TTL.任何缩短TTL的请求都被扩展到最小TTL.
		checkpointInterval:        checkpointInterval,        // 租约快照的默认时间间隔
		expiredLeaseRetryInterval: expiredLeaseRetryInterval, // 检查过期租约是否被撤销的默认时间间隔
		checkpointPersist:         cfg.CheckpointPersist,     //  lessor是否应始终保持剩余的TTL（在v3.6中始终启用）.
		expiredC:                  make(chan []*Lease, 16),   // 避免不必要的阻塞
		stopC:                     make(chan struct{}),
		doneC:                     make(chan struct{}),
		lg:                        lg,
		cluster:                   cluster,
	}
	l.initAndRecover() // 从bolt.db恢复租约信息

	go l.runLoop() // 开始检查是否有快过期的租约

	return l
}

// isPrimary 表示该出租人是否为主要出租人.主出租人负责管理租约的到期和更新.
// 在etcd中,raft leader是主要的.因此,在同一时间可能有两个主要的领导者（raft允许同时存在的领导者,但任期不同）,最多是一个领导者选举超时.
// 旧的主要领导者不能影响正确性,因为它的提议有一个较小的期限,不会被提交.
// TODO:raft的跟随者不转发租约管理提案.可能会有一个非常小的窗口（通常在一秒钟之内,这取决于调度）,在raft领导者降级和lessor降级之间
// 通常情况下,这不应该是一个问题.租约对时间不应该那么敏感.
func (le *lessor) isPrimary() bool {
	return le.demotec != nil
}

// SetRangeDeleter 主要是设置一个用于获取delete的写事务的函数
func (le *lessor) SetRangeDeleter(rd RangeDeleter) {
	le.mu.Lock()
	defer le.mu.Unlock()
	le.rd = rd
}

// Renew 在租约有效期内,重新计算过期时间
func (le *lessor) Renew(id LeaseID) (int64, error) {
	le.mu.RLock()
	if !le.isPrimary() {
		le.mu.RUnlock()
		return -1, ErrNotPrimary
	}

	demotec := le.demotec

	l := le.leaseMap[id]
	if l == nil {
		le.mu.RUnlock()
		return -1, ErrLeaseNotFound
	}
	// 清空剩余时间
	clearRemainingTTL := le.cp != nil && l.remainingTTL > 0

	le.mu.RUnlock()
	if l.expired() { // 租约过期了
		select {
		case <-l.revokec: // 当租约到期,会关闭
			return -1, ErrLeaseNotFound
		case <-demotec:
			return -1, ErrNotPrimary
		case <-le.stopC:
			return -1, ErrNotPrimary
		}
	}

	// Clear remaining TTL when we renew if it is set
	// By applying a RAFT entry only when the remainingTTL is already set, we limit the number
	// of RAFT entries written per lease to a max of 2 per checkpoint interval.
	if clearRemainingTTL {
		// 定期批量地将 Lease 剩余的 TTL 基于 Raft Log 同步给 Follower 节点,Follower 节点收到 CheckPoint 请求后,
		// 更新内存数据结构 LeaseMap 的剩余 TTL 信息.
		le.cp(context.Background(), &pb.LeaseCheckpointRequest{Checkpoints: []*pb.LeaseCheckpoint{{ID: int64(l.ID), RemainingTtl: 0}}})
	}

	le.mu.Lock()
	l.refresh(0)
	item := &LeaseWithTime{id: l.ID, time: l.expiry}
	le.leaseExpiredNotifier.RegisterOrUpdate(item)
	le.mu.Unlock()

	return l.ttl, nil
}

// Lookup 查找租约
func (le *lessor) Lookup(id LeaseID) *Lease {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.leaseMap[id]
}

// Leases 获取当前节点上的所有租约
func (le *lessor) Leases() []*Lease {
	le.mu.RLock()
	ls := le.unsafeLeases()
	le.mu.RUnlock()
	sort.Sort(leasesByExpiry(ls))
	return ls
}

// Attach 将一些key附加到租约上
func (le *lessor) Attach(id LeaseID, items []LeaseItem) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	l := le.leaseMap[id]
	if l == nil {
		return ErrLeaseNotFound
	}

	l.mu.Lock()
	for _, it := range items {
		l.itemSet[it] = struct{}{}
		le.itemMap[it] = id
	}
	l.mu.Unlock()
	return nil
}

// Detach 将一些key从租约上移除
func (le *lessor) Detach(id LeaseID, items []LeaseItem) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	l := le.leaseMap[id]
	if l == nil {
		return ErrLeaseNotFound
	}

	l.mu.Lock()
	for _, it := range items {
		delete(l.itemSet, it)
		delete(le.itemMap, it)
	}
	l.mu.Unlock()
	return nil
}

// refresh 刷新租约的过期时间
func (l *Lease) refresh(extend time.Duration) {
	newExpiry := time.Now().Add(extend + time.Duration(l.getRemainingTTL())*time.Second)
	l.expiryMu.Lock()
	defer l.expiryMu.Unlock()
	l.expiry = newExpiry
}

// Demote leader不是自己时,会触发
func (le *lessor) Demote() {
	le.mu.Lock()
	defer le.mu.Unlock()

	//  将所有租约的有效期设置为永久
	for _, l := range le.leaseMap {
		l.forever() // 内存中
	}
	// 清空租约 检查点信息
	le.clearScheduledLeasesCheckpoints()
	// 重置 租约到期通知器
	le.clearLeaseExpiredNotifier()

	if le.demotec != nil {
		close(le.demotec)
		le.demotec = nil
	}
}

// forever 设置永久过期时间,当不是主lessor
func (l *Lease) forever() {
	l.expiryMu.Lock()
	defer l.expiryMu.Unlock()
	l.expiry = forever
}

// 返回所有租约
func (le *lessor) unsafeLeases() []*Lease {
	leases := make([]*Lease, 0, len(le.leaseMap))
	for _, l := range le.leaseMap {
		leases = append(leases, l)
	}
	return leases
}

// 清空租约 检查点信息
func (le *lessor) clearScheduledLeasesCheckpoints() {
	le.leaseCheckpointHeap = make(LeaseQueue, 0)
}

// OK
func (le *lessor) clearLeaseExpiredNotifier() {
	le.leaseExpiredNotifier = newLeaseExpiredNotifier()
}

// 从bolt.db恢复租约信息
func (le *lessor) initAndRecover() {
	tx := le.b.BatchTx()
	tx.Lock()

	tx.UnsafeCreateBucket(buckets.Lease)
	_, vs := tx.UnsafeRange(buckets.Lease, int64ToBytes(0), int64ToBytes(math.MaxInt64), 0)
	for i := range vs {
		var lpb leasepb.Lease
		err := lpb.Unmarshal(vs[i])
		if err != nil {
			tx.Unlock()
			panic("反序列化lease 消息失败")
		}
		ID := LeaseID(lpb.ID)
		if lpb.TTL < le.minLeaseTTL {
			lpb.TTL = le.minLeaseTTL
		}
		le.leaseMap[ID] = &Lease{
			ID:  ID,
			ttl: lpb.TTL,
			// itemSet将在恢复键值对将过期时间设置为永久 ,提升时刷新
			itemSet:      make(map[LeaseItem]struct{}),
			expiry:       forever,
			revokec:      make(chan struct{}),
			remainingTTL: lpb.RemainingTTL,
		}
	}
	le.leaseExpiredNotifier.Init() // 填充mq.m
	heap.Init(&le.leaseCheckpointHeap)
	tx.Unlock()

	le.b.ForceCommit()
}

func (le *lessor) SetCheckpointer(cp Checkpointer) {
	le.mu.Lock()
	defer le.mu.Unlock()
	le.cp = cp
}

// OK
func (le *lessor) runLoop() {
	defer close(le.doneC)
	for {
		// 查找所有过期的租约,并将其发送到过期的通道中等待撤销.
		le.revokeExpiredLeases()
		// 查找所有到期的预定租约检查点将它们提交给检查点以将它们持久化到共识日志中.
		le.checkpointScheduledLeases() // 定时触发更新 Lease 的剩余到期时间的操作.

		select {
		case <-time.After(500 * time.Millisecond):
		case <-le.stopC:
			return
		}
	}
}

// 查找所有到期的预定租约检查点将它们提交给检查点以将它们持久化到共识日志中.
func (le *lessor) checkpointScheduledLeases() {
	var cps []*pb.LeaseCheckpoint

	// rate limit
	for i := 0; i < leaseCheckpointRate/2; i++ {
		le.mu.Lock()
		if le.isPrimary() {
			cps = le.findDueScheduledCheckpoints(maxLeaseCheckpointBatchSize)
		}
		le.mu.Unlock()

		if len(cps) != 0 {
			// 定期批量地将 Lease 剩余的 TTL 基于 Raft Log 同步给 Follower 节点,Follower 节点收到 CheckPoint 请求后,
			// 更新内存数据结构 LeaseMap 的剩余 TTL 信息.
			// 	srv.raftRequestOnce(ctx, pb.InternalRaftRequest{LeaseCheckpoint: cp})
			// case r.LeaseCheckpoint != nil:
			le.cp(context.Background(), &pb.LeaseCheckpointRequest{Checkpoints: cps})
		}
		if len(cps) < maxLeaseCheckpointBatchSize {
			return
		}
	}
}

// 开始执行检查 ,leader 变更时,防止ttl重置
// 租约创建时、成为leader后、收到checkpoint 共识消息后
func (le *lessor) scheduleCheckpointIfNeeded(lease *Lease) {
	if le.cp == nil {
		return
	}
	// 剩余存活时间,大于 checkpointInterval
	le.checkpointInterval = time.Second * 20
	if lease.getRemainingTTL() > int64(le.checkpointInterval.Seconds()) {
		if le.lg != nil {
			le.lg.Info("开始调度 租约 检查", zap.Int64("leaseID", int64(lease.ID)), zap.Duration("intervalSeconds", le.checkpointInterval))
		}
		heap.Push(&le.leaseCheckpointHeap, &LeaseWithTime{
			id:   lease.ID,
			time: time.Now().Add(le.checkpointInterval), // 300 秒后租约到期, 检查这个租约
		})
		le.lg.Info("租约", zap.Int("checkpoint", len(le.leaseCheckpointHeap)), zap.Int("lease", len(le.leaseMap)))
	}
}

// 查找到期的检查点
func (le *lessor) findDueScheduledCheckpoints(checkpointLimit int) []*pb.LeaseCheckpoint {
	if le.cp == nil {
		return nil
	}

	now := time.Now()
	var cps []*pb.LeaseCheckpoint
	for le.leaseCheckpointHeap.Len() > 0 && len(cps) < checkpointLimit {
		lt := le.leaseCheckpointHeap[0]
		if lt.time.After(now) { // 过了 检查点的时间
			return cps
		}
		heap.Pop(&le.leaseCheckpointHeap)
		var l *Lease
		var ok bool
		if l, ok = le.leaseMap[lt.id]; !ok {
			continue
		}
		if !now.Before(l.expiry) {
			continue
		}
		remainingTTL := int64(math.Ceil(l.expiry.Sub(now).Seconds())) // 剩余时间
		if remainingTTL >= l.ttl {
			continue
		}
		if le.lg != nil {
			le.lg.Debug("检查租约ing", zap.Int64("leaseID", int64(lt.id)), zap.Int64("remainingTTL", remainingTTL))
		}
		cps = append(cps, &pb.LeaseCheckpoint{ID: int64(lt.id), RemainingTtl: remainingTTL})
	}
	return cps
}

// Checkpoint 更新租约的剩余时间
func (le *lessor) Checkpoint(id LeaseID, remainingTTL int64) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	if l, ok := le.leaseMap[id]; ok {
		// 当检查点时,我们只更新剩余的TTL,Promote 负责将其应用于租赁到期.
		l.remainingTTL = remainingTTL
		if le.shouldPersistCheckpoints() { // true
			l.persistTo(le.b)
		}
		if le.isPrimary() {
			// 根据需要,安排下一个检查点
			le.scheduleCheckpointIfNeeded(l)
		}
	}
	return nil
}
