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

package mvcc

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sync"

	"github.com/ls-2018/etcd_cn/etcd/lease"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/buckets"
	"github.com/ls-2018/etcd_cn/offical/api/v3/mvccpb"
	"github.com/ls-2018/etcd_cn/pkg/schedule"
	"github.com/ls-2018/etcd_cn/pkg/traceutil"

	"go.uber.org/zap"
)

var (
	scheduledCompactKeyName = []byte("scheduledCompactRev")
	finishedCompactKeyName  = []byte("finishedCompactRev")

	ErrCompacted = errors.New("mvcc: 指定的修订版本已被压缩")
	ErrFutureRev = errors.New("mvcc: 指定的修订版本还没有")
)

const (
	// markedRevBytesLen is the byte length of marked revision.
	// The first `revBytesLen` bytes represents a normal revision. The last
	// one byte is the mark.
	markedRevBytesLen      = revBytesLen + 1
	markBytePosition       = markedRevBytesLen - 1
	markTombstone     byte = 't'
)

var (
	restoreChunkKeys         = 10000 // non-const for testing
	defaultCompactBatchLimit = 1000
)

type StoreConfig struct {
	CompactionBatchLimit int
}

type store struct {
	ReadView
	WriteView
	cfg StoreConfig
	// mu read locks for txns and write locks for non-txn store changes.
	mu             sync.RWMutex
	b              backend.Backend
	kvindex        index
	le             lease.Lessor // 租约管理器
	revMu          sync.RWMutex // 保护currentRev和compactMainRev
	currentRev     int64        // 是最后一个已完成事务的修订
	compactMainRev int64
	fifoSched      schedule.Scheduler
	stopc          chan struct{}
	lg             *zap.Logger
}

// NewStore returns a new store. It is useful to create a store inside
// mvcc pkg. It should only be used for testing externally.
func NewStore(lg *zap.Logger, b backend.Backend, le lease.Lessor, cfg StoreConfig) *store {
	if lg == nil {
		lg = zap.NewNop()
	}
	if cfg.CompactionBatchLimit == 0 {
		cfg.CompactionBatchLimit = defaultCompactBatchLimit
	}
	s := &store{
		cfg:     cfg,
		b:       b,
		kvindex: newTreeIndex(lg),

		le: le,

		currentRev:     1,
		compactMainRev: -1,

		fifoSched: schedule.NewFIFOScheduler(),

		stopc: make(chan struct{}),

		lg: lg,
	}
	s.ReadView = &readView{s}
	s.WriteView = &writeView{s}
	if s.le != nil {
		s.le.SetRangeDeleter(func() lease.TxnDelete { return s.Write(traceutil.TODO()) })
	}

	tx := s.b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(buckets.Key)
	tx.UnsafeCreateBucket(buckets.Meta)
	tx.Unlock()
	s.b.ForceCommit()

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.restore(); err != nil {
		// TODO: return the error instead of panic here?
		panic("failed to recover store from backend")
	}

	return s
}

// 返回读事务、  并发读,串行读
func (s *store) Read(mode ReadTxMode, trace *traceutil.Trace) TxnRead {
	s.mu.RLock()
	s.revMu.RLock()
	// 对于只读的工作负载,我们通过复制事务读缓冲区来使用共享缓冲区提高并发性
	// 对于写/写/读事务,我们使用共享缓冲区
	// 而不是复制事务读缓冲区,以避免事务开销.
	var tx backend.ReadTx
	if mode == ConcurrentReadTxMode {
		tx = s.b.ConcurrentReadTx()
	} else {
		tx = s.b.ReadTx()
	}

	tx.RLock()
	firstRev, rev := s.compactMainRev, s.currentRev
	s.revMu.RUnlock()
	return &storeTxnRead{s, tx, firstRev, rev, trace}
}

func (s *store) Write(trace *traceutil.Trace) TxnWrite {
	s.mu.RLock()
	tx := s.b.BatchTx()
	tx.Lock()
	tw := &storeTxnWrite{
		storeTxnRead: storeTxnRead{s, tx, 0, 0, trace},
		tx:           tx,
		beginRev:     s.currentRev,
		changes:      make([]mvccpb.KeyValue, 0, 4),
	}
	return tw
}

func (s *store) compactBarrier(ctx context.Context, ch chan struct{}) {
	if ctx == nil || ctx.Err() != nil {
		select {
		case <-s.stopc:
		default:
			// fix deadlock in mvcc,for more information, please refer to pr 11817.
			// s.stopc is only updated in restore operation, which is called by apply
			// snapshot call, compaction and apply snapshot requests are serialized by
			// raft, and do not happen at the same time.
			s.mu.Lock()
			f := func(ctx context.Context) { s.compactBarrier(ctx, ch) }
			s.fifoSched.Schedule(f)
			s.mu.Unlock()
		}
		return
	}
	close(ch)
}

// Hash OK
func (s *store) Hash() (hash uint32, revision int64, err error) {
	// TODO: hash和revision可能不一致,一个可能的解决方案是在函数的开头添加s.revMu.RLock(),这是昂贵的
	s.b.ForceCommit()
	h, err := s.b.Hash(buckets.DefaultIgnores)

	return h, s.currentRev, err
}

// HashByRev 计算所有MVCC修订到给定修订的哈希值.
func (s *store) HashByRev(rev int64) (hash uint32, currentRev int64, compactRev int64, err error) {
	s.mu.RLock()
	s.revMu.RLock()
	compactRev, currentRev = s.compactMainRev, s.currentRev
	s.revMu.RUnlock()

	if rev > 0 && rev <= compactRev {
		s.mu.RUnlock()
		return 0, 0, compactRev, ErrCompacted
	} else if rev > 0 && rev > currentRev {
		s.mu.RUnlock()
		return 0, currentRev, 0, ErrFutureRev
	}

	if rev == 0 {
		rev = currentRev
	}
	keep := s.kvindex.Keep(rev)

	tx := s.b.ReadTx()
	tx.RLock()
	defer tx.RUnlock()
	s.mu.RUnlock()

	upper := revision{Main: rev + 1}
	lower := revision{Main: compactRev + 1}
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	h.Write(buckets.Key.Name())
	err = tx.UnsafeForEach(buckets.Key, func(k, v []byte) error {
		kr := bytesToRev(k)
		if !upper.GreaterThan(kr) {
			return nil
		}
		// skip revisions that are scheduled for deletion
		// due to compacting; don't skip if there isn't one.
		if lower.GreaterThan(kr) && len(keep) > 0 {
			if _, ok := keep[kr]; !ok {
				return nil
			}
		}
		h.Write(k)
		h.Write(v)
		return nil
	})
	hash = h.Sum32()

	return hash, currentRev, compactRev, err
}

func (s *store) updateCompactRev(rev int64) (<-chan struct{}, error) {
	s.revMu.Lock()
	if rev <= s.compactMainRev {
		ch := make(chan struct{})
		f := func(ctx context.Context) { s.compactBarrier(ctx, ch) }
		s.fifoSched.Schedule(f)
		s.revMu.Unlock()
		return ch, ErrCompacted
	}
	if rev > s.currentRev {
		s.revMu.Unlock()
		return nil, ErrFutureRev
	}

	s.compactMainRev = rev

	rbytes := newRevBytes()
	revToBytes(revision{Main: rev}, rbytes)

	tx := s.b.BatchTx()
	tx.Lock()
	tx.UnsafePut(buckets.Meta, scheduledCompactKeyName, rbytes)
	tx.Unlock()
	// ensure that desired compaction is persisted
	s.b.ForceCommit()

	s.revMu.Unlock()

	return nil, nil
}

func (s *store) compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error) {
	ch := make(chan struct{})
	j := func(ctx context.Context) {
		if ctx.Err() != nil {
			s.compactBarrier(ctx, ch)
			return
		}
		keep := s.kvindex.Compact(rev)
		if !s.scheduleCompaction(rev, keep) { // 删除bolt.db中旧版本
			s.compactBarrier(context.TODO(), ch)
			return
		}
		close(ch)
	}

	s.fifoSched.Schedule(j)
	trace.Step("schedule compaction")
	return ch, nil
}

func (s *store) compactLockfree(rev int64) (<-chan struct{}, error) {
	ch, err := s.updateCompactRev(rev)
	if err != nil {
		return ch, err
	}

	return s.compact(traceutil.TODO(), rev)
}

func (s *store) Compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error) {
	s.mu.Lock()

	ch, err := s.updateCompactRev(rev)
	trace.Step("check and update compact revision")
	if err != nil {
		s.mu.Unlock()
		return ch, err
	}
	s.mu.Unlock()

	return s.compact(trace, rev) //  Compact
}

func (s *store) Commit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b.ForceCommit()
}

func (s *store) Restore(b backend.Backend) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	close(s.stopc)
	s.fifoSched.Stop()

	s.b = b
	s.kvindex = newTreeIndex(s.lg)

	{
		// During restore the metrics might report 'special' values
		s.revMu.Lock()
		s.currentRev = 1
		s.compactMainRev = -1
		s.revMu.Unlock()
	}

	s.fifoSched = schedule.NewFIFOScheduler()
	s.stopc = make(chan struct{})

	return s.restore()
}

func (s *store) restore() error {
	min, max := newRevBytes(), newRevBytes()
	revToBytes(revision{Main: 1}, min)
	revToBytes(revision{Main: math.MaxInt64, Sub: math.MaxInt64}, max)

	keyToLease := make(map[string]lease.LeaseID)

	// restore index
	tx := s.b.BatchTx()
	tx.Lock()

	_, finishedCompactBytes := tx.UnsafeRange(buckets.Meta, finishedCompactKeyName, nil, 0)
	if len(finishedCompactBytes) != 0 {
		s.revMu.Lock()
		s.compactMainRev = bytesToRev(finishedCompactBytes[0]).Main

		s.lg.Info(
			"restored last compact revision",
			zap.Stringer("meta-bucket-name", buckets.Meta),
			zap.String("meta-bucket-name-Key", string(finishedCompactKeyName)),
			zap.Int64("restored-compact-revision", s.compactMainRev),
		)
		s.revMu.Unlock()
	}
	_, scheduledCompactBytes := tx.UnsafeRange(buckets.Meta, scheduledCompactKeyName, nil, 0)
	scheduledCompact := int64(0)
	if len(scheduledCompactBytes) != 0 {
		scheduledCompact = bytesToRev(scheduledCompactBytes[0]).Main
	}

	// index keys concurrently as they're loaded in from tx
	rkvc, revc := restoreIntoIndex(s.lg, s.kvindex)
	for {
		keys, vals := tx.UnsafeRange(buckets.Key, min, max, int64(restoreChunkKeys))
		if len(keys) == 0 {
			break
		}
		// rkvc blocks if the total pending keys exceeds the restore
		// chunk size to keep keys from consuming too much memory.
		restoreChunk(s.lg, rkvc, keys, vals, keyToLease)
		if len(keys) < restoreChunkKeys {
			// partial set implies final set
			break
		}
		// next set begins after where this one ended
		newMin := bytesToRev(keys[len(keys)-1][:revBytesLen])
		newMin.Sub++
		revToBytes(newMin, min)
	}
	close(rkvc)

	{
		s.revMu.Lock()
		s.currentRev = <-revc

		// keys in the range [compacted revision -N, compaction] might all be deleted due to compaction.
		// the correct revision should be set to compaction revision in the case, not the largest revision
		// we have seen.
		if s.currentRev < s.compactMainRev {
			s.currentRev = s.compactMainRev
		}
		s.revMu.Unlock()
	}

	if scheduledCompact <= s.compactMainRev {
		scheduledCompact = 0
	}

	for key, lid := range keyToLease {
		if s.le == nil {
			tx.Unlock()
			panic("no lessor to attach lease")
		}
		err := s.le.Attach(lid, []lease.LeaseItem{{Key: key}})
		if err != nil {
			s.lg.Error(
				"failed to attach a lease",
				zap.String("lease-id", fmt.Sprintf("%016x", lid)),
				zap.Error(err),
			)
		}
	}

	tx.Unlock()

	s.lg.Info("kvstore restored", zap.Int64("current-rev", s.currentRev))

	if scheduledCompact != 0 {
		if _, err := s.compactLockfree(scheduledCompact); err != nil {
			s.lg.Warn("compaction encountered error", zap.Error(err))
		}

		s.lg.Info(
			"resume scheduled compaction",
			zap.Stringer("meta-bucket-name", buckets.Meta),
			zap.String("meta-bucket-name-Key", string(scheduledCompactKeyName)),
			zap.Int64("scheduled-compact-revision", scheduledCompact),
		)
	}

	return nil
}

type revKeyValue struct {
	key  []byte
	kv   mvccpb.KeyValue
	kstr string
}

func restoreIntoIndex(lg *zap.Logger, idx index) (chan<- revKeyValue, <-chan int64) {
	rkvc, revc := make(chan revKeyValue, restoreChunkKeys), make(chan int64, 1)
	go func() {
		currentRev := int64(1)
		defer func() { revc <- currentRev }()
		// restore the tree index from streaming the unordered index.
		kiCache := make(map[string]*keyIndex, restoreChunkKeys)
		for rkv := range rkvc {
			ki, ok := kiCache[rkv.kstr]
			// purge kiCache if many keys but still missing in the cache
			if !ok && len(kiCache) >= restoreChunkKeys {
				i := 10
				for k := range kiCache {
					delete(kiCache, k)
					if i--; i == 0 {
						break
					}
				}
			}
			// cache miss, fetch from tree index if there
			if !ok {
				ki = &keyIndex{Key: rkv.kv.Key}
				if idxKey := idx.KeyIndex(ki); idxKey != nil {
					kiCache[rkv.kstr], ki = idxKey, idxKey
					ok = true
				}
			}
			rev := bytesToRev(rkv.key)
			currentRev = rev.Main
			if ok {
				if isTombstone(rkv.key) {
					if err := ki.tombstone(lg, rev.Main, rev.Sub); err != nil {
						lg.Warn("tombstone encountered error", zap.Error(err))
					}
					continue
				}
				ki.put(lg, rev.Main, rev.Sub)
			} else if !isTombstone(rkv.key) {
				ki.restore(lg, revision{rkv.kv.CreateRevision, 0}, rev, rkv.kv.Version)
				idx.Insert(ki)
				kiCache[rkv.kstr] = ki
			}
		}
	}()
	return rkvc, revc
}

func restoreChunk(lg *zap.Logger, kvc chan<- revKeyValue, keys, vals [][]byte, keyToLease map[string]lease.LeaseID) {
	for i, key := range keys {
		rkv := revKeyValue{key: key}
		if err := rkv.kv.Unmarshal(vals[i]); err != nil {
			lg.Fatal("failed to unmarshal mvccpb.KeyValue", zap.Error(err))
		}
		rkv.kstr = string(rkv.kv.Key)
		if isTombstone(key) {
			delete(keyToLease, rkv.kstr)
		} else if lid := lease.LeaseID(rkv.kv.Lease); lid != lease.NoLease {
			keyToLease[rkv.kstr] = lid
		} else {
			delete(keyToLease, rkv.kstr)
		}
		kvc <- rkv
	}
}

func (s *store) Close() error {
	close(s.stopc)
	s.fifoSched.Stop()
	return nil
}

// appendMarkTombstone appends tombstone mark to normal revision bytes.
func appendMarkTombstone(lg *zap.Logger, b []byte) []byte {
	if len(b) != revBytesLen {
		lg.Panic(
			"cannot append tombstone mark to non-normal revision bytes",
			zap.Int("expected-revision-bytes-size", revBytesLen),
			zap.Int("given-revision-bytes-size", len(b)),
		)
	}
	return append(b, markTombstone)
}

// isTombstone checks whether the revision bytes is a tombstone.
func isTombstone(b []byte) bool {
	return len(b) == markedRevBytesLen && b[markBytePosition] == markTombstone
}
