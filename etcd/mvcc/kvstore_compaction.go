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
	"encoding/binary"
	"time"

	"github.com/ls-2018/etcd_cn/etcd/mvcc/buckets"
	"go.uber.org/zap"
)

// scheduleCompaction 任务遍历、删除 Key 的过程可能会对 boltdb 造成压力,为了不影响正常读写请求,它在执行过程中会通过参数控制每次遍历、
// 删除的 Key 数（默认为 100,每批间隔 10ms）,分批完成 boltdb Key 的删除操作.
func (s *store) scheduleCompaction(compactMainRev int64, keep map[revision]struct{}) bool {
	totalStart := time.Now()
	keyCompactions := 0

	end := make([]byte, 8)
	binary.BigEndian.PutUint64(end, uint64(compactMainRev+1))

	last := make([]byte, 8+1+8)
	for {
		var rev revision

		tx := s.b.BatchTx()
		tx.Lock()
		keys, _ := tx.UnsafeRange(buckets.Key, last, end, int64(s.cfg.CompactionBatchLimit))
		for _, key := range keys {
			rev = bytesToRev(key)
			if _, ok := keep[rev]; !ok {
				tx.UnsafeDelete(buckets.Key, key)
				keyCompactions++
			}
		}

		if len(keys) < s.cfg.CompactionBatchLimit {
			rbytes := make([]byte, 8+1+8)
			revToBytes(revision{Main: compactMainRev}, rbytes)
			tx.UnsafePut(buckets.Meta, finishedCompactKeyName, rbytes)
			tx.Unlock()
			s.lg.Info(
				"finished scheduled compaction",
				zap.Int64("compact-revision", compactMainRev),
				zap.Duration("took", time.Since(totalStart)),
			)
			return true
		}

		// update last
		revToBytes(revision{Main: rev.Main, Sub: rev.Sub + 1}, last)
		tx.Unlock()
		// Immediately commit the compaction deletes instead of letting them accumulate in the write buffer
		s.b.ForceCommit()

		select {
		case <-time.After(10 * time.Millisecond):
		case <-s.stopc:
			return false
		}
	}
}

// 当我们通过 boltdb 删除大量的 Key,在事务提交后 B+ tree 经过分裂、平衡,会释放出若干 branch/leaf page 页面,然而 boltdb 并不会将其释放给磁盘,
// 调整 db 大小操作是昂贵的,会对性能有较大的损害.
