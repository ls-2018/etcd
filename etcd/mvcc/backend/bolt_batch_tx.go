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

package backend

import (
	"bytes"
	"math"
	"sync"
	"sync/atomic"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

type BucketID int

type Bucket interface {
	ID() BucketID // ID返回一个水桶的唯一标识符.该ID必须不被持久化并且可以在内存地图中作为轻量级的标识符使用.
	Name() []byte
	String() string
	// IsSafeRangeBucket 是一种避免无意中读取重复key的方法;bucket上的覆盖应该只取limit=1,但已知safeerangebucket永远不会覆盖任何键,所以range是安全的.
	IsSafeRangeBucket() bool // 不要在非键桶上使用unsafeRange
}

// BatchTx 负责读请求
type BatchTx interface {
	ReadTx
	UnsafeCreateBucket(bucket Bucket)
	UnsafeDeleteBucket(bucket Bucket)
	UnsafePut(bucket Bucket, key []byte, value []byte)
	UnsafeSeqPut(bucket Bucket, key []byte, value []byte)
	UnsafeDelete(bucket Bucket, key []byte)
	Commit()        // Commit commits a previous tx and begins a new writable one.
	CommitAndStop() // CommitAndStop commits the previous tx and does not create a new one.
}

type batchTx struct {
	sync.Mutex
	tx      *bolt.Tx
	backend *backend
	pending int // 当前事务中的写入次数
}

func (t *batchTx) Lock() {
	t.Mutex.Lock()
}

func (t *batchTx) Unlock() {
	if t.pending >= t.backend.batchLimit {
		t.commit(false)
	}
	t.Mutex.Unlock()
}

func (t *batchTx) RLock() {
	panic("unexpected RLock")
}

func (t *batchTx) RUnlock() {
	panic("unexpected RUnlock")
}

func (t *batchTx) UnsafeCreateBucket(bucket Bucket) {
	_, err := t.tx.CreateBucket(bucket.Name())
	if err != nil && err != bolt.ErrBucketExists {
		t.backend.lg.Fatal("创建bucket", zap.Stringer("bucket-name", bucket), zap.Error(err))
	}
	t.pending++
}

func (t *batchTx) UnsafePut(bucket Bucket, key []byte, value []byte) {
	t.unsafePut(bucket, key, value, false)
}

// UnsafeSeqPut OK
func (t *batchTx) UnsafeSeqPut(bucket Bucket, key []byte, value []byte) {
	t.unsafePut(bucket, key, value, true)
}

// OK
func (t *batchTx) unsafePut(bucketType Bucket, key []byte, value []byte, seq bool) {
	bucket := t.tx.Bucket(bucketType.Name())
	if bucket == nil {
		t.backend.lg.Fatal("找不到bolt.db里的桶", zap.Stringer("bucket-name", bucketType), zap.Stack("stack"))
	}
	if seq {
		// 当工作负载大多为仅附加时,增加填充百分比是很有用的.这可以延迟页面分割和减少空间使用.
		// 告诉bolt 当页面已满时,它应该告诉它做一个 90-10 拆分,而不是 50-50 拆分,这更适合于顺序插入.这样可以让其体积稍小.
		// 一个例子:使用 FillPercent = 0.9 之前是 103MB,使用之后是64MB,实际数据是22MB.
		bucket.FillPercent = 0.9
	}
	if err := bucket.Put(key, value); err != nil {
		t.backend.lg.Fatal(
			"桶写数据失败", zap.Stringer("bucket-name", bucketType), zap.Error(err),
		)
	}
	t.pending++
}

// UnsafeRange 调用法必须持锁
func (t *batchTx) UnsafeRange(bucketType Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	bucket := t.tx.Bucket(bucketType.Name())
	if bucket == nil {
		t.backend.lg.Fatal("无法找到bucket", zap.Stringer("bucket-name", bucketType), zap.Stack("stack"))
	}
	return unsafeRange(bucket.Cursor(), key, endKey, limit)
}

// 从bolt.db 查找k,v
func unsafeRange(c *bolt.Cursor, key, endKey []byte, limit int64) (keys [][]byte, vs [][]byte) {
	if limit <= 0 {
		limit = math.MaxInt64
	}
	var isMatch func(b []byte) bool
	if len(endKey) > 0 {
		// 判断是不是相等
		isMatch = func(b []byte) bool { return bytes.Compare(b, endKey) < 0 }
	} else {
		isMatch = func(b []byte) bool { return bytes.Equal(b, key) }
		limit = 1
	}

	for ck, cv := c.Seek(key); ck != nil && isMatch(ck); ck, cv = c.Next() {
		vs = append(vs, cv)
		keys = append(keys, ck)
		if limit == int64(len(keys)) {
			break
		}
	}
	return keys, vs
}

// UnsafeDelete 调用方必须持锁
func (t *batchTx) UnsafeDelete(bucketType Bucket, key []byte) {
	bucket := t.tx.Bucket(bucketType.Name())
	if bucket == nil {
		t.backend.lg.Fatal(
			"查找桶失败",
			zap.Stringer("bucket-name", bucketType),
			zap.Stack("stack"),
		)
	}
	err := bucket.Delete(key)
	if err != nil {
		t.backend.lg.Fatal(
			"删除一个key失败",
			zap.Stringer("bucket-name", bucketType),
			zap.Error(err),
		)
	}
	t.pending++
}

// Commit commits a previous tx and begins a new writable one.
func (t *batchTx) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

// CommitAndStop commits the previous tx and does not create a new one.
func (t *batchTx) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTx) safePending() int {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	return t.pending
}

func (t *batchTx) commit(stop bool) {
	// 提交最新的事务
	if t.tx != nil {
		if t.pending == 0 && !stop {
			return
		}
		err := t.tx.Commit() // bolt.Commit
		atomic.AddInt64(&t.backend.commits, 1)

		t.pending = 0
		if err != nil {
			t.backend.lg.Fatal("提交事务失败", zap.Error(err))
		}
	}
	if !stop {
		t.tx = t.backend.begin(true)
	}
}

// -------------------------------------------- OVER  -------------------------------------------------------------

// UnsafeForEach 调用方必须持锁
func (t *batchTx) UnsafeForEach(bucket Bucket, visitor func(k, v []byte) error) error {
	return unsafeForEach(t.tx, bucket, visitor)
}

func unsafeForEach(tx *bolt.Tx, bucket Bucket, visitor func(k, v []byte) error) error {
	if b := tx.Bucket(bucket.Name()); b != nil {
		return b.ForEach(visitor)
	}
	return nil
}

// UnsafeDeleteBucket 删除桶
func (t *batchTx) UnsafeDeleteBucket(bucket Bucket) {
	err := t.tx.DeleteBucket(bucket.Name())
	if err != nil && err != bolt.ErrBucketNotFound {
		t.backend.lg.Fatal("删除桶失败", zap.Stringer("bucket-name", bucket), zap.Error(err))
	}
	t.pending++
}
