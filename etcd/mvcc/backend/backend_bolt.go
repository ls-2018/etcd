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
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	humanize "github.com/dustin/go-humanize"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

var (
	defaultBatchLimit    = 10000
	defaultBatchInterval = 100 * time.Millisecond

	defragLimit = 10000

	// initialMmapSize is the initial size of the mmapped region. Setting this larger than
	// the potential max db size can prevent writer from blocking reader.
	// This only works for linux.
	initialMmapSize = uint64(10 * 1024 * 1024 * 1024)

	// minSnapshotWarningTimeout is the minimum threshold to trigger a long running snapshot warning.
	minSnapshotWarningTimeout = 30 * time.Second
)

type Backend interface {
	ReadTx() ReadTx           // // ReadTx 返回一个读事务.它被主数据路径中的 ConcurrentReadTx 替换
	BatchTx() BatchTx         // 开启写事务
	ConcurrentReadTx() ReadTx // 主流程中都是使用的这个并发读事务
	Snapshot() Snapshot       // 对db做快照
	Hash(ignores func(bucketName, keyName []byte) bool) (uint32, error)
	Size() int64        // DB占用的物理磁盘大小,空间可以预分配,所以不是实际数据大小
	SizeInUse() int64   // 实际使用的磁盘空间
	OpenReadTxN() int64 // 返回当前读事务个数
	Defrag() error      // 数据文件整理,会回收已删除key和已更新的key旧版本占用的磁盘
	ForceCommit()       // 强制当前的批处理tx提交
	Close() error
}

type Snapshot interface {
	Size() int64                              // 快照的大小
	WriteTo(w io.Writer) (n int64, err error) // 写快照数据
	Close() error                             // 关闭快照
}

type txReadBufferCache struct {
	mu         sync.Mutex
	buf        *txReadBuffer
	bufVersion uint64
}

type (
	MyBackend = backend
	backend   struct {
		size          int64            // 已经占用的磁盘大小
		sizeInUse     int64            // 实际使用的大小
		commits       int64            // 已提交事务数
		openReadTxN   int64            // 当前开启的读事务数
		mlock         bool             // mlock prevents backend database file to be swapped
		boltdbMu      sync.RWMutex     // 这里的锁也是隔离下面的db对象；正常的创建bolt.DB事务只需要读锁；但是做 defrag 时候需要写锁隔离
		db            *bolt.DB         // 底层存储为boltDB
		batchInterval time.Duration    // 批量写提交间隔   默认100ms
		batchLimit    int              // 批量写最大事务数  10000
		batchTx       *batchTxBuffered // 负责写请求
		readTx        *readTx          // 负责读请求
		// txReadBufferCache mirrors "txReadBuffer" within "readTx" -- readTx.baseReadTx.buf.
		// When creating "concurrentReadTx":
		// - if the cache is up-to-date, "readTx.baseReadTx.buf" copy can be skipped
		// - if the cache is empty or outdated, "readTx.baseReadTx.buf" copy is required
		txReadBufferCache txReadBufferCache
		stopc             chan struct{}
		donec             chan struct{}
		hooks             Hooks
		lg                *zap.Logger
	}
)

type BackendConfig struct {
	Path                string            // 是指向后端文件的文件路径.
	BatchInterval       time.Duration     // 是冲刷BatchTx之前的最长时间
	BatchLimit          int               // 是冲刷BatchTx之前的最大puts数
	BackendFreelistType bolt.FreelistType // 是后端boltdb的freelist类型
	MmapSize            uint64            // 是为后端提供的mmap的字节数.
	Logger              *zap.Logger       //
	UnsafeNoFsync       bool              `json:"unsafe-no-fsync"` // 禁用所有fsync的使用.
	Mlock               bool              // 防止后端数据库文件被调换
	Hooks               Hooks             // 在后端事务的生命周期中被执行
}

func DefaultBackendConfig() BackendConfig {
	return BackendConfig{
		BatchInterval: defaultBatchInterval,
		BatchLimit:    defaultBatchLimit,
		MmapSize:      initialMmapSize,
	}
}

func New(bcfg BackendConfig) Backend {
	return newBackend(bcfg)
}

func NewDefaultBackend(path string) Backend {
	bcfg := DefaultBackendConfig()
	bcfg.Path = path
	return newBackend(bcfg)
}

func newBackend(bcfg BackendConfig) *backend {
	if bcfg.Logger == nil {
		bcfg.Logger = zap.NewNop()
	}

	bopts := &bolt.Options{}
	if boltOpenOptions != nil {
		*bopts = *boltOpenOptions
	}
	bopts.InitialMmapSize = bcfg.mmapSize()
	bopts.FreelistType = bcfg.BackendFreelistType
	bopts.NoSync = bcfg.UnsafeNoFsync
	bopts.NoGrowSync = bcfg.UnsafeNoFsync
	bopts.Mlock = bcfg.Mlock

	db, err := bolt.Open(bcfg.Path, 0o600, bopts)
	if err != nil {
		bcfg.Logger.Panic("打开数据库失败", zap.String("path", bcfg.Path), zap.Error(err))
	}

	b := &backend{
		db: db,

		batchInterval: bcfg.BatchInterval,
		batchLimit:    bcfg.BatchLimit,
		mlock:         bcfg.Mlock,

		readTx: &readTx{
			baseReadTx: baseReadTx{
				buf: txReadBuffer{
					txBuffer:   txBuffer{make(map[BucketID]*bucketBuffer)},
					bufVersion: 0,
				},
				buckets: make(map[BucketID]*bolt.Bucket),
				txWg:    new(sync.WaitGroup),
				txMu:    new(sync.RWMutex),
			},
		},
		txReadBufferCache: txReadBufferCache{
			mu:         sync.Mutex{},
			bufVersion: 0,
			buf:        nil,
		},

		stopc: make(chan struct{}),
		donec: make(chan struct{}),

		lg: bcfg.Logger,
	}

	b.batchTx = newBatchTxBuffered(b)
	b.hooks = bcfg.Hooks

	go b.run()
	return b
}

// BatchTx 返回当前的批次tx.该tx可以用于读和写操作.
// 写入的结果可以立即在同一个tx中被检索到.
// 写入的结果与其他tx隔离直到当前的tx被提交.
func (b *backend) BatchTx() BatchTx {
	return b.batchTx
}

func (b *backend) ReadTx() ReadTx { return b.readTx }

// ConcurrentReadTx 创建并返回一个新的 ReadTx它.
// A) 创建并保留backend.readTx.txReadBuffer的副本.
// B) 引用当前批次间隔的 boltdb read Tx(和它的桶缓存).
func (b *backend) ConcurrentReadTx() ReadTx {
	// 这里需要读 readTx 的buffer 所以需要读锁 这里的锁占用时间是很低的
	b.readTx.RLock()
	defer b.readTx.RUnlock()
	b.readTx.txWg.Add(1)

	b.txReadBufferCache.mu.Lock()

	curCache := b.txReadBufferCache.buf           // 当前的缓存
	curCacheVer := b.txReadBufferCache.bufVersion // 缓存里的版本
	curBufVer := b.readTx.buf.bufVersion          // 当前的版本

	isEmptyCache := curCache == nil
	isStaleCache := curCacheVer != curBufVer // 是不是陈旧的缓存

	var buf *txReadBuffer
	switch {
	case isEmptyCache: // 缓冲为空
		// 当持有b.txReadBufferCache.boltdbMu.Lock时执行安全的缓冲区拷贝这只应该运行一次,所以不会有太多的开销
		curBuf := b.readTx.buf.unsafeCopy()
		buf = &curBuf
	case isStaleCache:
		// 最大化并发,尝试不安全的缓冲区拷贝在复制buffer时释放锁——cache可能会再次失效
		// 被其他人覆盖.因此,我们需要再次检查readTx缓冲区版本
		b.txReadBufferCache.mu.Unlock()
		curBuf := b.readTx.buf.unsafeCopy()
		b.txReadBufferCache.mu.Lock()
		buf = &curBuf
	default:
		// 既不为空也不过时的缓存,只使用当前缓冲区
		buf = curCache
	}
	if isEmptyCache || curCacheVer == b.txReadBufferCache.bufVersion {
		b.txReadBufferCache.buf = buf
		b.txReadBufferCache.bufVersion = curBufVer
	}

	b.txReadBufferCache.mu.Unlock()
	// concurrentReadTx 不应该写入它的 txReadBuffer
	return &concurrentReadTx{
		baseReadTx: baseReadTx{
			buf:     *buf, // copy一份backend的readTx.buf, 这样就可以不用持有readTx.mu对buffer的保护从而提升读的性能 这里就是空间换时间(锁的竞争)
			txMu:    b.readTx.txMu,
			tx:      b.readTx.tx,
			buckets: b.readTx.buckets,
			txWg:    b.readTx.txWg,
		},
	}
}

// ForceCommit 强制当前的批处理tx提交.
func (b *backend) ForceCommit() {
	b.batchTx.Commit()
}

// Snapshot 获取一个blot.db快照结构体
func (b *backend) Snapshot() Snapshot {
	b.batchTx.Commit()

	b.boltdbMu.RLock()
	defer b.boltdbMu.RUnlock()
	tx, err := b.db.Begin(false) // 读事务
	if err != nil {
		b.lg.Fatal("开启读事务失败", zap.Error(err))
	}

	stopc, donec := make(chan struct{}), make(chan struct{})
	dbBytes := tx.Size() // 返回该事务所看到的当前数据库大小(以字节为单位).
	go func() {
		defer close(donec)
		// sendRateBytes基于1千兆/秒的连接传输快照数据,假设tcp最小吞吐量为100MB/s.
		var sendRateBytes int64 = 100 * 1024 * 1024
		warningTimeout := time.Duration(int64((float64(dbBytes) / float64(sendRateBytes)) * float64(time.Second)))
		if warningTimeout < minSnapshotWarningTimeout {
			warningTimeout = minSnapshotWarningTimeout
		}
		start := time.Now()
		ticker := time.NewTicker(warningTimeout)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				b.lg.Warn("快照传输时间过长", zap.Duration("taking", time.Since(start)),
					zap.Int64("bytes", dbBytes),
					zap.String("size", humanize.Bytes(uint64(dbBytes))),
				)
			case <-stopc:
				return
			}
		}
	}()

	return &snapshot{tx, stopc, donec}
}

func (b *backend) Hash(ignores func(bucketName, keyName []byte) bool) (uint32, error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	b.boltdbMu.RLock()
	defer b.boltdbMu.RUnlock()
	err := b.db.View(func(tx *bolt.Tx) error {
		c := tx.Cursor()
		for next, _ := c.First(); next != nil; next, _ = c.Next() {
			b := tx.Bucket(next)
			if b == nil {
				return fmt.Errorf("获取桶的hash失败 %s", string(next))
			}
			h.Write(next)
			b.ForEach(func(k, v []byte) error {
				if ignores != nil && !ignores(next, k) {
					fmt.Println(string(k), string(v))
					h.Write(k)
					h.Write(v)
				}
				return nil
			})
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return h.Sum32(), nil
}

func (b *backend) Size() int64 {
	return atomic.LoadInt64(&b.size)
}

func (b *backend) SizeInUse() int64 {
	return atomic.LoadInt64(&b.sizeInUse)
}

// 提交bolt事务
func (b *backend) run() {
	defer close(b.donec)
	t := time.NewTimer(b.batchInterval) // 100ms 定时提交事务
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-b.stopc:
			b.batchTx.CommitAndStop()
			return
		}
		if b.batchTx.safePending() != 0 {
			b.batchTx.Commit()
		}
		t.Reset(b.batchInterval) // 使其重新触发
	}
}

func (b *backend) Close() error {
	close(b.stopc)
	<-b.donec
	return b.db.Close()
}

// Commits returns total number of commits since start
func (b *backend) Commits() int64 {
	return atomic.LoadInt64(&b.commits)
}

// Defrag 碎片整理
func (b *backend) Defrag() error {
	return b.defrag()
}

// 碎片整理
func (b *backend) defrag() error {
	now := time.Now()

	// 锁定batchTx以确保没有人在使用以前的tx然后关闭以前正在进行的tx.
	b.batchTx.Lock()
	defer b.batchTx.Unlock()

	// 锁定数据库后锁定tx以避免死锁.
	b.boltdbMu.Lock()
	defer b.boltdbMu.Unlock()

	// 阻止并发的读请求同时重置TX.
	b.readTx.Lock()
	defer b.readTx.Unlock()

	b.batchTx.unsafeCommit(true)

	b.batchTx.tx = nil

	// Create a temporary file to ensure we start with a clean slate.
	// Snapshotter.cleanupSnapdir cleans up any of these that are found during startup.
	dir := filepath.Dir(b.db.Path())
	temp, err := ioutil.TempFile(dir, "db.tmp.*")
	if err != nil {
		return err
	}
	options := bolt.Options{}
	if boltOpenOptions != nil {
		options = *boltOpenOptions
	}
	options.OpenFile = func(_ string, _ int, _ os.FileMode) (file *os.File, err error) {
		return temp, nil
	}
	// 不管打开选项是什么,都不要加载tmp db到内存中
	options.Mlock = false
	tdbp := temp.Name()
	tmpdb, err := bolt.Open(tdbp, 0o600, &options)
	if err != nil {
		return err
	}

	dbp := b.db.Path()
	size1, sizeInUse1 := b.Size(), b.SizeInUse()
	if b.lg != nil {
		b.lg.Info(
			"內存碎片清理中",
			zap.String("path", dbp),
			zap.Int64("current-db-size-bytes", size1),
			zap.String("current-db-size", humanize.Bytes(uint64(size1))),
			zap.Int64("current-db-size-in-use-bytes", sizeInUse1),
			zap.String("current-db-size-in-use", humanize.Bytes(uint64(sizeInUse1))),
		)
	}
	err = defragdb(b.db, tmpdb, defragLimit)
	if err != nil {
		tmpdb.Close()
		if rmErr := os.RemoveAll(tmpdb.Path()); rmErr != nil {
			b.lg.Error("在碎片整理完成后未能删除db.tmp", zap.Error(rmErr))
		}
		return err
	}

	err = b.db.Close()
	if err != nil {
		b.lg.Fatal("关闭数据库失败", zap.Error(err))
	}
	err = tmpdb.Close()
	if err != nil {
		b.lg.Fatal("关闭tmp数据库失败", zap.Error(err))
	}
	err = os.Rename(tdbp, dbp)
	if err != nil {
		b.lg.Fatal("重命名tmp数据库失败", zap.Error(err))
	}

	defragmentedBoltOptions := bolt.Options{}
	if boltOpenOptions != nil {
		defragmentedBoltOptions = *boltOpenOptions
	}
	defragmentedBoltOptions.Mlock = b.mlock

	b.db, err = bolt.Open(dbp, 0o600, &defragmentedBoltOptions)
	if err != nil {
		b.lg.Fatal("打开数据库失败", zap.String("path", dbp), zap.Error(err))
	}
	b.batchTx.tx = b.unsafeBegin(true)

	b.readTx.reset()
	b.readTx.tx = b.unsafeBegin(false)

	size := b.readTx.tx.Size()
	db := b.readTx.tx.DB()
	atomic.StoreInt64(&b.size, size)
	atomic.StoreInt64(&b.sizeInUse, size-(int64(db.Stats().FreePageN)*int64(db.Info().PageSize)))

	took := time.Since(now)

	size2, sizeInUse2 := b.Size(), b.SizeInUse()
	if b.lg != nil {
		b.lg.Info(
			"完成了目录碎片整理工作",
			zap.String("path", dbp),
			zap.Int64("current-db-size-bytes-diff", size2-size1),
			zap.Int64("current-db-size-bytes", size2),
			zap.String("current-db-size", humanize.Bytes(uint64(size2))),
			zap.Int64("current-db-size-in-use-bytes-diff", sizeInUse2-sizeInUse1),
			zap.Int64("current-db-size-in-use-bytes", sizeInUse2),
			zap.String("current-db-size-in-use", humanize.Bytes(uint64(sizeInUse2))),
			zap.Duration("took", took),
		)
	}
	return nil
}

func defragdb(odb, tmpdb *bolt.DB, limit int) error {
	// open a tx on tmpdb for writes
	tmptx, err := tmpdb.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tmptx.Rollback()
		}
	}()

	// open a tx on old db for read
	tx, err := odb.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	c := tx.Cursor()

	count := 0
	for next, _ := c.First(); next != nil; next, _ = c.Next() {
		b := tx.Bucket(next)
		if b == nil {
			return fmt.Errorf("backend: cannot defrag bucket %s", string(next))
		}

		tmpb, berr := tmptx.CreateBucketIfNotExists(next)
		if berr != nil {
			return berr
		}
		tmpb.FillPercent = 0.9 // for bucket2seq write in for each

		if err = b.ForEach(func(k, v []byte) error {
			count++
			if count > limit {
				err = tmptx.Commit()
				if err != nil {
					return err
				}
				tmptx, err = tmpdb.Begin(true)
				if err != nil {
					return err
				}
				tmpb = tmptx.Bucket(next)
				tmpb.FillPercent = 0.9 // for bucket2seq write in for each

				count = 0
			}
			return tmpb.Put(k, v)
		}); err != nil {
			return err
		}
	}

	return tmptx.Commit()
}

func (b *backend) begin(write bool) *bolt.Tx {
	b.boltdbMu.RLock()
	tx := b.unsafeBegin(write)
	b.boltdbMu.RUnlock()

	size := tx.Size() // 返回该事务所看到的当前数据库大小(字节).   24576
	db := tx.DB()
	stats := db.Stats()
	atomic.StoreInt64(&b.size, size)
	atomic.StoreInt64(&b.sizeInUse, size-(int64(stats.FreePageN)*int64(db.Info().PageSize))) // 24576-2*4096
	atomic.StoreInt64(&b.openReadTxN, int64(stats.OpenTxN))                                  // 当前的的读事务数

	return tx
}

// 开启写事务?
func (b *backend) unsafeBegin(write bool) *bolt.Tx {
	tx, err := b.db.Begin(write)
	if err != nil {
		b.lg.Fatal("开启事务失败", zap.Error(err))
	}
	return tx
}

func (b *backend) OpenReadTxN() int64 {
	return atomic.LoadInt64(&b.openReadTxN)
}

type snapshot struct {
	*bolt.Tx
	stopc chan struct{}
	donec chan struct{}
}

func (s *snapshot) Close() error {
	close(s.stopc)
	<-s.donec
	return s.Tx.Rollback()
}
