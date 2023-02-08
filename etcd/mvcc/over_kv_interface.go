package mvcc

import (
	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"
	"github.com/ls-2018/etcd_cn/pkg/traceutil"
)

type KV interface {
	ReadView
	WriteView
	Read(mode ReadTxMode, trace *traceutil.Trace) TxnRead                           // 创建读事务
	Write(trace *traceutil.Trace) TxnWrite                                          // 创建写事务
	Hash() (hash uint32, revision int64, err error)                                 // 计算kv存储的hash值
	HashByRev(rev int64) (hash uint32, revision int64, compactRev int64, err error) // 计算所有MVCC修订到给定修订的哈希值.
	Compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error)             // 释放所有被替换的修订数小于rev的键.
	Commit()                                                                        // 将未完成的TXNS提交到底层后端.
	Restore(b backend.Backend) error
	Close() error
}

type WatchableKV interface {
	KV
	Watchable
}

type Watchable interface {
	NewWatchStream() WatchStream
}
