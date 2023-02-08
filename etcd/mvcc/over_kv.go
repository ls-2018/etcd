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

	"github.com/ls-2018/etcd_cn/etcd/lease"
	"github.com/ls-2018/etcd_cn/offical/api/v3/mvccpb"
)

// RangeOptions 请求参数
type RangeOptions struct {
	Limit int64 // 用户限制的数据量
	Rev   int64 // 指定的修订版本
	Count bool  // 是否统计修订版本数
}

// RangeResult 响应
type RangeResult struct {
	KVs   []mvccpb.KeyValue
	Rev   int64 // 最新的修订版本
	Count int   // 统计当前的   修订版本数
}

type ReadView interface {
	// FirstRev
	//     before    			   cur
	//             compact
	//           rev       rev   		rev
	FirstRev() int64                                                                         // 在打开txn时返回第一个KV修订.在压实之后,第一个修订增加到压实修订.
	Rev() int64                                                                              // 在打开txn时返回KV的修订.
	Range(ctx context.Context, key, end []byte, ro RangeOptions) (r *RangeResult, err error) // 读取数据
}

// TxnRead 只读事务,不会锁住其他只读事务
type TxnRead interface {
	ReadView
	End() // 标记事务已完成 并且准备提交
}

type WriteView interface {
	DeleteRange(key, end []byte) (n, rev int64) // 删除指定范围的数据
	// Put 将给定的k v放入存储区.Put还接受额外的参数lease,将lease作为元数据附加到键值对上.KV实现 不验证租约id.
	// put还会增加存储的修订版本,并在事件历史中生成一个事件.返回的修订版本是执行操作时KV的当前修订版本.
	Put(key, value []byte, lease lease.LeaseID) (rev int64)
}

type TxnWrite interface {
	TxnRead
	WriteView
	// Changes 获取打开write txn后所做的更改.
	Changes() []mvccpb.KeyValue
}

// txnReadWrite 读事务-->写事务,对任何写操作都感到恐慌.
type txnReadWrite struct {
	TxnRead
}

func (trw *txnReadWrite) DeleteRange(key, end []byte) (n, rev int64) { panic("unexpected DeleteRange") }
func (trw *txnReadWrite) Put(key, value []byte, lease lease.LeaseID) (rev int64) {
	panic("unexpected Put")
}
func (trw *txnReadWrite) Changes() []mvccpb.KeyValue { return nil }

func NewReadOnlyTxnWrite(txn TxnRead) TxnWrite { return &txnReadWrite{txn} }

type ReadTxMode uint32

const (
	ConcurrentReadTxMode = ReadTxMode(1) // 缓冲区拷贝,提高性能   并发ReadTx模式
	SharedBufReadTxMode  = ReadTxMode(2)
)
