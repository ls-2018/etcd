// Copyright 2017 The etcd Authors
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
	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"
	"github.com/ls-2018/etcd_cn/offical/api/v3/mvccpb"
	"github.com/ls-2018/etcd_cn/pkg/traceutil"
)

type storeTxnRead struct {
	s        *store
	tx       backend.ReadTx
	firstRev int64
	rev      int64 // 总的修订版本
	trace    *traceutil.Trace
}

func (tr *storeTxnRead) FirstRev() int64 { return tr.firstRev }

func (tr *storeTxnRead) Rev() int64 {
	return tr.rev
}

func (tr *storeTxnRead) End() {
	tr.tx.RUnlock() // RUnlock signals the end of concurrentReadTx.
	tr.s.mu.RUnlock()
}

type storeTxnWrite struct {
	storeTxnRead
	tx       backend.BatchTx
	beginRev int64             // 是TXN开始时的修订版本;它将写到下次修订.
	changes  []mvccpb.KeyValue // 写事务接收到的k,v 包含修订版本数据
}

func (tw *storeTxnWrite) Rev() int64 {
	return tw.beginRev
}

func (tw *storeTxnWrite) Changes() []mvccpb.KeyValue { return tw.changes }

// End 主要是用来解锁
func (tw *storeTxnWrite) End() {
	// 只有在Txn修改了Mvcc状态时才会更新索引.
	if len(tw.changes) != 0 {
		// 保持revMu锁,以防止新的读Txns打开,直到写回.
		tw.s.revMu.Lock()
		tw.s.currentRev++
	}
	tw.tx.Unlock()
	if len(tw.changes) != 0 {
		tw.s.revMu.Unlock()
	}
	tw.s.mu.RUnlock()
}
