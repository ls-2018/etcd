package mvcc

import (
	"github.com/ls-2018/etcd_cn/etcd/lease"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/buckets"
	"github.com/ls-2018/etcd_cn/offical/api/v3/mvccpb"
	"github.com/ls-2018/etcd_cn/pkg/traceutil"
	"go.uber.org/zap"
)

// DeleteRange 1
func (wv *writeView) DeleteRange(key, end []byte) (n, rev int64) {
	tw := wv.kv.Write(traceutil.TODO())
	defer tw.End()

	return tw.(*storeTxnWrite).DeleteRange(key, end)
}

// DeleteRange 2
func (tw *storeTxnWrite) DeleteRange(key, end []byte) (int64, int64) {
	if n := tw.deleteRange(key, end); n != 0 || len(tw.changes) > 0 {
		return n, tw.beginRev + 1
	}
	return 0, tw.beginRev
}

// 从k,v index中删除
func (tw *storeTxnWrite) deleteRange(key, end []byte) int64 {
	rrev := tw.beginRev
	if len(tw.changes) > 0 {
		rrev++
	}
	keys, _ := tw.s.kvindex.Range(key, end, rrev)
	if len(keys) == 0 {
		return 0
	}
	for _, key := range keys {
		tw.delete(key)
	} // 4.20 号
	return int64(len(keys))
}

// bolt.db 删除数据
func (tw *storeTxnWrite) delete(key []byte) {
	indexBytes := newRevBytes()
	idxRev := revision{Main: tw.beginRev + 1, Sub: int64(len(tw.changes))}
	revToBytes(idxRev, indexBytes)

	indexBytes = appendMarkTombstone(tw.storeTxnRead.s.lg, indexBytes)

	kv := mvccpb.KeyValue{Key: string(key)}

	d, err := kv.Marshal()
	if err != nil {
		tw.storeTxnRead.s.lg.Fatal("序列化失败 mvccpb.KeyValue", zap.Error(err))
	}

	tw.tx.UnsafeSeqPut(buckets.Key, indexBytes, d)
	err = tw.s.kvindex.Tombstone(key, idxRev)
	if err != nil {
		tw.storeTxnRead.s.lg.Fatal(
			"failed to tombstone an existing Key",
			zap.String("Key", string(key)),
			zap.Error(err),
		)
	}
	tw.changes = append(tw.changes, kv)

	item := lease.LeaseItem{Key: string(key)}
	leaseID := tw.s.le.GetLease(item)

	if leaseID != lease.NoLease {
		err = tw.s.le.Detach(leaseID, []lease.LeaseItem{item})
		if err != nil {
			tw.storeTxnRead.s.lg.Error("未能从key上分离出旧租约", zap.Error(err))
		}
	}
}
