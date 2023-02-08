package mvcc

import (
	"github.com/ls-2018/etcd_cn/etcd/lease"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/buckets"
	"github.com/ls-2018/etcd_cn/offical/api/v3/mvccpb"
	"go.uber.org/zap"
)

// OK
func (tw *storeTxnWrite) put(key, value []byte, leaseID lease.LeaseID) {
	rev := tw.beginRev + 1 // 事务开始时有一个ID,写这个操作,对应的ID应+1
	c := rev
	oldLease := lease.NoLease

	// 如果该键之前存在,使用它之前创建的并获取它之前的leaseID
	_, created, beforeVersion, err := tw.s.kvindex.Get(key, rev) // 0,0,nil  <= rev的最新修改
	if err == nil {
		c = created.Main
		oldLease = tw.s.le.GetLease(lease.LeaseItem{Key: string(key)})
		tw.trace.Step("获取键先前的created_revision和leaseID")
	}
	indexBytes := newRevBytes()
	idxRev := revision{Main: rev, Sub: int64(len(tw.changes))} // 当前请求的修订版本
	revToBytes(idxRev, indexBytes)

	kv := mvccpb.KeyValue{
		Key:            string(key),
		Value:          string(value),
		CreateRevision: c,                 // 当前代,创建时的修订版本
		ModRevision:    rev,               // 修订版本
		Version:        beforeVersion + 1, // Version是key的版本.删除键会将该键的版本重置为0,对键的任何修改都会增加它的版本.
		Lease:          int64(leaseID),    // 租约ID
	}

	d, err := kv.Marshal()
	if err != nil {
		tw.storeTxnRead.s.lg.Fatal("序列化失败 mvccpb.KeyValue", zap.Error(err))
	}

	tw.trace.Step("序列化 mvccpb.KeyValue")
	tw.tx.UnsafeSeqPut(buckets.Key, indexBytes, d) // ✅ 写入db,buf
	_ = (&treeIndex{}).Put
	tw.s.kvindex.Put(key, idxRev) // 当前请求的修订版本
	tw.changes = append(tw.changes, kv)
	tw.trace.Step("存储键值对到bolt.db")

	// 如果用户没穿,就是 NoLease
	if oldLease != lease.NoLease {
		if tw.s.le == nil {
			panic("没找到租约")
		}
		// 分离旧的租约
		err = tw.s.le.Detach(oldLease, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			tw.storeTxnRead.s.lg.Error("从key中分离旧的租约失败", zap.Error(err))
		}
	}
	if leaseID != lease.NoLease {
		if tw.s.le == nil {
			panic("没找到租约")
		}
		err = tw.s.le.Attach(leaseID, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			panic("租约附加失败")
		}
	}
	tw.trace.Step("附加租约到key")
}

func (tw *storeTxnWrite) Put(key, value []byte, lease lease.LeaseID) int64 {
	tw.put(key, value, lease)
	return tw.beginRev + 1
}
