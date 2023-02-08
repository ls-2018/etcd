package mvcc

import (
	"context"

	"github.com/ls-2018/etcd_cn/etcd/mvcc/buckets"
	"github.com/ls-2018/etcd_cn/offical/api/v3/mvccpb"
	"go.uber.org/zap"
)

// Range OK
func (tr *storeTxnRead) Range(ctx context.Context, key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	return tr.rangeKeys(ctx, key, end, tr.Rev(), ro)
}

func (tw *storeTxnWrite) Range(ctx context.Context, key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	rev := tw.beginRev
	if len(tw.changes) > 0 {
		rev++
	}
	return tw.rangeKeys(ctx, key, end, rev, ro)
}

// OK
func (tr *storeTxnRead) rangeKeys(ctx context.Context, key, end []byte, curRev int64, ro RangeOptions) (*RangeResult, error) {
	rev := ro.Rev // 指定修订版本
	if rev > curRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: curRev}, ErrFutureRev
	}
	if rev <= 0 {
		rev = curRev
	}
	if rev < tr.s.compactMainRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: 0}, ErrCompacted
	}
	if ro.Count { // 是否统计修订版本数
		total := tr.s.kvindex.CountRevisions(key, end, rev)
		tr.trace.Step("从内存索引树中统计修订数")
		return &RangeResult{KVs: nil, Count: total, Rev: curRev}, nil
	}
	// 获取版本数据
	revpairs, total := tr.s.kvindex.Revisions(key, end, rev, int(ro.Limit))
	tr.trace.Step("从内存索引树中获取指定范围的keys")
	if len(revpairs) == 0 {
		return &RangeResult{KVs: nil, Count: total, Rev: curRev}, nil
	}

	limit := int(ro.Limit)
	if limit <= 0 || limit > len(revpairs) {
		limit = len(revpairs) // 实际收到的数据量
	}

	kvs := make([]mvccpb.KeyValue, limit)
	revBytes := newRevBytes() // len 为17的数组
	// 拿着索引数据去bolt.db 查数据
	for i, revpair := range revpairs[:len(kvs)] {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		revToBytes(revpair, revBytes)
		// 根据修订版本获取数据
		_, vs := tr.tx.UnsafeRange(buckets.Key, revBytes, nil, 0)
		if len(vs) != 1 {
			tr.s.lg.Fatal("Range找不到修订对", zap.Int64("revision-Main", revpair.Main), zap.Int64("revision-Sub", revpair.Sub))
		}
		if err := kvs[i].Unmarshal([]byte(vs[0])); err != nil {
			tr.s.lg.Fatal(
				"反序列失败 mvccpb.KeyValue",
				zap.Error(err),
			)
		}
	}
	tr.trace.Step("从bolt.db 中range Key")
	return &RangeResult{KVs: kvs, Count: total, Rev: curRev}, nil
}
