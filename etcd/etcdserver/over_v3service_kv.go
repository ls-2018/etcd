package etcdserver

import (
	"context"
	"time"

	"github.com/ls-2018/etcd_cn/etcd/auth"
	pb "github.com/ls-2018/etcd_cn/offical/etcdserverpb"
	"github.com/ls-2018/etcd_cn/pkg/traceutil"
)

type RaftKV interface {
	Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error)
	Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error)
	DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error)
	Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error)
	Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error)
}

func (s *EtcdServer) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	if isTxnReadonly(r) {
		trace := traceutil.New("transaction", s.Logger(), traceutil.Field{Key: "read_only", Value: true})
		ctx = context.WithValue(ctx, traceutil.TraceKey, trace)
		if !isTxnSerializable(r) {
			err := s.linearizeReadNotify(ctx)
			trace.Step("在线性读之前,保持raft节点间的一致性")
			if err != nil {
				return nil, err
			}
		}
		var resp *pb.TxnResponse
		var err error
		chk := func(ai *auth.AuthInfo) error {
			return checkTxnAuth(s.authStore, ai, r)
		}

		get := func() { resp, _, err = s.applyV3Base.Txn(ctx, r) }
		if serr := s.doSerialize(ctx, chk, get); serr != nil {
			return nil, serr
		}
		return resp, err
	}

	ctx = context.WithValue(ctx, traceutil.StartTimeKey, time.Now())
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{Txn: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.TxnResponse), nil
}

func (s *EtcdServer) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{DeleteRange: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.DeleteRangeResponse), nil
}

// Compact  压缩kv历史版本
func (s *EtcdServer) Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	startTime := time.Now()
	result, err := s.processInternalRaftRequestOnce(ctx, pb.InternalRaftRequest{Compaction: r})
	trace := traceutil.TODO()
	if result != nil && result.trace != nil {
		trace = result.trace
		applyStart := result.trace.GetStartTime()
		result.trace.SetStartTime(startTime)
		trace.InsertStep(0, applyStart, "处理raft请求")
	}
	if r.Physical && result != nil && result.physc != nil {
		<-result.physc
		// 压实工作已经完成,删除了键；现在哈希已经解决了,但数据不一定被提交.如果出现崩溃,
		// 如果压实工作恢复,哈希值可能会恢复到压实完成前的哈希值.强制完成的压实到 提交,这样它就不会在崩溃后恢复.
		s.backend.ForceCommit()
		trace.Step("物理压实")
	}
	if err != nil {
		return nil, err
	}
	if result.err != nil {
		return nil, result.err
	}
	resp := result.resp.(*pb.CompactionResponse)
	if resp == nil {
		resp = &pb.CompactionResponse{}
	}
	if resp.Header == nil {
		resp.Header = &pb.ResponseHeader{}
	}
	resp.Header.Revision = s.kv.Rev()
	trace.AddField(traceutil.Field{Key: "response_revision", Value: resp.Header.Revision})
	return resp, nil
}

// RaftRequest myself test
func (s *EtcdServer) RaftRequest(ctx context.Context, r pb.InternalRaftRequest) {
	s.raftRequest(ctx, r)
}

func (s *EtcdServer) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	trace := traceutil.New("range", s.Logger(), traceutil.Field{Key: "range_begin", Value: string(r.Key)}, traceutil.Field{Key: "range_end", Value: string(r.RangeEnd)})
	ctx = context.WithValue(ctx, traceutil.TraceKey, trace) // trace
	var resp *pb.RangeResponse
	var err error
	defer func(start time.Time) {
		if resp != nil {
			trace.AddField(
				traceutil.Field{Key: "response_count", Value: len(resp.Kvs)},
				traceutil.Field{Key: "response_revision", Value: resp.Header.Revision},
			)
		}
	}(time.Now())
	// 如果需要线性一致性读,执行 linearizableReadNotify
	// 此处将会一直阻塞直到 apply index >= read index
	if !r.Serializable {
		err = s.linearizeReadNotify(ctx) // 发准备信号,并等待结果
		trace.Step("在线性化读数之前,raft节点之间的一致.")
		if err != nil {
			return nil, err
		}
	}
	// serializable read 会直接读取当前节点的数据返回给客户端,它并不能保证返回给客户端的数据是最新的
	chk := func(ai *auth.AuthInfo) error {
		return s.authStore.IsRangePermitted(ai, []byte(r.Key), []byte(r.RangeEnd)) // health,nil
	}

	get := func() {
		_ = applierV3backend{}
		// 执行到这里说明读请求的 apply index >= read index
		// 可以安全地读 bbolt 进行 read 操作
		resp, err = s.applyV3Base.Range(ctx, nil, r)
	}
	if serr := s.doSerialize(ctx, chk, get); serr != nil {
		err = serr
		return nil, err
	}
	return resp, err
}

// Put OK
func (s *EtcdServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	ctx = context.WithValue(ctx, traceutil.StartTimeKey, time.Now())
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{Put: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.PutResponse), nil
}
