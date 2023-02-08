package etcdserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/ls-2018/etcd_cn/pkg/traceutil"
	"github.com/ls-2018/etcd_cn/raft"
	"go.uber.org/zap"
)

type notifier struct {
	c   chan struct{}
	err error
}

// 通知
func newNotifier() *notifier {
	return &notifier{
		c: make(chan struct{}),
	}
}

func (nc *notifier) notify(err error) {
	nc.err = err
	close(nc.c)
}

// 线性一致性读,保证强一致性  , 阻塞,直到applyid >= 当前生成的ID
func (s *EtcdServer) linearizableReadLoop() {
	for {
		requestId := s.reqIDGen.Next()
		leaderChangedNotifier := s.LeaderChangedNotify()
		select {
		case <-leaderChangedNotifier:
			continue
		case <-s.readwaitc:
			// 在client发起一次Linearizable Read的时候,会向readwaitc写入一个空的结构体作为信号
			fmt.Println("开始一次linearizableRead")
		case <-s.stopping:
			return
		}

		// 因为一个循环可以解锁多个读数所以从Txn或Range传播追踪不是很有用.
		trace := traceutil.New("linearizableReadLoop", s.Logger())

		s.readMu.Lock()
		nr := s.readNotifier
		s.readNotifier = newNotifier()
		s.readMu.Unlock()
		// 处理不同的消息
		// 这里会监听 readwaitc,发送MsgReadIndex 并等待 MsgReadIndexRsp
		// 同时获取当前已提交的日志索引
		// 串行执行的
		confirmedIndex, err := s.requestCurrentIndex(leaderChangedNotifier, requestId) // MsgReadIndex 携带requestId经过raft走一圈
		if isStopped(err) {
			return
		}
		if err != nil {
			nr.notify(err)
			continue
		}

		trace.Step("收到要读的索引")
		trace.AddField(traceutil.Field{Key: "readStateIndex", Value: confirmedIndex})
		appliedIndex := s.getAppliedIndex()
		trace.AddField(traceutil.Field{Key: "appliedIndex", Value: strconv.FormatUint(appliedIndex, 10)})
		// 此处是重点 等待 apply index >= read index
		if appliedIndex < confirmedIndex {
			select {
			case <-s.applyWait.Wait(confirmedIndex):
			case <-s.stopping:
				return
			}
		}
		// 发出可以进行读取状态机的信号
		nr.notify(nil)
		trace.Step("applied 索引现在低于 readState.Index")
	}
}

// 请求当前索引
func (s *EtcdServer) requestCurrentIndex(leaderChangedNotifier <-chan struct{}, requestId uint64) (uint64, error) {
	err := s.sendReadIndex(requestId) // 线性读生成的7587861540711705347  就是异步发送一条raft的消息
	if err != nil {
		return 0, err
	}

	lg := s.Logger()
	errorTimer := time.NewTimer(s.Cfg.ReqTimeout())
	defer errorTimer.Stop()
	retryTimer := time.NewTimer(readIndexRetryTime) // 500ms
	defer retryTimer.Stop()

	firstCommitInTermNotifier := s.FirstCommitInTermNotify()

	for {
		select {
		case rs := <-s.r.readStateC: //  err := s.sendReadIndex(requestId) 经由raft会往这里发一个信号
			requestIdBytes := uint64ToBigEndianBytes(requestId)
			gotOwnResponse := bytes.Equal(rs.RequestCtx, requestIdBytes)
			// rs.RequestCtx<requestIdBytes 可能是前一次请求的响应刚到这里
			// rs.RequestCtx>requestIdBytes 可能是高并发情景下,下一次get请求导致的
			if !gotOwnResponse {
				// 前一个请求可能超时.现在我们应该忽略它的响应,继续等待当前请求的响应.
				responseId := uint64(0)
				if len(rs.RequestCtx) == 8 {
					responseId = binary.BigEndian.Uint64(rs.RequestCtx)
				}
				lg.Warn(
					"忽略过期的读索引响应;本地节点读取索引排队等待后端与leader同步",
					zap.Uint64("sent-request-id", requestId),
					zap.Uint64("received-request-id", responseId),
				)
				continue
			}
			return rs.Index, nil // 返回的是leader已经committed的索引
		case <-leaderChangedNotifier:
			return 0, ErrLeaderChanged
		case <-firstCommitInTermNotifier:
			firstCommitInTermNotifier = s.FirstCommitInTermNotify()
			lg.Info("第一次提交:重发ReadIndex请求")
			err := s.sendReadIndex(requestId)
			if err != nil {
				return 0, err
			}
			retryTimer.Reset(readIndexRetryTime)
			continue
		case <-retryTimer.C:
			lg.Warn("等待ReadIndex响应时间过长,需要重新尝试", zap.Uint64("sent-request-id", requestId), zap.Duration("retry-timeout", readIndexRetryTime))
			err := s.sendReadIndex(requestId)
			if err != nil {
				return 0, err
			}
			retryTimer.Reset(readIndexRetryTime)
			continue
		case <-errorTimer.C:
			lg.Warn("等待读索引响应时超时(本地节点可能有较慢的网络)", zap.Duration("timeout", s.Cfg.ReqTimeout()))
			return 0, ErrTimeout
		case <-s.stopping:
			return 0, ErrStopped
		}
	}
}

// etcdctl get  就是异步发送一条raft的消息
func (s *EtcdServer) sendReadIndex(requestIndex uint64) error {
	ctxToSend := uint64ToBigEndianBytes(requestIndex)

	cctx, cancel := context.WithTimeout(context.Background(), s.Cfg.ReqTimeout())
	// 就是异步发送一条raft的消息
	err := s.r.ReadIndex(cctx, ctxToSend) // 发出去就完事了,  发到内存里
	cancel()
	if err == raft.ErrStopped {
		return err
	}
	if err != nil {
		lg := s.Logger()
		lg.Warn("未能从Raft获取读取索引", zap.Error(err))
		return err
	}
	return nil
}

// 进行一次 线性读取准备
func (s *EtcdServer) linearizeReadNotify(ctx context.Context) error {
	s.readMu.RLock()
	nc := s.readNotifier
	s.readMu.RUnlock()

	select {
	case s.readwaitc <- struct{}{}: // linearizableReadLoop就会开始结束阻塞开始工作
	default:
	}

	// 等待读状态通知
	select {
	case <-nc.c:
		return nc.err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrStopped
	}
}
