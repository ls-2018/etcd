// Copyright 2016 The etcd Authors
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

package concurrency

import (
	"context"
	"errors"
	"fmt"

	v3 "github.com/ls-2018/etcd_cn/client_sdk/v3"
	"github.com/ls-2018/etcd_cn/offical/api/v3/mvccpb"
	pb "github.com/ls-2018/etcd_cn/offical/etcdserverpb"
)

var (
	ErrElectionNotLeader = errors.New("election: not leader")
	ErrElectionNoLeader  = errors.New("election: no leader")
)

type Election struct {
	session       *Session
	keyPrefix     string
	leaderKey     string
	leaderRev     int64
	leaderSession *Session
	hdr           *pb.ResponseHeader
}

// NewElection 返回给定关键字前缀上的新选举结果.
func NewElection(s *Session, pfx string) *Election {
	return &Election{session: s, keyPrefix: pfx + "/"}
}

// ResumeElection initializes an election with a known leader.
func ResumeElection(s *Session, pfx string, leaderKey string, leaderRev int64) *Election {
	return &Election{
		keyPrefix:     pfx,
		session:       s,
		leaderKey:     leaderKey,
		leaderRev:     leaderRev,
		leaderSession: s,
	}
}

// Campaign 在前缀键上放置一个符合选举条件的值.
// 对于同一个前缀,多个会议可以参与选举,但一次只能有一个领导人.
// 如果context是'context. todo ()/context. background ()', Campaign将继续被阻塞,以便其他key被删除,除非etcd返回一个不可恢复的错误(例如ErrCompacted).
// 否则,直到上下文没有被取消或超时,Campaign将继续被阻塞,直到它成为leader.
func (e *Election) Campaign(ctx context.Context, val string) error {
	s := e.session
	client := e.session.Client()

	k := fmt.Sprintf("%s%x", e.keyPrefix, s.Lease())
	txn := client.Txn(ctx).If(v3.Compare(v3.CreateRevision(k), "=", 0))
	txn = txn.Then(v3.OpPut(k, val, v3.WithLease(s.Lease())))
	txn = txn.Else(v3.OpGet(k))
	resp, err := txn.Commit()
	if err != nil {
		return err
	}
	e.leaderKey, e.leaderRev, e.leaderSession = k, resp.Header.Revision, s
	if !resp.Succeeded {
		kv := resp.Responses[0].GetResponseRange().Kvs[0]
		e.leaderRev = kv.CreateRevision
		if kv.Value != val {
			if err = e.Proclaim(ctx, val); err != nil {
				e.Resign(ctx)
				return err
			}
		}
	}

	_, err = waitDeletes(ctx, client, e.keyPrefix, e.leaderRev-1)
	if err != nil {
		// 在上下文取消的情况下清理
		select {
		case <-ctx.Done():
			e.Resign(client.Ctx())
		default:
			e.leaderSession = nil
		}
		return err
	}
	e.hdr = resp.Header

	return nil
}

// Proclaim  让leader宣布一个新的值,而不需要一次选举.
func (e *Election) Proclaim(ctx context.Context, val string) error {
	if e.leaderSession == nil {
		return ErrElectionNotLeader
	}
	client := e.session.Client()
	cmp := v3.Compare(v3.CreateRevision(e.leaderKey), "=", e.leaderRev)
	txn := client.Txn(ctx).If(cmp)
	txn = txn.Then(v3.OpPut(e.leaderKey, val, v3.WithLease(e.leaderSession.Lease())))
	tresp, terr := txn.Commit()
	if terr != nil {
		return terr
	}
	if !tresp.Succeeded {
		e.leaderKey = ""
		return ErrElectionNotLeader
	}

	e.hdr = tresp.Header
	return nil
}

// Resign lets a leader start a new election.
func (e *Election) Resign(ctx context.Context) (err error) {
	if e.leaderSession == nil {
		return nil
	}
	client := e.session.Client()
	cmp := v3.Compare(v3.CreateRevision(e.leaderKey), "=", e.leaderRev)
	resp, err := client.Txn(ctx).If(cmp).Then(v3.OpDelete(e.leaderKey)).Commit()
	if err == nil {
		e.hdr = resp.Header
	}
	e.leaderKey = ""
	e.leaderSession = nil
	return err
}

// Leader returns the leader value for the current election.
func (e *Election) Leader(ctx context.Context) (*v3.GetResponse, error) {
	client := e.session.Client()
	resp, err := client.Get(ctx, e.keyPrefix, v3.WithFirstCreate()...)
	if err != nil {
		return nil, err
	} else if len(resp.Kvs) == 0 {
		// no leader currently elected
		return nil, ErrElectionNoLeader
	}
	return resp, nil
}

// Observe 返回一个通道,该通道可靠地观察有序的leader proposal 作为响应
func (e *Election) Observe(ctx context.Context) <-chan v3.GetResponse {
	retc := make(chan v3.GetResponse)
	go e.observe(ctx, retc)
	return retc
}

// 观察 节点变更
func (e *Election) observe(ctx context.Context, ch chan<- v3.GetResponse) {
	client := e.session.Client()

	defer close(ch)
	for {
		resp, err := client.Get(ctx, e.keyPrefix, v3.WithFirstCreate()...)
		if err != nil {
			return
		}

		var kv *mvccpb.KeyValue
		var hdr *pb.ResponseHeader

		if len(resp.Kvs) == 0 {
			cctx, cancel := context.WithCancel(ctx)
			// 等待在这个前缀更新第一个值wait for first key put on prefix
			opts := []v3.OpOption{v3.WithRev(resp.Header.Revision), v3.WithPrefix()}
			wch := client.Watch(cctx, e.keyPrefix, opts...)
			for kv == nil {
				wr, ok := <-wch
				if !ok || wr.Err() != nil {
					cancel()
					return
				}
				// only accept puts; a delete will make observe() spin
				// 只接受put;删除操作将使observe()重试
				for _, ev := range wr.Events {
					if ev.Type == mvccpb.PUT {
						hdr, kv = &wr.Header, ev.Kv
						// may have multiple revs; hdr.rev = the last rev
						// set to kv's rev in case batch has multiple Puts
						hdr.Revision = kv.ModRevision
						break
					}
				}
			}
			cancel()
		} else {
			hdr, kv = resp.Header, resp.Kvs[0]
		}

		select {
		case ch <- v3.GetResponse{Header: hdr, Kvs: []*mvccpb.KeyValue{kv}}:
		case <-ctx.Done():
			return
		}

		cctx, cancel := context.WithCancel(ctx)
		wch := client.Watch(cctx, kv.Key, v3.WithRev(hdr.Revision+1))
		keyDeleted := false
		for !keyDeleted {
			wr, ok := <-wch
			if !ok {
				cancel()
				return
			}
			for _, ev := range wr.Events {
				if ev.Type == mvccpb.DELETE {
					keyDeleted = true
					break
				}
				resp.Header = &wr.Header
				resp.Kvs = []*mvccpb.KeyValue{ev.Kv}
				select {
				case ch <- *resp:
				case <-cctx.Done():
					cancel()
					return
				}
			}
		}
		cancel()
	}
}

func (e *Election) Key() string { return e.leaderKey }

func (e *Election) Rev() int64 { return e.leaderRev }

func (e *Election) Header() *pb.ResponseHeader { return e.hdr }
