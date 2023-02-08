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

// Package mirror implements etcd mirroring operations.
package mirror

import (
	"context"

	clientv3 "github.com/ls-2018/etcd_cn/client_sdk/v3"
)

const (
	batchLimit = 1000
)

type Syncer interface {
	SyncBase(ctx context.Context) (<-chan clientv3.GetResponse, chan error) // 同步k-v 状态.通过返回的chan发送.
	SyncUpdates(ctx context.Context) clientv3.WatchChan                     // 在同步base数据后,同步增量数据
}

// NewSyncer 同步器
func NewSyncer(c *clientv3.Client, prefix string, rev int64) Syncer {
	return &syncer{c: c, prefix: prefix, rev: rev}
}

type syncer struct {
	c      *clientv3.Client
	rev    int64
	prefix string
}

func (s *syncer) SyncBase(ctx context.Context) (<-chan clientv3.GetResponse, chan error) {
	respchan := make(chan clientv3.GetResponse, 1024)
	errchan := make(chan error, 1)

	// 如果没有指定rev,我们将选择最近的修订.
	if s.rev == 0 {
		resp, err := s.c.Get(ctx, "foo")
		if err != nil {
			errchan <- err
			close(respchan)
			close(errchan)
			return respchan, errchan
		}
		s.rev = resp.Header.Revision
	}

	go func() {
		defer close(respchan)
		defer close(errchan)

		var key string

		opts := []clientv3.OpOption{clientv3.WithLimit(batchLimit), clientv3.WithRev(s.rev)}

		if len(s.prefix) == 0 {
			// 同步所有kv
			opts = append(opts, clientv3.WithFromKey())
			key = "\x00"
		} else {
			opts = append(opts, clientv3.WithRange(clientv3.GetPrefixRangeEnd(s.prefix)))
			key = s.prefix
		}

		for {
			resp, err := s.c.Get(ctx, key, opts...)
			if err != nil {
				errchan <- err
				return
			}

			respchan <- *resp

			if !resp.More {
				return
			}
			// move to next key
			key = string(append([]byte(resp.Kvs[len(resp.Kvs)-1].Key), 0))
		}
	}()

	return respchan, errchan
}

func (s *syncer) SyncUpdates(ctx context.Context) clientv3.WatchChan {
	if s.rev == 0 {
		panic("unexpected revision = 0. Calling SyncUpdates before SyncBase finishes?")
	}
	return s.c.Watch(ctx, s.prefix, clientv3.WithPrefix(), clientv3.WithRev(s.rev+1))
}
