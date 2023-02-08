// Copyright 2021 The etcd Authors
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

package membership

import (
	"encoding/json"
	"log"

	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/buckets"
	"github.com/ls-2018/etcd_cn/raft/raftpb"
	"go.uber.org/zap"
)

var confStateKey = []byte("confState")

// MustUnsafeSaveConfStateToBackend confstate ---> bolt.db/meta/confState
func MustUnsafeSaveConfStateToBackend(lg *zap.Logger, tx backend.BatchTx, confState *raftpb.ConfState) {
	confStateBytes, err := json.Marshal(confState)
	if err != nil {
		lg.Panic("不能序列化raftpb.ConfState", zap.Stringer("conf-state", confState), zap.Error(err))
	}

	tx.UnsafePut(buckets.Meta, confStateKey, confStateBytes)
}

// UnsafeConfStateFromBackend confstate <--- bolt.db/meta/confState
func UnsafeConfStateFromBackend(lg *zap.Logger, tx backend.ReadTx) *raftpb.ConfState {
	keys, vals := tx.UnsafeRange(buckets.Meta, confStateKey, nil, 0)
	if len(keys) == 0 {
		return nil
	}

	if len(keys) != 1 {
		lg.Panic("不期待的key: "+string(confStateKey)+" 当从bolt获取集群版本", zap.Int("number-of-key", len(keys)))
	}
	var confState raftpb.ConfState
	if err := json.Unmarshal(vals[0], &confState); err != nil {
		log.Panic("从bolt.db获取到的值无法反序列化",
			zap.ByteString("conf-state-json", []byte(vals[0])),
			zap.Error(err))
	}
	return &confState
}
