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

package main

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/snap"
	"github.com/ls-2018/etcd_cn/raft/raftpb"
)

// 基于raft的k,v存储
type kvstore struct {
	proposeC    chan<- string // 提议通知channel
	mu          sync.RWMutex
	kvStore     map[string]string // 当前提交的键值对
	snapshotter *snap.Snapshotter // 快照管理器
}

type kv struct {
	Key string `json:"key"`
	Val string `json:"val"`
}

func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) *kvstore {
	s := &kvstore{
		proposeC:    proposeC,
		kvStore:     make(map[string]string),
		snapshotter: snapshotter,
	}
	snapshot, err := s.loadSnapshot() // 加载最新的快照
	if err != nil {
		log.Panic(err)
	}
	if snapshot != nil {
		log.Printf("开始加载快照 at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			log.Panic(err)
		}
	}
	// 从raft读取提交到kvStore映射,直到错误
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.kvStore[key]
	return v, ok
}

// Propose 处理客户端提交的的数据    put k,v
func (s *kvstore) Propose(k string, v string) {
	marshal, _ := json.Marshal(kv{k, v})
	s.proposeC <- string(marshal)
}

// 客户端读取raft已经committed的数据
func (s *kvstore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		if commit == nil {
			// 信号加载快照
			snapshot, err := s.loadSnapshot()
			if err != nil {
				log.Panic(err)
			}
			if snapshot != nil {
				log.Printf("正在加载快照at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
			}
			continue
		}

		for _, data := range commit.data {
			var dataKv kv
			err := json.Unmarshal([]byte(data), &dataKv)
			if err != nil {
				log.Fatalf("raftexample:不能解析数据(%v)", err)
			}

			s.mu.Lock()
			s.kvStore[dataKv.Key] = dataKv.Val
			s.mu.Unlock()
		}
		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

// 生成一个快照
func (s *kvstore) genSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.kvStore)
}

func (s *kvstore) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

// 恢复快照数据,启动时
func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kvStore = store
	return nil
}
