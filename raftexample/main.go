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
	"flag"
	"strings"

	"github.com/ls-2018/etcd_cn/raft/raftpb"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "http 服务器   key-value etcd port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeC := make(chan string) // 提议通道, ---->放入raft状态机,返回错误
	defer close(proposeC)
	triggerConfChangeC := make(chan raftpb.ConfChangeV1) // 配置通道
	defer close(triggerConfChangeC)

	// Raft为来自HTTP API的propose提供了commitC
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.genSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(*id, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, triggerConfChangeC)

	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	// kv http处理程序将propose 更新到raft上
	serveHttpKVAPI(kvs, *kvport, triggerConfChangeC, errorC)
}

/*
 	--->	proposeC  --->
app <---	commitC	  <---  raft
 	<---	errorC    <---
*/
