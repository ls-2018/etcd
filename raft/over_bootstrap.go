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

package raft

import (
	"errors"

	pb "github.com/ls-2018/etcd_cn/raft/raftpb"
)

// Bootstrap 引导集群, 将集群信息加载到 memoryStorage
func (rn *RawNode) Bootstrap(peers []Peer) error {
	//  [{"id":10276657743932975437,"peerURLs":["http://localhost:2380"],"name":"default"}]
	if len(peers) == 0 {
		return errors.New("必须提供至少一个peer")
	}
	lastIndex, err := rn.raft.raftLog.storage.LastIndex() // 内存中最新索引  0
	if err != nil {
		return err
	}

	if lastIndex != 0 {
		return errors.New("不能引导一个非空的存储空间")
	}
	rn.prevHardSt = emptyState

	rn.raft.becomeFollower(1, None)
	ents := make([]pb.Entry, len(peers))
	for i, peer := range peers {
		cc := pb.ConfChangeV1{Type: pb.ConfChangeAddNode, NodeID: peer.ID, Context: string(peer.Context)}
		data, err := cc.Marshal()
		if err != nil {
			return err
		}

		ents[i] = pb.Entry{Type: pb.EntryConfChange, Term: 1, Index: uint64(i + 1), Data: data} // ok
	}
	rn.raft.raftLog.append(ents...) // 有多少个节点就记录多少个日志项
	rn.raft.raftLog.committed = uint64(len(ents))
	for _, peer := range peers {
		rn.raft.applyConfChange(pb.ConfChangeV1{NodeID: peer.ID, Type: pb.ConfChangeAddNode}.AsV2())
	}
	return nil
}
