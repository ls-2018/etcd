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

package v2store

import (
	"sort"
	"time"

	"github.com/jonboulle/clockwork"
)

var _ node

// NodeExtern 是内部节点的外部表示,带有附加字段 PrevValue是节点的前一个值 TTL是生存时间,以秒为单位
type NodeExtern struct {
	Key           string      `json:"key,omitempty"` // /0/members/8e9e05c52164694d/raftAttributes
	Value         *string     `json:"value,omitempty"`
	Dir           bool        `json:"dir,omitempty"`
	Expiration    *time.Time  `json:"expiration,omitempty"`
	TTL           int64       `json:"ttl,omitempty"`
	ExternNodes   NodeExterns `json:"nodes,omitempty"`
	ModifiedIndex uint64      `json:"modifiedIndex,omitempty"`
	CreatedIndex  uint64      `json:"createdIndex,omitempty"`
}

//  &v2store.NodeExtern{Key: "/1234", ExternNodes: []*v2store.NodeExtern{
//		{Key: "/1234/attributes", Value: stringp(`{"name":"node1","clientURLs":null}`)},
//		{Key: "/1234/raftAttributes", Value: stringp(`{"peerURLs":null}`)},
//	}}

// 加载node,主要是获取node中数据   n: /0/members
func (eNode *NodeExtern) loadInternalNode(n *node, recursive, sorted bool, clock clockwork.Clock) {
	if n.IsDir() {
		eNode.Dir = true
		children, _ := n.List()
		eNode.ExternNodes = make(NodeExterns, len(children))
		// 我们不直接使用子片中的索引,我们需要跳过隐藏的node.
		i := 0
		for _, child := range children {
			if child.IsHidden() {
				continue
			}
			eNode.ExternNodes[i] = child.Repr(recursive, sorted, clock)
			i++
		}
		// 消除隐藏节点
		eNode.ExternNodes = eNode.ExternNodes[:i]
		if sorted {
			sort.Sort(eNode.ExternNodes)
		}
	} else {
		value, _ := n.Read()
		eNode.Value = &value
	}
	eNode.Expiration, eNode.TTL = n.expirationAndTTL(clock) // 过期时间和 剩余时间 TTL
}

func (eNode *NodeExtern) Clone() *NodeExtern {
	if eNode == nil {
		return nil
	}
	nn := &NodeExtern{
		Key:           eNode.Key,
		Dir:           eNode.Dir,
		TTL:           eNode.TTL,
		ModifiedIndex: eNode.ModifiedIndex,
		CreatedIndex:  eNode.CreatedIndex,
	}
	if eNode.Value != nil {
		s := *eNode.Value
		nn.Value = &s
	}
	if eNode.Expiration != nil {
		t := *eNode.Expiration
		nn.Expiration = &t
	}
	if eNode.ExternNodes != nil {
		nn.ExternNodes = make(NodeExterns, len(eNode.ExternNodes))
		for i, n := range eNode.ExternNodes {
			nn.ExternNodes[i] = n.Clone()
		}
	}
	return nn
}

type NodeExterns []*NodeExtern

// interfaces for sorting

func (ns NodeExterns) Len() int {
	return len(ns)
}

func (ns NodeExterns) Less(i, j int) bool {
	return ns[i].Key < ns[j].Key
}

func (ns NodeExterns) Swap(i, j int) {
	ns[i], ns[j] = ns[j], ns[i]
}
