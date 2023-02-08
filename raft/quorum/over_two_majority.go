// Copyright 2019 The etcd Authors
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

package quorum

// JointConfig JointConfig和MajorityConfig功能是一样的,只是JointConfig的做法是
// 根据两个MajorityConfig的结果做一次融合性操作
// MakeProgressTracker 初始化
// case   [map[id]=true,map] 集群中只有一个投票成员(领导者).
// [变更节点集合,老节点集合] 或 [节点、nil]
type JointConfig [2]MajorityConfig

func (c JointConfig) String() string {
	if len(c[1]) > 0 {
		return c[0].String() + "&&" + c[1].String()
	}
	return c[0].String()
}

func (c JointConfig) IDs() map[uint64]struct{} {
	m := map[uint64]struct{}{}
	for _, cc := range c {
		for id := range cc {
			m[id] = struct{}{}
		}
	}
	return m
}

func (c JointConfig) Describe(l AckedIndexer) string {
	return MajorityConfig(c.IDs()).Describe(l)
}

// CommittedIndex 已提交索引
func (c JointConfig) CommittedIndex(l AckedIndexer) Index {
	// 返回的是二者最小的那个,这时候可以理解MajorityConfig.CommittedIndex()为什么Peers数
	// 为0的时候返回无穷大了吧,如果返回0该函数就永远返回0了.
	idx0 := c[0].CommittedIndex(l)
	idx1 := c[1].CommittedIndex(l)
	if idx0 < idx1 {
		return idx0
	}
	return idx1
}

func (c JointConfig) VoteResult(votes map[uint64]bool) VoteResult {
	r1 := c[0].VoteResult(votes)
	r2 := c[1].VoteResult(votes)
	// 相同的,下里面的判断逻辑基就可以知道MajorityConfig.VoteResult()在peers数为0返回选举
	// 胜利的原因.
	if r1 == r2 {
		return r1
	}
	if r1 == VoteLost || r2 == VoteLost {
		return VoteLost
	}
	return VotePending
}
