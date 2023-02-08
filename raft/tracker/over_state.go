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

package tracker

// StateType follower状态追踪
type StateType uint64

const (
	// StateProbe 一般是系统选举完成后,Leader不知道所有Follower都是什么进度,所以需要发消息探测一下,从
	// Follower的回复消息获取进度.在还没有收到回消息前都还是探测状态.因为不确定Follower是否活跃,
	// 所以发送太多的探测消息意义不大,只发送一个探测消息即可.
	StateProbe StateType = iota // 探测
	// StateReplicate :当Peer回复探测消息后,消息中有该节点接收的最大日志索引,如果回复的最大索引大于Match,   【可能会出现日志冲突】
	//    以此索引更新Match,Progress就进入了复制状态,开启高速复制模式.复制制状态不同于
	//    探测状态,Leader会发送更多的日志消息来提升IO效率,就是上面提到的异步发送.这里就要引入
	//    Inflight概念了,飞行中的日志,意思就是已经发送给Follower还没有被确认接收的日志数据,
	StateReplicate // 复制
	StateSnapshot  // 快照状态说明Follower正在复制Leader的快照
)

var prstmap = [...]string{
	"StateProbe",
	"StateReplicate",
	"StateSnapshot",
}

func (st StateType) String() string { return prstmap[uint64(st)] }
