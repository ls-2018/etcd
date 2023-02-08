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

import (
	"fmt"
	"sort"
	"strings"
)

// Progress 在leader看来,Progress代表follower的进度.leader维护所有follower的进度,并根据follower的进度向其发送条目.
// NB(tbg).Progress基本上是一个状态机
type Progress struct {
	Match uint64 // 对应Follower节点当前己经成功复制的Entry记录的索引值.
	Next  uint64 // 对应Follower节点下一个待复制的Entry记录的索引值  就是还在飞行中或者还在路上的日志数量(Inflights)
	// State 对应Follower节点的复制状态
	// 当处于StateProbe状态时,leader在每个心跳间隔内最多发送一条复制消息.它也会探测follower的实际进度.
	// 当处于StateReplicate状态时,leader在发送复制消息后,乐观地增加next 索引.这是一个优化后的的状态,用于快速复制日志条目给follower.
	// 当处于StateSnapshot状态时,leader应该已经发送了快照,并停止发送任何复制消息.
	State           StateType
	PendingSnapshot uint64     // 表示Leader节点正在向目标节点发送快照数据.快照的索引值[最大的索引]
	RecentActive    bool       // 从当前Leader节点的角度来看,该Progress实例对应的Follower节点是否存活.如果新一轮的选举,那么新的Leader默认为都是不活跃的.
	ProbeSent       bool       // 探测状态时才有用,表示探测消息是否已经发送了,如果发送了就不会再发了,避免不必要的IO.
	Inflights       *Inflights // 维护着向该follower已发送,但未收到确认的消息索引 [环形队列]
	IsLearner       bool       // 该节点是不是learner
}

// MaybeUpdate 更新已收到、下次发送的 日志索引, n:上一次发送出去的最大日志索引号
func (pr *Progress) MaybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n // 对应Follower节点当前己经成功复制的Entry记录的索引值
		updated = true
		// 这个函数就是把ProbeSent设置为false,试问为什么在这个条件下才算是确认收到探测包?
		// 这就要从探测消息说起了,raft可以把日志消息、心跳消息当做探测消息,此处是把日志消息
		// 当做探测消息的处理逻辑.新的日志肯定会让Match更新,只有收到了比Match更大的回复才
		// 能算是这个节点收到了新日志消息,其他的反馈都可以视为过时消息.比如Match=9,新的日志
		// 索引是10,只有收到了>=10的反馈才能确定节点收到了当做探测消息的日志.
		pr.ProbeAcked()
	}
	// 这会发生在什么时候?Next是Leader认为发送给Peer最大的日志索引了,Peer怎么可能会回复一个
	// 比Next更大的日志索引呢?这个其实是在系统初始化的时候亦或是每轮选举完成后,新的Leader
	// 还不知道Leer的接收的最大日志索引,所以此时的Next还是个初识值.
	pr.Next = max(pr.Next, n+1) // 对应Follower节点下一个待复制的Entry记录的索引值
	return updated
}

// OptimisticUpdate 记录下次日志发送的起始位置,n是已发送的最新日志索引
func (pr *Progress) OptimisticUpdate(n uint64) { pr.Next = n + 1 }

// MaybeDecrTo  follower拒绝了rejected索引的日志, matchHint是应该重新调整的日志索引[leader记录的];
// leader是否降低对该节点索引记录     9          6
func (pr *Progress) MaybeDecrTo(rejected, matchHint uint64) bool {
	if pr.State == StateReplicate {
		if rejected <= pr.Match {
			return false
		}
		// 根据前面对MsgApp消息发送过程的分析,处于ProgressStateReplicate状态时,发送MsgApp
		// 消息的同时会直接调用Progress.optimisticUpdate()方法增加Next,这就使得Next可能会
		// 比Match大很多,这里回退Next至Match位置,并在后面重新发送MsgApp消息进行尝试

		// 在复制状态下Leader会发送多个日志信息给Peer再等待Peer的回复,例如:Match+1,Match+2,Match+3,Match+4,
		// 此时如果Match+3丢了,那么Match+4肯定好会被拒绝,此时match应该是Match+2,Next=last+1
		// 应该更合理.但是从peer的角度看,如果收到了Match+2的日志就会给leader一次回复,这个
		// 回复理论上是早于当前这个拒绝消息的,所以当Leader收到Match+4拒绝消息,此时的Match
		// 已经更新到Match+2,如果Peer回复的消息也丢包了Match可能也没有更新.所以Match+1
		// 大概率和last相同,少数情况可能last更好,但是用Match+1做可能更保险一点.

		pr.Next = pr.Match + 1
		return true
	}
	// 源码注释翻译:如果拒绝日志索引不是Next-1,肯定是陈旧消息这是因为非复制状态探测消息一次只
	// 发送一条日志.这句话是什么意思呢,读者需要注意,代码执行到这里说明Progress不是复制状态,
	// 应该是探测状态.为了效率考虑,Leader向Peer发送日志消息一次会带多条日志,比如一个日志消息
	// 会带有10条日志.上面Match+1,Match+2,Match+3,Match+4的例子是为了理解方便假设每个
	// 日志消息一条日志.真实的情况是Message[Match,Match+9],Message[Match+10,Match+15],
	// 一个日志消息如果带有多条日志,Peer拒绝的是其中一条日志.此时用什么判断拒绝索引日志就在刚刚
	// 发送的探测消息中呢?所以探测消息一次只发送一条日志就能做到了,因为这个日志的索引肯定是Next-1.

	// 出现过时的MsgAppResp消息直接忽略
	// 只有拒绝比当前大,才会重新      一般都是当前记录了A给follower发送了A+1,[leader会将Next设为A+2]被拒绝了   此时 rejected=A+1
	if pr.Next-1 != rejected {
		return false
	}

	//   idx        1 2 3 4 5 6 7 8 9
	//              -----------------
	//   term (L)   1 3 3 3 5 5 5 5 5
	//   term (F)   1 1 1 1 2 2
	pr.Next = max(min(rejected, matchHint+1), 1) // 此时Next就设为2 		// 根据Peer的反馈调整Next
	pr.ProbeSent = false                         // Next重置完成,恢复消息发送,并在后面重新发送MsgApp消息
	return true
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

// ProbeAcked 当follower接受了append消息,标志着可以继续向该节点发送消息
func (pr *Progress) ProbeAcked() {
	pr.ProbeSent = false
}

// IsPaused 返回发往该节点的消息是否被限流
// 当一个节点拒绝了最近的MsgApps,目前正在等待快照,或者已经达到MaxInflightMsgs限制时,就会这样做.
// 在正常操作中,这是假的.一个被节流的节点将被减少联系的频率,直到它达到能够再次接受稳定的日志条目的状态.
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case StateProbe: // 每个心跳间隔内最多发送一条复制消息,默认false
		// 探测状态下如果已经发送了探测消息Progress就暂停了,意味着不能再发探测消息了,前一个消息
		// 还没回复呢,如果节点真的不活跃,发再多也没用.
		return pr.ProbeSent
	case StateReplicate: // 消息复制状态
		return pr.Inflights.Full() // 根据队列是否满,判断
	case StateSnapshot:
		// 快照状态Progress就是暂停的,Peer正在复制Leader发送的快照,这个过程是一个相对较大
		// 而且重要的事情,因为所有的日志都是基于某一快照基础上的增量,所以快照不完成其他的都是
		// 徒劳.
		return true
	default:
		panic("未知的状态")
	}
}

// ResetState 重置节点的跟踪状态
func (pr *Progress) ResetState(state StateType) {
	pr.ProbeSent = false // 表示探测消息是否已经发送了,如果发送了就不会再发了,避免不必要的IO.
	pr.PendingSnapshot = 0
	pr.State = state
	pr.Inflights.reset()
}

// BecomeProbe 转变为StateProbe.下一步是重置为Match+1,或者,如果更大的话,重置为待定快照的索引.
// 恢复follower状态,以正常发送消息
func (pr *Progress) BecomeProbe() {
	// 变成探测状态,等发出去的消息响应了,再继续发消息
	if pr.State == StateSnapshot { // 当前状态是发送快照
		// 如果原始状态是快照,说明快照已经被Peer接收了,那么Next=pendingSnapshot+1,
		// 意思就是从快照索引的下一个索引开始发送.
		pendingSnapshot := pr.PendingSnapshot
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else { // follower 链接有问题 、网络有问题
		// 上面的逻辑是Peer接收完快照后再探测一次才能继续发日志,而这里的逻辑是Peer从复制状态转
		// 到探测状态,这在Peer拒绝了日志、日志消息丢失的情况会发生,此时Leader不知道从哪里开始,
		// 倒不如从Match+1开始,因为Match是节点明确已经收到的.
		pr.Next = pr.Match + 1 //  match  ---> next的数据丢弃, 下次重新发送
	}
	pr.ResetState(StateProbe)
}

func (pr *Progress) BecomeReplicate() {
	// 除了复位一下状态就是调整Next,为什么Next也是Match+1?进入复制状态肯定是收到了探测消息的
	// 反馈,此时Match会被更新,那从Match+1也就理所当然了
	pr.ResetState(StateReplicate)
	pr.Next = pr.Match + 1
}

// BecomeSnapshot 正在发送快照snapshoti 为快照的最新日志索引
func (pr *Progress) BecomeSnapshot(snapshoti uint64) {
	// 除了复位一下状态就是设置快照的索引,此处为什么不需要调整Next?因为这个状态无需在发日志给
	// peer,直到快照完成后才能继续
	pr.ResetState(StateSnapshot)
	pr.PendingSnapshot = snapshoti
}

func (pr *Progress) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%s match=%d next=%d", pr.State, pr.Match, pr.Next)
	if pr.IsLearner {
		fmt.Fprint(&buf, " learner")
	}
	if pr.IsPaused() {
		fmt.Fprint(&buf, " paused")
	}
	if pr.PendingSnapshot > 0 {
		fmt.Fprintf(&buf, " 发送中的快照=%d", pr.PendingSnapshot)
	}
	if !pr.RecentActive {
		fmt.Fprintf(&buf, " 不活跃的")
	}
	if n := pr.Inflights.Count(); n > 0 {
		fmt.Fprintf(&buf, " 未确认的消息=%d", n)
		if pr.Inflights.Full() {
			fmt.Fprint(&buf, "[full]")
		}
	}
	return buf.String()
}

type ProgressMap map[uint64]*Progress

func (m ProgressMap) String() string {
	ids := make([]uint64, 0, len(m))
	for k := range m {
		ids = append(ids, k)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	var buf strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&buf, "%d: %s\n", id, m[id])
	}
	return buf.String()
}
