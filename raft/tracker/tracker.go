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

	"github.com/ls-2018/etcd_cn/raft/quorum"
	pb "github.com/ls-2018/etcd_cn/raft/raftpb"
)

type Config struct {
	Voters quorum.JointConfig // 投票者,分为两个阶段   【新的配置、 旧的配置】或  【配置、 nil】
	// AutoLeave 如果配置是joint的,并且在可能的情况下,应该由Raft自动进行到传递配置的过渡,则为true.
	// 如果为false,则该配置将被连接,直到应用程序手动启动转换.
	AutoLeave bool
	Learners  map[uint64]struct{} // Learners 当前配置中的learner ID
	// 当我们在联合共识转换过程中把voter变成learner时,我们不能在进入联合状态时直接增加learner.
	// 这是因为这将违反voter和learner的交集是空的这一不变性.例如,假设一个voter被移除,并立即重新添加为learner
	// (或者换句话说,它被降级).
	//
	// 最初,配置将是
	//   voters:   {1 2 3}
	//   learners: {}
	//
	// 而我们想降级3.进入联合配置,我们天真地认为
	//   voters:   {1 2} & {1 2 3}
	//   learners: {3}
	//
	// 但这违反了不变量(3既是投票者又是learner).相反,我们得到
	//   voters:   {1 2} & {1 2 3}
	//   learners: {}
	//   next_learners: {3}
	//
	// 其中3号现在还是纯粹的投票者,但我们记住了在过渡到最终配置时使其成为learner的意图.
	//   voters:   {1 2}
	//   learners: {3}
	//   next_learners: {}
	//
	// 请注意,在添加一个不属于joint config中投票人的learner时,不使用next_learners.
	// 在这种情况下,learners在进入联合配置时被立即添加,以便尽快赶上.
	LearnersNext map[uint64]struct{}
}

func (c Config) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "voters=%s", c.Voters)
	if c.Learners != nil {
		fmt.Fprintf(&buf, " learners=%s", quorum.MajorityConfig(c.Learners).String())
	}
	if c.LearnersNext != nil {
		fmt.Fprintf(&buf, " learners_next=%s", quorum.MajorityConfig(c.LearnersNext).String())
	}
	if c.AutoLeave {
		fmt.Fprintf(&buf, " autoleave")
	}
	return buf.String()
}

func (c *Config) Clone() Config {
	clone := func(m map[uint64]struct{}) map[uint64]struct{} {
		if m == nil {
			return nil
		}
		mm := make(map[uint64]struct{}, len(m))
		for k := range m {
			mm[k] = struct{}{}
		}
		return mm
	}
	return Config{
		Voters:       quorum.JointConfig{clone(c.Voters[0]), clone(c.Voters[1])},
		Learners:     clone(c.Learners),
		LearnersNext: clone(c.LearnersNext),
	}
}

// ProgressTracker 追踪配置以及节点信息.
type ProgressTracker struct {
	Config
	// leader需要缓存当前所有Follower的日志同步进度
	Progress    ProgressMap     // nodeID ---> nodeInfoMap
	Votes       map[uint64]bool // 记录接收到了哪些节点的投票
	MaxInflight int
}

// MakeProgressTracker 初始化
func MakeProgressTracker(maxInflight int) ProgressTracker {
	p := ProgressTracker{
		MaxInflight: maxInflight, // 最大的处理中的消息数量
		Config: Config{
			Voters: quorum.JointConfig{
				quorum.MajorityConfig{}, // 只初始化了第一个
				nil,                     // 使用时初始化
			},
			Learners:     nil, // 使用时初始化
			LearnersNext: nil, // 使用时初始化
		},
		Votes:    map[uint64]bool{},
		Progress: map[uint64]*Progress{},
	}
	return p
}

// ConfState 返回一个代表active配置的ConfState.
func (p *ProgressTracker) ConfState() pb.ConfState {
	return pb.ConfState{
		Voters:         p.Voters[0].Slice(),
		VotersOutgoing: p.Voters[1].Slice(),
		Learners:       quorum.MajorityConfig(p.Learners).Slice(),
		LearnersNext:   quorum.MajorityConfig(p.LearnersNext).Slice(),
		AutoLeave:      p.AutoLeave,
	}
}

// IsSingleton 集群中只有一个投票成员(领导者).
func (p *ProgressTracker) IsSingleton() bool {
	return len(p.Voters[0]) == 1 && len(p.Voters[1]) == 0
}

// QuorumActive 如果从本地raft状态机的角度来看,该法定人数是活动的,则返回true.否则,它将返回false.
func (p *ProgressTracker) QuorumActive() bool {
	votes := map[uint64]bool{}
	p.Visit(func(id uint64, pr *Progress) {
		if pr.IsLearner {
			return
		}
		votes[id] = pr.RecentActive
	})

	return p.Voters.VoteResult(votes) == quorum.VoteWon
}

type matchAckIndexer map[uint64]*Progress

var _ quorum.AckedIndexer = matchAckIndexer(nil)

// AckedIndex 返回指定ID的Peer接收的最大日志索引,就是Progress.Match.
func (l matchAckIndexer) AckedIndex(id uint64) (quorum.Index, bool) {
	pr, ok := l[id]
	if !ok {
		return 0, false
	}
	return quorum.Index(pr.Match), true
}

// Committed 根据投票成员已确认的 返回已提交的最大日志索引.
func (p *ProgressTracker) Committed() uint64 {
	return uint64(p.Voters.CommittedIndex(matchAckIndexer(p.Progress)))
}

func insertionSort(sl []uint64) {
	a, b := 0, len(sl)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && sl[j] < sl[j-1]; j-- {
			sl[j], sl[j-1] = sl[j-1], sl[j]
		}
	}
}

// Visit 对所有跟踪的进度按稳定的顺序调用所提供的闭包.
func (p *ProgressTracker) Visit(f func(id uint64, pr *Progress)) {
	n := len(p.Progress)
	var sl [7]uint64
	var ids []uint64
	if len(sl) >= n {
		ids = sl[:n]
	} else {
		ids = make([]uint64, n)
	}
	for id := range p.Progress {
		n--
		ids[n] = id
	}
	insertionSort(ids)
	for _, id := range ids {
		f(id, p.Progress[id])
	}
}

// VoterNodes 返回一个经过排序的选民
func (p *ProgressTracker) VoterNodes() []uint64 {
	m := p.Voters.IDs()
	nodes := make([]uint64, 0, len(m))
	for id := range m {
		nodes = append(nodes, id)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes
}

// LearnerNodes 返回所有的learners
func (p *ProgressTracker) LearnerNodes() []uint64 {
	if len(p.Learners) == 0 {
		return nil
	}
	nodes := make([]uint64, 0, len(p.Learners))
	for id := range p.Learners {
		nodes = append(nodes, id)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes
}

// ResetVotes 准备通过recordVote进行新一轮的计票工作.
func (p *ProgressTracker) ResetVotes() {
	p.Votes = map[uint64]bool{} // 记录接收到了哪些节点的投票
}

// RecordVote id=true, 该节点给本节点投了票; id=false, 该节点没有给本节点投票
func (p *ProgressTracker) RecordVote(id uint64, v bool) {
	_, ok := p.Votes[id] // 记录接收到了哪些节点的投票
	if !ok {
		p.Votes[id] = v // 记录接收到了哪些节点的投票
	}
}

// TallyVotes 返回批准和拒绝的票数,以及是否知道选举结果.
func (p *ProgressTracker) TallyVotes() (granted int, rejected int, _ quorum.VoteResult) {
	for id, pr := range p.Progress {
		if pr.IsLearner {
			continue
		}
		v, voted := p.Votes[id] // 记录接收到了哪些节点的投票
		if !voted {             // 没有收到结果
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}
	result := p.Voters.VoteResult(p.Votes)
	return granted, rejected, result
}
