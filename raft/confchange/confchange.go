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

package confchange

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ls-2018/etcd_cn/raft/quorum"
	pb "github.com/ls-2018/etcd_cn/raft/raftpb"
	"github.com/ls-2018/etcd_cn/raft/tracker"
)

// Changer facilitates configuration changes. It exposes methods to handle
// simple and joint consensus while performing the proper validation that allows
// refusing invalid configuration changes before they affect the active
// configuration.
type Changer struct {
	Tracker   tracker.ProgressTracker
	LastIndex uint64
}

// EnterJoint verifies that the outgoing (=right) majority config of the joint
// config is empty and initializes it with a copy of the incoming (=left)
// majority config. That is, it transitions from
//
//     (1 2 3)&&()
// to
//     (1 2 3)&&(1 2 3).
//
// The supplied changes are then applied to the incoming majority config,
// resulting in a joint configuration that in terms of the Raft thesis[1]
// (Section 4.3) corresponds to `C_{new,old}`.
//
// [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
func (c Changer) EnterJoint(autoLeave bool, ccs ...pb.ConfChangeSingle) (tracker.Config, tracker.ProgressMap, error) {
	cfg, prs, err := c.checkAndCopy()
	if err != nil {
		return c.err(err)
	}
	if joint(cfg) {
		err := errors.New("config is already joint")
		return c.err(err)
	}
	if len(incoming(cfg.Voters)) == 0 {
		// We allow adding nodes to an empty config for convenience (testing and
		// bootstrap), but you can't enter a joint state.
		err := errors.New("can't make a zero-voter config joint")
		return c.err(err)
	}
	// Clear the outgoing config.
	*outgoingPtr(&cfg.Voters) = quorum.MajorityConfig{}
	// Copy incoming to outgoing.
	for id := range incoming(cfg.Voters) {
		outgoing(cfg.Voters)[id] = struct{}{}
	}

	if err := c.apply(&cfg, prs, ccs...); err != nil {
		return c.err(err)
	}
	cfg.AutoLeave = autoLeave
	return checkAndReturn(cfg, prs)
}

// LeaveJoint transitions out of a joint configuration. It is an error to call
// this method if the configuration is not joint, i.e. if the outgoing majority
// config Voters[1] is empty.
//
// The outgoing majority config of the joint configuration will be removed,
// that is, the incoming config is promoted as the sole decision maker. In the
// notation of the Raft thesis[1] (Section 4.3), this method transitions from
// `C_{new,old}` into `C_new`.
//
// At the same time, any staged learners (LearnersNext) the addition of which
// was held back by an overlapping voter in the former outgoing config will be
// inserted into Learners.
//
// [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
func (c Changer) LeaveJoint() (tracker.Config, tracker.ProgressMap, error) {
	cfg, prs, err := c.checkAndCopy()
	if err != nil {
		return c.err(err)
	}
	if !joint(cfg) {
		err := errors.New("can't leave a non-joint config")
		return c.err(err)
	}
	if len(outgoing(cfg.Voters)) == 0 {
		err := fmt.Errorf("configuration is not joint: %v", cfg)
		return c.err(err)
	}
	for id := range cfg.LearnersNext {
		nilAwareAdd(&cfg.Learners, id)
		prs[id].IsLearner = true
	}
	cfg.LearnersNext = nil

	for id := range outgoing(cfg.Voters) {
		_, isVoter := incoming(cfg.Voters)[id]
		_, isLearner := cfg.Learners[id]

		if !isVoter && !isLearner {
			delete(prs, id)
		}
	}
	*outgoingPtr(&cfg.Voters) = nil
	cfg.AutoLeave = false

	return checkAndReturn(cfg, prs)
}

// ------------------------------------  OVER  --------------------------------------------------

// 是不是进入联合共识
func joint(cfg tracker.Config) bool {
	return len(outgoing(cfg.Voters)) > 0
}

// 节点未发生变更时,节点信息存储在JointConfig[0]即incoming的指向的集合中.
// 当EnterJoint时,将老节点拷贝至outgoing中,变更节点拷贝至incoming中.
// LeaveJoint时,删除下线的节点,合并在线的节点并合并至incoming中,完成节点变更过程.
func incoming(voters quorum.JointConfig) quorum.MajorityConfig      { return voters[0] }
func outgoing(voters quorum.JointConfig) quorum.MajorityConfig      { return voters[1] }
func outgoingPtr(voters *quorum.JointConfig) *quorum.MajorityConfig { return &voters[1] }

// Describe 打印配置变更
func Describe(ccs ...pb.ConfChangeSingle) string {
	var buf strings.Builder
	for _, cc := range ccs {
		if buf.Len() > 0 {
			buf.WriteByte(' ')
		}
		fmt.Fprintf(&buf, "%s(%d)", cc.Type, cc.NodeID)
	}
	return buf.String()
}

// checkInvariants 确保配置和进度是相互兼容的.
func checkInvariants(cfg tracker.Config, prs tracker.ProgressMap) error {
	// cfg是深拷贝,prs是浅拷贝'
	for _, ids := range []map[uint64]struct{}{
		cfg.Voters.IDs(), // JointConfig里的所有节点ID
		cfg.Learners,     //
		cfg.LearnersNext, // 即将变成learner的几点
	} {
		for id := range ids {
			if _, ok := prs[id]; !ok {
				return fmt.Errorf("没有该节点的进度信息 %d", id)
			}
		}
	}

	// LearnersNext 不能直接添加,  只能由leader、follower 变成 ; 即降级得到的.
	for id := range cfg.LearnersNext {
		// 之前的旧配置中必须存在它
		if _, ok := outgoing(cfg.Voters)[id]; !ok {
			return fmt.Errorf("%d 是在LearnersNext中,但不是在Voters中.[1]", id)
		}
		// 是不是已经被标记位了learner
		if prs[id].IsLearner {
			return fmt.Errorf("%d 是在LearnersNext中,但已经被标记为learner.", id)
		}
	}
	// 反之,learner和投票者根本没有交集.
	for id := range cfg.Learners {
		if _, ok := outgoing(cfg.Voters)[id]; ok {
			return fmt.Errorf("%d 在 Learners 、 Voters[1]", id)
		}
		if _, ok := incoming(cfg.Voters)[id]; ok {
			return fmt.Errorf("%d 在 Learners 、 Voters[0]", id)
		}
		if !prs[id].IsLearner {
			return fmt.Errorf("%d 是 Learners, 但没有被标记为learner", id)
		}
	}

	if !joint(cfg) { // 没有进入共识状态
		// 我们强制规定,空map是nil而不是0.
		if outgoing(cfg.Voters) != nil {
			return fmt.Errorf("cfg.Voters[1]必须是nil 当没有进入联合共识")
		}
		if cfg.LearnersNext != nil {
			return fmt.Errorf("cfg.LearnersNext必须是nil 当没有进入联合共识")
		}
		if cfg.AutoLeave {
			return fmt.Errorf("AutoLeave必须是false 当没有进入联合共识")
		}
	}
	return nil
}

// checkAndCopy 复制跟踪器的配置和进度Map,并返回这些副本.
func (c Changer) checkAndCopy() (tracker.Config, tracker.ProgressMap, error) {
	cfg := c.Tracker.Config.Clone() // 现有的集群配置
	var _ tracker.ProgressMap = c.Tracker.Progress
	prs := tracker.ProgressMap{} // 新的

	for id, pr := range c.Tracker.Progress {
		// pr是一个指针
		// 一个浅层拷贝就足够了,因为我们只对Learner字段进行变异.
		ppr := *pr
		prs[id] = &ppr
	}
	return checkAndReturn(cfg, prs) // cfg是深拷贝,prs是浅拷贝;确保配置和进度是相互兼容的
}

// checkAndReturn 在输入上调用checkInvariants,并返回结果错误或输入.
func checkAndReturn(cfg tracker.Config, prs tracker.ProgressMap) (tracker.Config, tracker.ProgressMap, error) {
	// cfg是深拷贝,prs是浅拷贝
	if err := checkInvariants(cfg, prs); err != nil { // 确保配置和进度是相互兼容的
		return tracker.Config{}, tracker.ProgressMap{}, err
	}
	return cfg, prs, nil
}

// initProgress 初始化给定Follower或Learner的Progress,该节点不能以任何一种形式存在,否则异常.
// ID是Peer的ID,match和next用来初始化Progress的.
func (c Changer) initProgress(cfg *tracker.Config, prs tracker.ProgressMap, id uint64, isLearner bool) {
	if !isLearner {
		// 等同于  cfg.Voters[0][id] = struct{}{}
		incoming(cfg.Voters)[id] = struct{}{}
	} else {
		nilAwareAdd(&cfg.Learners, id)
	}
	// Follower可以参与选举和投票,Learner不可以,只要知道这一点就可以了.无论是Follower还是
	// Learner都会有一个Progress,但是他们再次进行了分组管理.
	prs[id] = &tracker.Progress{
		// 初始化Progress需要给定Next、Match、Inflights容量以及是否是learner,其他也没啥
		// 此处可以剧透一下,raft的代码初始化的时候Match=0,Next=1.
		Next:      c.LastIndex,
		Match:     0,
		Inflights: tracker.NewInflights(c.Tracker.MaxInflight),
		IsLearner: isLearner,
		// 当一个节点第一次被添加时,我们应该把它标记为最近活跃.否则,如果CheckQuorum在被调用之前,可能会导致我们停止工作.
		// 在被添加的节点有机会与我们通信之前调用它,可能会导致我们降级.
		RecentActive: true,
	}
}

// nilAwareAdd 填充一个map条目,如果需要的话,创建map
func nilAwareAdd(m *map[uint64]struct{}, id uint64) {
	if *m == nil {
		*m = map[uint64]struct{}{}
	}
	(*m)[id] = struct{}{}
}

// nilAwareDelete 从一个map中删除,如果之后map是空的,则将其置空.
func nilAwareDelete(m *map[uint64]struct{}, id uint64) {
	if *m == nil {
		return
	}
	delete(*m, id)
	if len(*m) == 0 {
		*m = nil
	}
}

// err 返回零值和一个错误.
func (c Changer) err(err error) (tracker.Config, tracker.ProgressMap, error) {
	return tracker.Config{}, nil, err
}

// makeVoter 增加或提升给定的ID,使其成为入选的多数人配置中的选民.
func (c Changer) makeVoter(cfg *tracker.Config, prs tracker.ProgressMap, id uint64) {
	// cfg是深拷贝,prs是浅拷贝;获取当前的配置,确保配置和进度是相互兼容的
	pr := prs[id]
	if pr == nil {
		// 添加节点
		c.initProgress(cfg, prs, id, false /* isLearner */)
		return
	}
	// 提升角色 [learner->follower]
	pr.IsLearner = false
	nilAwareDelete(&cfg.Learners, id)
	nilAwareDelete(&cfg.LearnersNext, id)
	incoming(cfg.Voters)[id] = struct{}{}
}

// remove 从 voter[0]、learner、learnersNext 中删除
func (c Changer) remove(cfg *tracker.Config, prs tracker.ProgressMap, id uint64) {
	if _, ok := prs[id]; !ok {
		return
	}

	delete(incoming(cfg.Voters), id)
	nilAwareDelete(&cfg.Learners, id)
	nilAwareDelete(&cfg.LearnersNext, id)

	// 如果不再vote[1]中 删除;如果在vote[1]中,之后在leaveJoint处理
	if _, onRight := outgoing(cfg.Voters)[id]; !onRight {
		// cfg.Voters[1][id]
		delete(prs, id)
	}
}

// makeLearner 添加learner
func (c Changer) makeLearner(cfg *tracker.Config, prs tracker.ProgressMap, id uint64) {
	pr := prs[id]
	if pr == nil {
		// 新增加一个learner
		c.initProgress(cfg, prs, id, true /* isLearner */)
		return
	}
	if pr.IsLearner {
		return
	}
	// // 从 voters、learner、learnersNext 中删除
	c.remove(cfg, prs, id) // 从 voter[0]、learner、learnersNext 中删除
	prs[id] = pr
	//  如果我们不能直接将learner添加到learner中,也就是说,该peer在veto[1]中,是降级来的
	//  则使用 LearnersNext.
	//  在LeaveJoint()中,LearnersNext将被转变成一个learner.
	//  否则,立即添加一个普通的learner.
	if _, onRight := outgoing(cfg.Voters)[id]; onRight {
		// 降级
		nilAwareAdd(&cfg.LearnersNext, id)
	} else {
		// 新增
		pr.IsLearner = true
		nilAwareAdd(&cfg.Learners, id)
	}
}

// apply 一个对配置的改变.按照惯例,对voter的更改总是对 Voters[0] 进行.
//  [变更节点集合,老节点集合] 或 [节点、nil]
//  Voters[1]要么是空的,要么在联合状态下保留传出的多数配置.
func (c Changer) apply(cfg *tracker.Config, prs tracker.ProgressMap, ccs ...pb.ConfChangeSingle) error {
	// cfg是深拷贝,prs是浅拷贝;获取当前的配置,确保配置和进度是相互兼容的
	for _, cc := range ccs {
		if cc.NodeID == 0 {
			continue
		}
		// 只需要 节点 id、角色;   peer url 不需要
		// 更新cfg prs 数据
		switch cc.Type {
		case pb.ConfChangeAddNode:
			c.makeVoter(cfg, prs, cc.NodeID)
		case pb.ConfChangeAddLearnerNode:
			c.makeLearner(cfg, prs, cc.NodeID)
		case pb.ConfChangeRemoveNode:
			c.remove(cfg, prs, cc.NodeID)
		case pb.ConfChangeUpdateNode:
		default:
			return fmt.Errorf("未知的conf type %d", cc.Type)
		}
	}
	if len(incoming(cfg.Voters)) == 0 {
		return errors.New("删除了所有选民")
	}
	return nil
}

// symdiff 返回二者的差异数  len(a-b)+len(b-a):描述的不够准确
func symdiff(l, r map[uint64]struct{}) int {
	var n int
	// [[1,2],[1,2]]
	pairs := [][2]quorum.MajorityConfig{
		{l, r}, // count elems in l but not in r
		{r, l}, // count elems in r but not in l
	}
	for _, p := range pairs {
		for id := range p[0] {
			if _, ok := p[1][id]; !ok {
				n++
			}
		}
	}
	return n
}

// Simple 进行一系列的配置改变,(总的来说)使传入的多数配置Voters[0]最多变化一个.
// 如果不是这样,如果响应数:quorum为零,或者如果配置处于联合状态(即如果有一个传出的配置),这个方法将返回一个错误.
func (c Changer) Simple(ccs ...pb.ConfChangeSingle) (tracker.Config, tracker.ProgressMap, error) {
	cfg, prs, err := c.checkAndCopy() // cfg是深拷贝,prs是浅拷贝;获取当前的配置,确保配置和进度是相互兼容的
	if err != nil {
		return c.err(err)
	}
	if joint(cfg) { // 是不是进入联合共识
		err := errors.New("不能在联合配置中应用简单的配置更改")
		return c.err(err)
	}
	if err := c.apply(&cfg, prs, ccs...); err != nil { // 只更新cfg、prs记录
		return c.err(err)
	}
	// incoming voters[0]
	// prs = c.Tracker.Progress cfg = c.Tracker.Config
	_ = c.Tracker.Voters //  quorum.JointConfig
	// c.Tracker.Config.Voters   没有区别  Config是匿名结构体  c.Tracker.Voters

	if n := symdiff(incoming(c.Tracker.Voters), incoming(cfg.Voters)); n > 1 {
		// [12,123] 一般是 1
		// 存在不同的个数
		return tracker.Config{}, nil, errors.New("多个选民在没有进入联合配置的情况下发生变化")
	}

	return checkAndReturn(cfg, prs) // 内容校验
}
