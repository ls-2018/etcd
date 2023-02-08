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

package membership

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ls-2018/etcd_cn/raft"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2store"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/buckets"
	"github.com/ls-2018/etcd_cn/offical/api/v3/version"
	"github.com/ls-2018/etcd_cn/pkg/netutil"
	"github.com/ls-2018/etcd_cn/raft/raftpb"

	"github.com/coreos/go-semver/semver"
	"go.uber.org/zap"
)

const maxLearners = 1

// RaftCluster raft集群成员
type RaftCluster struct {
	lg            *zap.Logger
	localID       types.ID             // 本机节点ID
	cid           types.ID             // 集群ID,根据所有初始 memberID hash 得到的
	v2store       v2store.Store        // 内存里面的一个树形node结构
	be            backend.Backend      //
	sync.Mutex                         // 守住下面的字段
	version       *semver.Version      //
	members       map[types.ID]*Member //
	removed       map[types.ID]bool    // 记录被删除的节点ID,删除后的节点无法重用
	downgradeInfo *DowngradeInfo       // 降级信息
}

type ConfigChangeContext struct {
	Member
	IsPromote bool `json:"isPromote"` // 是否提升learner
}

type ShouldApplyV3 bool

const (
	ApplyBoth        = ShouldApplyV3(true)
	ApplyV2storeOnly = ShouldApplyV3(false)
)

// Recover 接收到快照之后,会调用此函数
func (c *RaftCluster) Recover(onSet func(*zap.Logger, *semver.Version)) {
	c.Lock()
	defer c.Unlock()

	if c.v2store != nil {
		c.version = clusterVersionFromStore(c.lg, c.v2store)
		c.members, c.removed = membersFromStore(c.lg, c.v2store)
	} else {
		c.version = clusterVersionFromBackend(c.lg, c.be)
		c.members, c.removed = membersFromBackend(c.lg, c.be)
	}

	if c.be != nil {
		c.downgradeInfo = downgradeInfoFromBackend(c.lg, c.be)
	}
	d := &DowngradeInfo{Enabled: false}
	if c.downgradeInfo != nil {
		d = &DowngradeInfo{Enabled: c.downgradeInfo.Enabled, TargetVersion: c.downgradeInfo.TargetVersion}
	}
	mustDetectDowngrade(c.lg, c.version, d) // 检测版本降级
	onSet(c.lg, c.version)

	for _, m := range c.members {
		c.lg.Info(
			"从store中恢复/增加成员", zap.String("cluster-id", c.cid.String()), zap.String("local-member-id", c.localID.String()), zap.String("recovered-remote-peer-id", m.ID.String()), zap.Strings("recovered-remote-peer-urls", m.PeerURLs),
		)
	}
	if c.version != nil {
		c.lg.Info("从store获取version,并设置", zap.String("cluster-version", version.Cluster(c.version.String())))
	}
}

// NewClusterFromMembers 从远端节点获取到的集群节点信息
func NewClusterFromMembers(lg *zap.Logger, id types.ID, membs []*Member) *RaftCluster {
	c := NewCluster(lg)
	c.cid = id
	for _, m := range membs {
		c.members[m.ID] = m
	}
	return c
}

// UpdateAttributes 更新成员属性
func (c *RaftCluster) UpdateAttributes(id types.ID, attr Attributes, shouldApplyV3 ShouldApplyV3) {
	c.Lock()
	defer c.Unlock()

	if m, ok := c.members[id]; ok {
		m.Attributes = attr
		if c.v2store != nil {
			mustUpdateMemberAttrInStore(c.lg, c.v2store, m)
		}
		if c.be != nil && shouldApplyV3 {
			unsafeSaveMemberToBackend(c.lg, c.be, m)
		}
		return
	}

	_, ok := c.removed[id]
	if !ok {
		c.lg.Panic("更新失败", zap.String("cluster-id", c.cid.String()), zap.String("local-member-id", c.localID.String()), zap.String("unknown-remote-peer-id", id.String()))
	}

	c.lg.Warn("移除的成员 不进行属性更新", zap.String("cluster-id", c.cid.String()), zap.String("local-member-id", c.localID.String()), zap.String("updated-peer-id", id.String()))
}

// ValidateClusterAndAssignIDs 通过匹配PeerURLs来验证本地集群与现有集群是否匹配.如果验证成功,它将把现有集群的IDs归入本地集群.
// 如果验证失败,将返回一个错误.
func ValidateClusterAndAssignIDs(lg *zap.Logger, local *RaftCluster, existing *RaftCluster) error {
	ems := existing.Members()
	lms := local.Members()
	if len(ems) != len(lms) {
		return fmt.Errorf("成员数量不一致")
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	for i := range ems {
		var err error
		ok := false
		for j := range lms {
			if ok, err = netutil.URLStringsEqual(ctx, lg, ems[i].PeerURLs, lms[j].PeerURLs); ok {
				lms[j].ID = ems[i].ID
				break
			}
		}
		if !ok {
			return fmt.Errorf("PeerURLs: 没有找到匹配的现有成员(%v, %v),最后的解析器错误(%v).", ems[i].ID, ems[i].PeerURLs, err)
		}
	}
	local.members = make(map[types.ID]*Member)
	for _, m := range lms {
		local.members[m.ID] = m
	}
	return nil
}

func (c *RaftCluster) ID() types.ID { return c.cid }

func (c *RaftCluster) Members() []*Member {
	c.Lock()
	defer c.Unlock()
	var ms MembersByID
	for _, m := range c.members {
		ms = append(ms, m.Clone())
	}
	sort.Sort(ms)
	return []*Member(ms)
}

// Member ok
func (c *RaftCluster) Member(id types.ID) *Member {
	c.Lock()
	defer c.Unlock()
	return c.members[id].Clone()
}

// 从v2Store中获取所有的集群节点
func membersFromStore(lg *zap.Logger, st v2store.Store) (map[types.ID]*Member, map[types.ID]bool) {
	members := make(map[types.ID]*Member)
	removed := make(map[types.ID]bool)
	e, err := st.Get(StoreMembersPrefix, true, true) // 获取/0/members 事件
	if err != nil {
		if isKeyNotFound(err) { // 不存在 /0/members节点
			return members, removed
		}
		lg.Panic("从store获取成员失败", zap.String("path", StoreMembersPrefix), zap.Error(err))
	}
	for _, n := range e.NodeExtern.ExternNodes {
		var m *Member
		m, err = nodeToMember(lg, n)
		if err != nil {
			lg.Panic("node--->member失败", zap.Error(err))
		}
		members[m.ID] = m
	}

	e, err = st.Get(storeRemovedMembersPrefix, true, true) // 获取/0/removed_members 事件
	if err != nil {
		if isKeyNotFound(err) {
			return members, removed
		}
		lg.Panic("从store中获取移除节点失败", zap.String("path", storeRemovedMembersPrefix), zap.Error(err))
	}
	for _, n := range e.NodeExtern.ExternNodes {
		removed[MustParseMemberIDFromKey(lg, n.Key)] = true
	}
	return members, removed
}

func (c *RaftCluster) IsIDRemoved(id types.ID) bool {
	c.Lock()
	defer c.Unlock()
	return c.removed[id]
}

func (c *RaftCluster) String() string {
	c.Lock()
	defer c.Unlock()
	b := &bytes.Buffer{}
	fmt.Fprintf(b, "{ClusterID:%s ", c.cid)
	var ms []string
	for _, m := range c.members {
		ms = append(ms, fmt.Sprintf("%+v", m))
	}
	fmt.Fprintf(b, "Members:[%s] ", strings.Join(ms, " "))
	var ids []string
	for id := range c.removed {
		ids = append(ids, id.String())
	}
	fmt.Fprintf(b, "RemovedMemberIDs:[%s]}", strings.Join(ids, " "))
	return b.String()
}

// 生成集群ID
func (c *RaftCluster) genID() {
	mIDs := c.MemberIDs() // 返回所有成员iD
	b := make([]byte, 8*len(mIDs))
	//[id,id,id,id,id,id,id]
	for i, id := range mIDs {
		binary.BigEndian.PutUint64(b[8*i:], uint64(id))
	}
	hash := sha1.Sum(b)
	c.cid = types.ID(binary.BigEndian.Uint64(hash[:8]))
}

// UpdateRaftAttributes 节点的属性更新
func (c *RaftCluster) UpdateRaftAttributes(id types.ID, raftAttr RaftAttributes, shouldApplyV3 ShouldApplyV3) {
	c.Lock()
	defer c.Unlock()

	c.members[id].RaftAttributes = raftAttr
	if c.v2store != nil {
		mustUpdateMemberInStore(c.lg, c.v2store, c.members[id])
	}
	if c.be != nil && shouldApplyV3 {
		unsafeSaveMemberToBackend(c.lg, c.be, c.members[id])
	}

	c.lg.Info("更新成员属性", zap.String("cluster-id", c.cid.String()),
		zap.String("local-member-id", c.localID.String()),
		zap.String("updated-remote-peer-id", id.String()),
		zap.Strings("updated-remote-peer-urls", raftAttr.PeerURLs),
	)
}

// MemberByName 返回一个具有给定名称的成员
func (c *RaftCluster) MemberByName(name string) *Member {
	c.Lock()
	defer c.Unlock()
	var memb *Member
	for _, m := range c.members {
		if m.Name == name {
			if memb != nil {
				c.lg.Panic("发现了两个相同名称的成员", zap.String("name", name))
			}
			memb = m
		}
	}
	return memb.Clone()
}

// MemberIDs 返回所有成员iD
func (c *RaftCluster) MemberIDs() []types.ID {
	c.Lock()
	defer c.Unlock()
	var ids []types.ID
	for _, m := range c.members {
		ids = append(ids, m.ID)
	}
	sort.Sort(types.IDSlice(ids))
	return ids
}

// SetID 设置ID
func (c *RaftCluster) SetID(localID, cid types.ID) {
	c.localID = localID
	c.cid = cid
}

// SetStore OK
func (c *RaftCluster) SetStore(st v2store.Store) { c.v2store = st }

func (c *RaftCluster) SetBackend(be backend.Backend) {
	c.be = be
	mustCreateBackendBuckets(c.be)
}

// VotingMembers 集群中的可投票成员
func (c *RaftCluster) VotingMembers() []*Member {
	c.Lock()
	defer c.Unlock()
	var ms MembersByID
	for _, m := range c.members {
		if !m.IsLearner {
			ms = append(ms, m.Clone())
		}
	}
	sort.Sort(ms)
	return []*Member(ms)
}

// Version 集群版本
func (c *RaftCluster) Version() *semver.Version {
	c.Lock()
	defer c.Unlock()
	if c.version == nil {
		return nil
	}
	return semver.Must(semver.NewVersion(c.version.String()))
}

// SetVersion 设置集群版本
func (c *RaftCluster) SetVersion(ver *semver.Version, onSet func(*zap.Logger, *semver.Version), shouldApplyV3 ShouldApplyV3) {
	c.Lock()
	defer c.Unlock()
	if c.version != nil {
		c.lg.Info("更新集群版本",
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
			zap.String("from", version.Cluster(c.version.String())),
			zap.String("to", version.Cluster(ver.String())),
		)
	} else {
		c.lg.Info("设置初始集群版本",
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
			zap.String("cluster-version", version.Cluster(ver.String())),
		)
	}
	c.version = ver
	mustDetectDowngrade(c.lg, c.version, c.downgradeInfo)
	if c.v2store != nil {
		mustSaveClusterVersionToStore(c.lg, c.v2store, ver)
	}
	if c.be != nil && shouldApplyV3 {
		mustSaveClusterVersionToBackend(c.be, ver)
	}
	onSet(c.lg, ver)
}

// NewClusterFromURLsMap 使用提供的url映射创建一个新的raft集群.目前,该算法不支持使用raft learner成员创建集群.
func NewClusterFromURLsMap(lg *zap.Logger, token string, urlsmap types.URLsMap) (*RaftCluster, error) {
	c := NewCluster(lg) //  RaftCluster struct
	for name, urls := range urlsmap {
		m := NewMember(name, urls, token, nil)
		if _, ok := c.members[m.ID]; ok {
			return nil, fmt.Errorf(" %v", m)
		}
		if uint64(m.ID) == raft.None {
			return nil, fmt.Errorf("不能使用 %x作为成员ID", raft.None)
		}
		c.members[m.ID] = m
	}
	c.genID() // 生成集群ID
	return c, nil
}

// PeerURLs 返回所有成员的通信地址
func (c *RaftCluster) PeerURLs() []string {
	c.Lock()
	defer c.Unlock()
	urls := make([]string, 0)
	for _, p := range c.members {
		urls = append(urls, p.PeerURLs...)
	}
	sort.Strings(urls)
	return urls
}

func NewCluster(lg *zap.Logger) *RaftCluster {
	if lg == nil {
		lg = zap.NewNop()
	}
	return &RaftCluster{
		lg:            lg,
		members:       make(map[types.ID]*Member),
		removed:       make(map[types.ID]bool),
		downgradeInfo: &DowngradeInfo{Enabled: false},
	}
}

func clusterVersionFromBackend(lg *zap.Logger, be backend.Backend) *semver.Version {
	ckey := backendClusterVersionKey()
	tx := be.ReadTx()
	tx.RLock()
	defer tx.RUnlock()
	keys, vals := tx.UnsafeRange(buckets.Cluster, ckey, nil, 0) // 从集群获取 获取 clusterVersion
	if len(keys) == 0 {
		return nil
	}
	if len(keys) != 1 {
		lg.Panic("从后端获取集群版本时,键的数量超出预期", zap.Int("number-of-key", len(keys)))
	}
	return semver.Must(semver.NewVersion(string(vals[0])))
}

func downgradeInfoFromBackend(lg *zap.Logger, be backend.Backend) *DowngradeInfo {
	dkey := backendDowngradeKey()
	tx := be.ReadTx()
	tx.Lock()
	defer tx.Unlock()
	keys, vals := tx.UnsafeRange(buckets.Cluster, dkey, nil, 0) // 从集群获取 获取 downgrade

	if len(keys) == 0 {
		return nil
	}

	if len(keys) != 1 {
		lg.Panic(
			"unexpected number of keys when getting cluster version from backend",
			zap.Int("number-of-key", len(keys)),
		)
	}
	var d DowngradeInfo
	if err := json.Unmarshal([]byte(vals[0]), &d); err != nil {
		lg.Panic("反序列化失败", zap.Error(err))
	}
	if d.Enabled {
		if _, err := semver.NewVersion(d.TargetVersion); err != nil {
			lg.Panic(
				"降级目标版本的版本格式出乎意料",
				zap.String("target-version", d.TargetVersion),
			)
		}
	}
	return &d
}

func (c *RaftCluster) IsMemberExist(id types.ID) bool {
	c.Lock()
	defer c.Unlock()
	_, ok := c.members[id]
	return ok
}

func (c *RaftCluster) VotingMemberIDs() []types.ID {
	c.Lock()
	defer c.Unlock()
	var ids []types.ID
	for _, m := range c.members {
		if !m.IsLearner {
			ids = append(ids, m.ID)
		}
	}
	sort.Sort(types.IDSlice(ids))
	return ids
}

func (c *RaftCluster) IsLocalMemberLearner() bool {
	c.Lock()
	defer c.Unlock()
	localMember, ok := c.members[c.localID]
	if !ok {
		c.lg.Panic("无法查找到本地节点", zap.String("cluster-id", c.cid.String()), zap.String("local-member-id", c.localID.String()))
	}
	return localMember.IsLearner
}

func (c *RaftCluster) DowngradeInfo() *DowngradeInfo {
	c.Lock()
	defer c.Unlock()
	if c.downgradeInfo == nil {
		return &DowngradeInfo{Enabled: false}
	}
	d := &DowngradeInfo{Enabled: c.downgradeInfo.Enabled, TargetVersion: c.downgradeInfo.TargetVersion}
	return d
}

func (c *RaftCluster) SetDowngradeInfo(d *DowngradeInfo, shouldApplyV3 ShouldApplyV3) {
	c.Lock()
	defer c.Unlock()

	if c.be != nil && shouldApplyV3 {
		mustSaveDowngradeToBackend(c.lg, c.be, d)
	}

	c.downgradeInfo = d

	if d.Enabled {
		c.lg.Info(
			"The etcd is ready to downgrade",
			zap.String("target-version", d.TargetVersion),
			zap.String("etcd-version", version.Version),
		)
	}
}

// PushMembershipToStorage 是覆盖集群成员的存储信息,使其完全反映RaftCluster的内部存储.
func (c *RaftCluster) PushMembershipToStorage() {
	if c.be != nil {
		TrimMembershipFromBackend(c.lg, c.be)
		for _, m := range c.members {
			unsafeSaveMemberToBackend(c.lg, c.be, m)
		}
	}
	if c.v2store != nil {
		TrimMembershipFromV2Store(c.lg, c.v2store)
		for _, m := range c.members {
			mustSaveMemberToStore(c.lg, c.v2store, m)
		}
	}
}

func clusterVersionFromStore(lg *zap.Logger, st v2store.Store) *semver.Version {
	e, err := st.Get(path.Join(storePrefix, "version"), false, false)
	if err != nil {
		if isKeyNotFound(err) {
			return nil
		}
		lg.Panic("从store获取集群版本信息失败", zap.String("path", path.Join(storePrefix, "version")), zap.Error(err))
	}
	return semver.Must(semver.NewVersion(*e.NodeExtern.Value))
}

// IsValidVersionChange 检查两种情况下的版本变更是否有效.
// 1.降级:集群版本比本地版本高一个小版本.集群版本应该改变.
// 2.集群启动:当不是所有成员的版本都可用时,集群版本被设置为MinVersion(3.0),当所有成员都在较高版本时,集群版本低于本地版本时,簇的版本应该改变.
func IsValidVersionChange(cv *semver.Version, lv *semver.Version) bool {
	// 集群版本
	cv = &semver.Version{Major: cv.Major, Minor: cv.Minor}
	// 本地版本
	lv = &semver.Version{Major: lv.Major, Minor: lv.Minor}

	if isValidDowngrade(cv, lv) || (cv.Major == lv.Major && cv.LessThan(*lv)) {
		return true
	}
	return false
}

// ValidateConfigurationChange  验证接受 提议的ConfChange 并确保它仍然有效.
func (c *RaftCluster) ValidateConfigurationChange(cc raftpb.ConfChangeV1) error {
	members, removed := membersFromStore(c.lg, c.v2store) // 从v2store中获取所有成员
	// members 包括leader、follower、learner、候选者
	id := types.ID(cc.NodeID)
	if removed[id] { // 不能在移除的节点中
		return ErrIDRemoved
	}
	_ = cc.Context // ConfigChangeContext Member 的序列化数据
	switch cc.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		confChangeContext := new(ConfigChangeContext)
		if err := json.Unmarshal([]byte(cc.Context), confChangeContext); err != nil {
			c.lg.Panic("发序列化失败confChangeContext", zap.Error(err))
		}
		if confChangeContext.IsPromote { // 将一个learner提升为投票节点, 那他应该是learner
			if members[id] == nil {
				return ErrIDNotFound
			}
			if !members[id].IsLearner {
				return ErrMemberNotLearner
			}
		} else { // 添加新节点
			if members[id] != nil {
				return ErrIDExists
			}

			urls := make(map[string]bool) // 当前集群所有节点的通信地址
			for _, m := range members {
				for _, u := range m.PeerURLs {
					urls[u] = true
				}
			}
			// 检查peer地址是否已存在
			for _, u := range confChangeContext.Member.PeerURLs {
				if urls[u] {
					return ErrPeerURLexists
				}
			}

			if confChangeContext.Member.IsLearner { // 新加入的节点时learner
				numLearners := 0
				for _, m := range members {
					if m.IsLearner {
						numLearners++
					}
				}
				if numLearners+1 > maxLearners {
					return ErrTooManyLearners
				}
			}
		}

	case raftpb.ConfChangeRemoveNode:
		if members[id] == nil {
			return ErrIDNotFound
		}

	case raftpb.ConfChangeUpdateNode:
		// 有这个成员,且peer地址不存在
		if members[id] == nil {
			return ErrIDNotFound
		}
		urls := make(map[string]bool)
		for _, m := range members {
			if m.ID == id {
				continue
			}
			for _, u := range m.PeerURLs {
				urls[u] = true
			}
		}
		m := new(Member)
		if err := json.Unmarshal([]byte(cc.Context), m); err != nil {
			c.lg.Panic("反序列化成员失败", zap.Error(err))
		}
		for _, u := range m.PeerURLs {
			if urls[u] {
				return ErrPeerURLexists
			}
		}

	default:
		c.lg.Panic("未知的 ConfChange type", zap.String("type", cc.Type.String()))
	}
	return nil
}

// ClientURLs 所有监听客户端请求的地址
func (c *RaftCluster) ClientURLs() []string {
	c.Lock()
	defer c.Unlock()
	urls := make([]string, 0)
	for _, p := range c.members {
		urls = append(urls, p.ClientURLs...)
	}
	sort.Strings(urls)
	return urls
}
