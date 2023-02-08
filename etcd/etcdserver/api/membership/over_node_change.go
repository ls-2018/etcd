package membership

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2error"
	"go.uber.org/zap"
)

type Member struct {
	ID             types.ID `json:"id"` // hash得到的, 本节点ID
	RaftAttributes          // 与raft相关的etcd成员属性
	Attributes              // 代表一个etcd成员的所有非raft的相关属性
}

// RaftAttributes  与raft相关的etcd成员属性
type RaftAttributes struct {
	PeerURLs  []string `json:"peerURLs"`            // 是raft集群中的对等体列表.
	IsLearner bool     `json:"isLearner,omitempty"` // 表示该成员是否是raft Learner.
}

// Attributes 代表一个etcd成员的所有非raft的相关属性.
type Attributes struct {
	Name       string   `json:"name,omitempty"`       // 节点创建时设置的name   默认default
	ClientURLs []string `json:"clientURLs,omitempty"` // 当接受到来自该Name的请求时,会
}

// NewMember 创建一个没有ID的成员,并根据集群名称、peer的URLS 和时间生成一个ID.这是用来引导/添加新成员的.
func NewMember(name string, peerURLs types.URLs, clusterName string, now *time.Time) *Member {
	memberId := computeMemberId(peerURLs, clusterName, now)
	return newMember(name, peerURLs, memberId, false)
}

// NewMemberAsLearner 创建一个没有ID的成员,并根据集群名称、peer的URLS 和时间生成一个ID.这是用来引导新learner成员的.
func NewMemberAsLearner(name string, peerURLs types.URLs, clusterName string, now *time.Time) *Member {
	memberId := computeMemberId(peerURLs, clusterName, now)
	return newMember(name, peerURLs, memberId, true)
}

// IsReadyToAddVotingMember OK
func (c *RaftCluster) IsReadyToAddVotingMember() bool {
	nmembers := 1 // 新添加的节点 先置1
	nstarted := 0

	for _, member := range c.VotingMembers() {
		if member.IsStarted() {
			nstarted++
		}
		nmembers++
	}

	if nstarted == 1 && nmembers == 2 {
		// 在一个成员集群中添加一个新节点,用于恢复集群数据
		c.lg.Debug("启动成员数为1;是否可以接受添加成员的请求")
		return true
	}

	nquorum := nmembers/2 + 1
	if nstarted < nquorum {
		c.lg.Warn("拒绝添加成员;启动的成员将少于法定人数", zap.Int("number-of-started-member", nstarted), zap.Int("quorum", nquorum), zap.String("cluster-id", c.cid.String()), zap.String("local-member-id", c.localID.String()))
		return false
	}

	return true
}

func (c *RaftCluster) IsReadyToRemoveVotingMember(id uint64) bool {
	nmembers := 0
	nstarted := 0

	for _, member := range c.VotingMembers() {
		if uint64(member.ID) == id {
			continue
		}

		if member.IsStarted() {
			nstarted++
		}
		nmembers++
	}

	nquorum := nmembers/2 + 1
	if nstarted < nquorum {
		c.lg.Warn(
			"rejecting member remove; started member will be less than quorum",
			zap.Int("number-of-started-member", nstarted),
			zap.Int("quorum", nquorum),
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
		)
		return false
	}

	return true
}

// IsReadyToPromoteMember 是否准备好提升节点角色, 提升以后现有成员是否可以达到大多数
func (c *RaftCluster) IsReadyToPromoteMember(id uint64) bool {
	nmembers := 1 // 我们为未来的法定人数计算被提拔的学习者
	nstarted := 1

	for _, member := range c.VotingMembers() {
		if member.IsStarted() {
			nstarted++
		}
		nmembers++
	}

	nquorum := nmembers/2 + 1
	if nstarted < nquorum {
		c.lg.Warn("拒绝成员晋升;启动成员将少于法定人数",
			zap.Int("number-of-started-member", nstarted),
			zap.Int("quorum", nquorum),
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
		)
		return false
	}

	return true
}

// ------------------------------------------------ over ------------------------------------------------

// PromoteMember 将该成员的IsLearner属性标记为false.
func (c *RaftCluster) PromoteMember(id types.ID, shouldApplyV3 ShouldApplyV3) {
	c.Lock()
	defer c.Unlock()

	c.members[id].RaftAttributes.IsLearner = false
	if c.v2store != nil {
		// 内存里面的一个树形node结构
		mustUpdateMemberInStore(c.lg, c.v2store, c.members[id])
	}
	if c.be != nil && shouldApplyV3 {
		unsafeSaveMemberToBackend(c.lg, c.be, c.members[id])
	}

	c.lg.Info("成员角色提升", zap.String("cluster-id", c.cid.String()), zap.String("local-member-id", c.localID.String()))
}

// AddMember 在集群中添加一个新的成员,并将给定成员的raftAttributes保存到存储空间.给定的成员应该有空的属性. 一个具有匹配id的成员必须不存在.
func (c *RaftCluster) AddMember(m *Member, shouldApplyV3 ShouldApplyV3) {
	c.Lock()
	defer c.Unlock()

	var v2Err, beErr error
	if c.v2store != nil {
		v2Err = unsafeSaveMemberToStore(c.lg, c.v2store, m)
		if v2Err != nil {
			if e, ok := v2Err.(*v2error.Error); !ok || e.ErrorCode != v2error.EcodeNodeExist {
				c.lg.Panic("保存member到v2store失败", zap.String("member-id", m.ID.String()), zap.Error(v2Err))
			}
		}
	}
	_ = backend.MyBackend{}
	if c.be != nil && shouldApplyV3 {
		beErr = unsafeSaveMemberToBackend(c.lg, c.be, m) // 保存到bolt.db     members
		if beErr != nil && !errors.Is(beErr, errMemberAlreadyExist) {
			c.lg.Panic("保存member到backend失败", zap.String("member-id", m.ID.String()), zap.Error(beErr))
		}
	}
	if v2Err != nil && (beErr != nil || c.be == nil) {
		c.lg.Panic("保存member到store失败", zap.String("member-id", m.ID.String()), zap.Error(v2Err))
	}

	c.members[m.ID] = m

	c.lg.Info("添加成员", zap.String("cluster-id", c.cid.String()), zap.String("local-member-id", c.localID.String()), zap.String("added-peer-id", m.ID.String()), zap.Strings("added-peer-peer-urls", m.PeerURLs))
}

// RemoveMember  store中必须存在该ID,否则会panic
func (c *RaftCluster) RemoveMember(id types.ID, shouldApplyV3 ShouldApplyV3) {
	c.Lock()
	defer c.Unlock()
	var v2Err, beErr error
	if c.v2store != nil {
		v2Err = unsafeDeleteMemberFromStore(c.v2store, id)
		if v2Err != nil {
			if e, ok := v2Err.(*v2error.Error); !ok || e.ErrorCode != v2error.EcodeKeyNotFound {
				c.lg.Panic("从v2store删除节点失败", zap.String("member-id", id.String()), zap.Error(v2Err))
			}
		}
	}
	if c.be != nil && shouldApplyV3 {
		beErr = unsafeDeleteMemberFromBackend(c.be, id)
		if beErr != nil && !errors.Is(beErr, errMemberNotFound) {
			c.lg.Panic("从backend  bolt 删除节点失败", zap.String("member-id", id.String()), zap.Error(beErr))
		}
	}
	if v2Err != nil && (beErr != nil || c.be == nil) {
		c.lg.Panic("从store中删除节点失败", zap.String("member-id", id.String()), zap.Error(v2Err))
	}

	m, ok := c.members[id]
	delete(c.members, id)
	c.removed[id] = true

	if ok {
		c.lg.Info("移除成员", zap.String("cluster-id", c.cid.String()), zap.String("local-member-id", c.localID.String()), zap.String("removed-remote-peer-id", id.String()), zap.Strings("removed-remote-peer-urls", m.PeerURLs))
	} else {
		c.lg.Warn("该成员已经移除", zap.String("cluster-id", c.cid.String()), zap.String("local-member-id", c.localID.String()), zap.String("removed-remote-peer-id", id.String()))
	}
}

// 计算成员ID
func computeMemberId(peerURLs types.URLs, clusterName string, now *time.Time) types.ID {
	peerURLstrs := peerURLs.StringSlice()
	sort.Strings(peerURLstrs)
	joinedPeerUrls := strings.Join(peerURLstrs, "")
	b := []byte(joinedPeerUrls)

	b = append(b, []byte(clusterName)...)
	if now != nil {
		b = append(b, []byte(fmt.Sprintf("%d", now.Unix()))...)
	}

	hash := sha1.Sum(b)
	return types.ID(binary.BigEndian.Uint64(hash[:8]))
}

func newMember(name string, peerURLs types.URLs, memberId types.ID, isLearner bool) *Member {
	m := &Member{
		RaftAttributes: RaftAttributes{
			PeerURLs:  peerURLs.StringSlice(),
			IsLearner: isLearner,
		},
		Attributes: Attributes{Name: name},
		ID:         memberId,
	}
	return m
}

// PickPeerURL 随机从 Member's PeerURLs 选择一个
func (m *Member) PickPeerURL() string {
	if len(m.PeerURLs) == 0 {
		panic("peer url 应该>0")
	}
	return m.PeerURLs[rand.Intn(len(m.PeerURLs))]
}

// Clone 返回member deepcopy
func (m *Member) Clone() *Member {
	if m == nil {
		return nil
	}
	mm := &Member{
		ID: m.ID,
		RaftAttributes: RaftAttributes{
			IsLearner: m.IsLearner,
		},
		Attributes: Attributes{
			Name: m.Name,
		},
	}
	if m.PeerURLs != nil {
		mm.PeerURLs = make([]string, len(m.PeerURLs))
		copy(mm.PeerURLs, m.PeerURLs)
	}
	if m.ClientURLs != nil {
		mm.ClientURLs = make([]string, len(m.ClientURLs))
		copy(mm.ClientURLs, m.ClientURLs)
	}
	return mm
}

func (m *Member) IsStarted() bool {
	return len(m.Name) != 0
}

type MembersByID []*Member

func (ms MembersByID) Len() int           { return len(ms) }
func (ms MembersByID) Less(i, j int) bool { return ms[i].ID < ms[j].ID }
func (ms MembersByID) Swap(i, j int)      { ms[i], ms[j] = ms[j], ms[i] }

type MembersByPeerURLs []*Member

func (ms MembersByPeerURLs) Len() int { return len(ms) }
func (ms MembersByPeerURLs) Less(i, j int) bool {
	return ms[i].PeerURLs[0] < ms[j].PeerURLs[0]
}
func (ms MembersByPeerURLs) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }
