// Copyright 2016 The etcd Authors
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
	"encoding/json"
	"fmt"
	"path"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2store"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/buckets"

	"github.com/coreos/go-semver/semver"
	"go.uber.org/zap"
)

const (
	attributesSuffix     = "attributes"
	raftAttributesSuffix = "raftAttributes"
	storePrefix          = "/0" // 在store中存储成员信息的前缀

)

var (
	StoreMembersPrefix        = path.Join(storePrefix, "members")         // /0/members
	storeRemovedMembersPrefix = path.Join(storePrefix, "removed_members") // /0/removed_members
	errMemberAlreadyExist     = fmt.Errorf("member already exists")
	errMemberNotFound         = fmt.Errorf("member not found")
)

// v2store 更新成员属性
func mustUpdateMemberAttrInStore(lg *zap.Logger, s v2store.Store, m *Member) {
	b, err := json.Marshal(m.Attributes)
	if err != nil {
		lg.Panic("序列化 属性失败", zap.Error(err))
	}
	p := path.Join(MemberStoreKey(m.ID), attributesSuffix)
	if _, err := s.Set(p, false, string(b), v2store.TTLOptionSet{ExpireTime: v2store.Permanent}); err != nil {
		lg.Panic("更新属性失败", zap.String("path", p), zap.Error(err))
	}
}

// v2store 保存集群版本
func mustSaveClusterVersionToStore(lg *zap.Logger, s v2store.Store, ver *semver.Version) {
	if _, err := s.Set(StoreClusterVersionKey(), false, ver.String(), v2store.TTLOptionSet{ExpireTime: v2store.Permanent}); err != nil {
		lg.Panic(
			"保存集群版本到store失败",
			zap.String("path", StoreClusterVersionKey()),
			zap.Error(err),
		)
	}
}

// 创建blot.db存储桶
func mustCreateBackendBuckets(be backend.Backend) {
	tx := be.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	tx.UnsafeCreateBucket(buckets.Members)
	tx.UnsafeCreateBucket(buckets.MembersRemoved)
	tx.UnsafeCreateBucket(buckets.Cluster)
}

// MemberAttributesStorePath v2store 成员属性路径
func MemberAttributesStorePath(id types.ID) string {
	return path.Join(MemberStoreKey(id), attributesSuffix)
}

func mustParseMemberIDFromBytes(lg *zap.Logger, key []byte) types.ID {
	id, err := types.IDFromString(string(key))
	if err != nil {
		lg.Panic("从key解析成员ID失败", zap.Error(err))
	}
	return id
}

// OK
func mustSaveMemberToStore(lg *zap.Logger, s v2store.Store, m *Member) {
	err := unsafeSaveMemberToStore(lg, s, m)
	if err != nil {
		lg.Panic(
			"保存member到store失败",
			zap.String("member-id", m.ID.String()),
			zap.Error(err),
		)
	}
}

// node---->v2store [memory]
func unsafeSaveMemberToStore(lg *zap.Logger, s v2store.Store, m *Member) error {
	b, err := json.Marshal(m.RaftAttributes) // 是raft集群中的对等体列表. 表示该成员是否是raft Learner.
	if err != nil {
		lg.Panic("序列化失败raftAttributes", zap.Error(err))
	}
	_ = computeMemberId                                        // id 由这个函数生成,需要 peerURLs clusterName 创建时间,创建时间一般为nil
	p := path.Join(MemberStoreKey(m.ID), raftAttributesSuffix) // /0/members/123/raftAttributes
	_, err = s.Create(p, false, string(b),                     // ✅
		false, v2store.TTLOptionSet{ExpireTime: v2store.Permanent})
	return err
}

func mustUpdateMemberInStore(lg *zap.Logger, s v2store.Store, m *Member) {
	// s 内存里面的一个树形node结构
	b, err := json.Marshal(m.RaftAttributes) // 是raft集群中的对等体列表. 表示该成员是否是raft Learner.
	if err != nil {
		lg.Panic("序列化raft相关属性失败", zap.Error(err))
	}
	p := path.Join(MemberStoreKey(m.ID), raftAttributesSuffix) //   123213/raftAttributes
	if _, err := s.Update(p, string(b), v2store.TTLOptionSet{ExpireTime: v2store.Permanent}); err != nil {
		lg.Panic("更新raftAttributes失败", zap.String("path", p), zap.Error(err))
	}
}

// MustParseMemberIDFromKey ok
func MustParseMemberIDFromKey(lg *zap.Logger, key string) types.ID {
	id, err := types.IDFromString(path.Base(key)) // /0/members/8e9e05c52164694d
	if err != nil {
		lg.Panic("从key解析member ID 失败", zap.Error(err))
	}
	return id
}

// 将member保存到Backend   bolt.db
func unsafeSaveMemberToBackend(lg *zap.Logger, be backend.Backend, m *Member) error {
	mkey := backendMemberKey(m.ID) // 16进制字符串
	mvalue, err := json.Marshal(m)
	if err != nil {
		lg.Panic("序列化失败", zap.Error(err))
	}

	tx := be.BatchTx() // 写事务
	tx.Lock()
	defer tx.Unlock()
	if unsafeMemberExists(tx, mkey) { // ✅
		return errMemberAlreadyExist
	}
	tx.UnsafePut(buckets.Members, mkey, mvalue)
	return nil
}

// MemberStoreKey 15   ----->  /0/members/e
func MemberStoreKey(id types.ID) string {
	return path.Join(StoreMembersPrefix, id.String()) // /0/members/e
}

// RemovedMemberStoreKey 15   -----> /0/removed_members/e
func RemovedMemberStoreKey(id types.ID) string {
	return path.Join(storeRemovedMembersPrefix, id.String())
}

// 移除节点,并添加到removed_member
func unsafeDeleteMemberFromStore(s v2store.Store, id types.ID) error {
	if _, err := s.Delete(MemberStoreKey(id), true, true); err != nil {
		return err
	}
	if _, err := s.Create(RemovedMemberStoreKey(id), // ✅
		false, "", false, v2store.TTLOptionSet{ExpireTime: v2store.Permanent}); err != nil {
		return err
	}
	return nil
}

// 首先遍历bolt.db members下的所有k,v
func unsafeMemberExists(tx backend.ReadTx, mkey []byte) bool {
	var found bool
	tx.UnsafeForEach(buckets.Members, func(k, v []byte) error {
		if bytes.Equal(k, mkey) {
			found = true
		}
		return nil
	})
	return found
}

// 从bolt.db删除节点信息
func unsafeDeleteMemberFromBackend(be backend.Backend, id types.ID) error {
	mkey := backendMemberKey(id)

	tx := be.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	tx.UnsafePut(buckets.MembersRemoved, mkey, []byte("removed")) // 更新
	if !unsafeMemberExists(tx, mkey) {
		return errMemberNotFound
	}
	tx.UnsafeDelete(buckets.Members, mkey)
	return nil
}

// 在bolt.db存储的key
func backendMemberKey(id types.ID) []byte {
	return []byte(id.String())
}

// nodeToMember 从node构建一个member
func nodeToMember(lg *zap.Logger, n *v2store.NodeExtern) (*Member, error) {
	m := &Member{ID: MustParseMemberIDFromKey(lg, n.Key)}
	attrs := make(map[string][]byte)
	raftAttrKey := path.Join(n.Key, raftAttributesSuffix) // /0/members/8e9e05c52164694d/raftAttributes
	attrKey := path.Join(n.Key, attributesSuffix)         // /0/members/8e9e05c52164694d/raftAttributes/attributes
	//  &v2store.NodeExtern{Key: "/0/members/8e9e05c52164694d", ExternNodes: []*v2store.NodeExtern{
	//		{Key: "/0/members/8e9e05c52164694d/raftAttributes/attributes", Value: stringp(`{"name":"node1","clientURLs":null}`)},
	//		{Key: "/0/members/8e9e05c52164694d/raftAttributes", Value: stringp(`{"peerURLs":null}`)},
	//	}}
	for _, nn := range n.ExternNodes {
		if nn.Key != raftAttrKey && nn.Key != attrKey {
			return nil, fmt.Errorf("未知的 key %q", nn.Key)
		}
		attrs[nn.Key] = []byte(*nn.Value)
	}
	if data := attrs[raftAttrKey]; data != nil {
		if err := json.Unmarshal(data, &m.RaftAttributes); err != nil {
			return nil, fmt.Errorf("反序列化 raftAttributes 失败: %v", err)
		}
	} else {
		return nil, fmt.Errorf("raftAttributes key不存在")
	}
	if data := attrs[attrKey]; data != nil {
		if err := json.Unmarshal(data, &m.Attributes); err != nil {
			return m, fmt.Errorf("反序列化 attributes 失败: %v", err)
		}
	}
	return m, nil
}

// TrimClusterFromBackend 从bolt.db移除cluster 桶
func TrimClusterFromBackend(be backend.Backend) error {
	tx := be.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	tx.UnsafeDeleteBucket(buckets.Cluster)
	return nil
}

// 读取bolt.db中的member桶
func readMembersFromBackend(lg *zap.Logger, be backend.Backend) (map[types.ID]*Member, map[types.ID]bool, error) {
	members := make(map[types.ID]*Member)
	removed := make(map[types.ID]bool)

	tx := be.ReadTx()
	tx.RLock()
	defer tx.RUnlock()
	err := tx.UnsafeForEach(buckets.Members, func(k, v []byte) error {
		memberId := mustParseMemberIDFromBytes(lg, k)
		m := &Member{ID: memberId}
		if err := json.Unmarshal(v, &m); err != nil {
			return err
		}
		members[memberId] = m
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("不能读取bolt.db中的member桶: %w", err)
	}

	err = tx.UnsafeForEach(buckets.MembersRemoved, func(k, v []byte) error {
		memberId := mustParseMemberIDFromBytes(lg, k)
		removed[memberId] = true
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("不能读取bolt.db中的 members_removed 桶: %w", err)
	}
	return members, removed, nil
}

func membersFromBackend(lg *zap.Logger, be backend.Backend) (map[types.ID]*Member, map[types.ID]bool) {
	return mustReadMembersFromBackend(lg, be)
}

// 从bolt.db读取成员信息
func mustReadMembersFromBackend(lg *zap.Logger, be backend.Backend) (map[types.ID]*Member, map[types.ID]bool) {
	members, removed, err := readMembersFromBackend(lg, be)
	if err != nil {
		lg.Panic("不能从bolt.db读取成员信息", zap.Error(err))
	}
	return members, removed
}

// TrimMembershipFromBackend  从bolt.db删除成员信息
func TrimMembershipFromBackend(lg *zap.Logger, be backend.Backend) error {
	lg.Info("开始删除成员信息...")
	tx := be.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	err := tx.UnsafeForEach(buckets.Members, func(k, v []byte) error {
		tx.UnsafeDelete(buckets.Members, k)
		lg.Debug("删除成员信息", zap.Stringer("member", mustParseMemberIDFromBytes(lg, k)))
		return nil
	})
	if err != nil {
		return err
	}
	return tx.UnsafeForEach(buckets.MembersRemoved, func(k, v []byte) error {
		tx.UnsafeDelete(buckets.MembersRemoved, k)
		lg.Debug("删除 已移除的成员信息", zap.Stringer("member", mustParseMemberIDFromBytes(lg, k)))
		return nil
	})
}

// TrimMembershipFromV2Store 从v2store删除所有节点信息
func TrimMembershipFromV2Store(lg *zap.Logger, s v2store.Store) error {
	members, removed := membersFromStore(lg, s)

	for mID := range members {
		_, err := s.Delete(MemberStoreKey(mID), true, true)
		if err != nil {
			return err
		}
	}
	for mID := range removed {
		_, err := s.Delete(RemovedMemberStoreKey(mID), true, true)
		if err != nil {
			return err
		}
	}

	return nil
}

// 保存集群版本到bolt.db
func mustSaveClusterVersionToBackend(be backend.Backend, ver *semver.Version) {
	ckey := backendClusterVersionKey()
	tx := be.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	tx.UnsafePut(buckets.Cluster, ckey, []byte(ver.String()))
}

// bolt.db 集群版本key
func backendClusterVersionKey() []byte {
	return []byte("clusterVersion")
}

func backendDowngradeKey() []byte {
	return []byte("downgrade")
}

// 保存降级信息到blot.db
func mustSaveDowngradeToBackend(lg *zap.Logger, be backend.Backend, downgrade *DowngradeInfo) {
	dkey := backendDowngradeKey() // downgrade
	dvalue, err := json.Marshal(downgrade)
	if err != nil {
		lg.Panic("序列化降级信息失败", zap.Error(err))
	}
	tx := be.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	tx.UnsafePut(buckets.Cluster, dkey, dvalue)
}

// StoreClusterVersionKey v2store中集群版本路径
func StoreClusterVersionKey() string { // /0/version
	return path.Join(storePrefix, "version")
}
