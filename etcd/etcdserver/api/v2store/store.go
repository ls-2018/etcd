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
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2error"
)

// The 当store第一次被初始化时,要设置的默认版本.
const defaultVersion = 2

var minExpireTime time.Time

func init() {
	minExpireTime, _ = time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
}

// Store Etcd是存储有如下特点:
// 1、采用kv型数据存储,一般情况下比关系型数据库快.
// 2、支持动态存储(内存)以及静态存储(磁盘).
// 3、分布式存储,可集成为多节点集群.
// 4、存储方式,采用类似目录结构.
// 1)只有叶子节点才能真正存储数据,相当于文件.
// 2)叶子节点的父节点一定是目录,目录不能存储数据.
type Store interface {
	Version() int
	Index() uint64
	Get(nodePath string, recursive, sorted bool) (*Event, error)
	Set(nodePath string, dir bool, value string, expireOpts TTLOptionSet) (*Event, error)
	Update(nodePath string, newValue string, expireOpts TTLOptionSet) (*Event, error)
	Create(nodePath string, dir bool, value string, unique bool, expireOpts TTLOptionSet) (*Event, error)
	CompareAndSwap(nodePath string, prevValue string, prevIndex uint64, value string, expireOpts TTLOptionSet) (*Event, error)
	Delete(nodePath string, dir, recursive bool) (*Event, error)
	CompareAndDelete(nodePath string, prevValue string, prevIndex uint64) (*Event, error)
	Watch(prefix string, recursive, stream bool, sinceIndex uint64) (Watcher, error)
	Save() ([]byte, error)
	Recovery(state []byte) error
	Clone() Store
	SaveNoCopy() ([]byte, error)
	JsonStats() []byte
	DeleteExpiredKeys(cutoff time.Time)
	HasTTLKeys() bool
}

type TTLOptionSet struct {
	ExpireTime time.Time // key的有效期
	Refresh    bool
}

type store struct {
	Root           *node       // 根节点
	WatcherHub     *watcherHub // 关于node的所有key的watcher
	CurrentIndex   uint64      // 对应存储内容的index
	Stats          *Stats
	CurrentVersion int             // 最新数据的版本
	ttlKeyHeap     *ttlKeyHeap     // 用于数据恢复的（需手动操作）     过期时间的最小堆
	worldLock      sync.RWMutex    //  停止当前存储的world锁
	clock          clockwork.Clock //
	readonlySet    types.Set       // 只读操作
}

// New 创建一个存储空间,给定的命名空间将被创建为初始目录.
func New(namespaces ...string) Store {
	s := newStore(namespaces...)
	s.clock = clockwork.NewRealClock()
	return s
}

// OK /0 /1
func newStore(namespaces ...string) *store {
	s := new(store)
	s.CurrentVersion = defaultVersion                       // 2
	s.Root = newDir(s, "/", s.CurrentIndex, nil, Permanent) // 0  永久性  //创建其在etcd中对应的目录,第一个目录是以(/)
	for _, namespace := range namespaces {
		s.Root.Add(newDir(s, namespace, s.CurrentIndex, s.Root, Permanent))
	}
	s.Stats = newStats()
	s.WatcherHub = newWatchHub(1000)
	s.ttlKeyHeap = newTtlKeyHeap()
	s.readonlySet = types.NewUnsafeSet(append(namespaces, "/")...)
	return s
}

// Set creates or replace the node at nodePath.
func (s *store) Set(nodePath string, dir bool, value string, expireOpts TTLOptionSet) (*Event, error) {
	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(SetSuccess)
			return
		}

		s.Stats.Inc(SetFail)
	}()

	// Get prevNode value
	n, getErr := s.internalGet(nodePath)
	if getErr != nil && getErr.ErrorCode != v2error.EcodeKeyNotFound {
		err = getErr
		return nil, err
	}

	if expireOpts.Refresh {
		if getErr != nil {
			err = getErr
			return nil, err
		}
		value = n.Value
	}

	// Set new value
	e, err := s.internalCreate(nodePath, dir, value, false, true, expireOpts.ExpireTime, Set)
	if err != nil {
		return nil, err
	}
	e.EtcdIndex = s.CurrentIndex

	// Put prevNode into event
	if getErr == nil {
		prev := newEvent(Get, nodePath, n.ModifiedIndex, n.CreatedIndex)
		prev.NodeExtern.loadInternalNode(n, false, false, s.clock)
		e.PrevNode = prev.NodeExtern
	}

	if !expireOpts.Refresh {
		s.WatcherHub.notify(e)
	} else {
		e.SetRefresh()
		s.WatcherHub.add(e)
	}

	return e, nil
}

// returns user-readable cause of failed comparison
func getCompareFailCause(n *node, which int, prevValue string, prevIndex uint64) string {
	switch which {
	case CompareIndexNotMatch:
		return fmt.Sprintf("[%v != %v]", prevIndex, n.ModifiedIndex)
	case CompareValueNotMatch:
		return fmt.Sprintf("[%v != %v]", prevValue, n.Value)
	default:
		return fmt.Sprintf("[%v != %v] [%v != %v]", prevValue, n.Value, prevIndex, n.ModifiedIndex)
	}
}

func (s *store) CompareAndSwap(nodePath string, prevValue string, prevIndex uint64,
	value string, expireOpts TTLOptionSet) (*Event, error,
) {
	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(CompareAndSwapSuccess)
			return
		}

		s.Stats.Inc(CompareAndSwapFail)
	}()

	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	if s.readonlySet.Contains(nodePath) {
		return nil, v2error.NewError(v2error.EcodeRootROnly, "/", s.CurrentIndex)
	}

	n, err := s.internalGet(nodePath)
	if err != nil {
		return nil, err
	}
	if n.IsDir() { // can only compare and swap file
		err = v2error.NewError(v2error.EcodeNotFile, nodePath, s.CurrentIndex)
		return nil, err
	}

	// If both of the prevValue and prevIndex are given, we will test both of them.
	// Command will be executed, only if both of the tests are successful.
	if ok, which := n.Compare(prevValue, prevIndex); !ok {
		cause := getCompareFailCause(n, which, prevValue, prevIndex)
		err = v2error.NewError(v2error.EcodeTestFailed, cause, s.CurrentIndex)
		return nil, err
	}

	if expireOpts.Refresh {
		value = n.Value
	}

	// update etcd index
	s.CurrentIndex++

	e := newEvent(CompareAndSwap, nodePath, s.CurrentIndex, n.CreatedIndex)
	e.EtcdIndex = s.CurrentIndex
	e.PrevNode = n.Repr(false, false, s.clock)
	eNode := e.NodeExtern

	// if test succeed, write the value
	if err := n.Write(value, s.CurrentIndex); err != nil {
		return nil, err
	}
	n.UpdateTTL(expireOpts.ExpireTime)

	// copy the value for safety
	valueCopy := value
	eNode.Value = &valueCopy
	eNode.Expiration, eNode.TTL = n.expirationAndTTL(s.clock) // 过期时间和 剩余时间 TTL

	if !expireOpts.Refresh {
		s.WatcherHub.notify(e)
	} else {
		e.SetRefresh()
		s.WatcherHub.add(e)
	}

	return e, nil
}

// Delete 删除节点,并删除该节点包含的   /0/members/8e9e05c52164694d  true,true
func (s *store) Delete(nodePath string, dir, recursive bool) (*Event, error) {
	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(DeleteSuccess)
			return
		}

		s.Stats.Inc(DeleteFail)
	}()

	nodePath = path.Clean(path.Join("/", nodePath))
	if s.readonlySet.Contains(nodePath) {
		return nil, v2error.NewError(v2error.EcodeRootROnly, "/", s.CurrentIndex)
	}

	// 递归意味着dir
	if recursive {
		dir = true
	}

	n, err := s.internalGet(nodePath)
	if err != nil { // 如果该节点不存在,则返回错误
		return nil, err
	}

	nextIndex := s.CurrentIndex + 1
	e := newEvent(Delete, nodePath, nextIndex, n.CreatedIndex)
	e.EtcdIndex = nextIndex
	e.PrevNode = n.Repr(false, false, s.clock)
	eNode := e.NodeExtern

	if n.IsDir() {
		eNode.Dir = true
	}
	callback := func(path string) {
		s.WatcherHub.notifyWatchers(e, path, true)
	}

	err = n.Remove(dir, recursive, callback)
	if err != nil {
		return nil, err
	}
	s.CurrentIndex++
	s.WatcherHub.notify(e) // 通知上层

	return e, nil
}

func (s *store) CompareAndDelete(nodePath string, prevValue string, prevIndex uint64) (*Event, error) {
	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(CompareAndDeleteSuccess)
			return
		}

		s.Stats.Inc(CompareAndDeleteFail)
	}()

	nodePath = path.Clean(path.Join("/", nodePath))

	n, err := s.internalGet(nodePath)
	if err != nil { // if the node does not exist, return error
		return nil, err
	}
	if n.IsDir() { // can only compare and delete file
		return nil, v2error.NewError(v2error.EcodeNotFile, nodePath, s.CurrentIndex)
	}

	// If both of the prevValue and prevIndex are given, we will test both of them.
	// Command will be executed, only if both of the tests are successful.
	if ok, which := n.Compare(prevValue, prevIndex); !ok {
		cause := getCompareFailCause(n, which, prevValue, prevIndex)
		return nil, v2error.NewError(v2error.EcodeTestFailed, cause, s.CurrentIndex)
	}

	// update etcd index
	s.CurrentIndex++

	e := newEvent(CompareAndDelete, nodePath, s.CurrentIndex, n.CreatedIndex)
	e.EtcdIndex = s.CurrentIndex
	e.PrevNode = n.Repr(false, false, s.clock)

	callback := func(path string) { // notify function
		// notify the watchers with deleted set true
		s.WatcherHub.notifyWatchers(e, path, true)
	}

	err = n.Remove(false, false, callback)
	if err != nil {
		return nil, err
	}

	s.WatcherHub.notify(e)

	return e, nil
}

func (s *store) Watch(key string, recursive, stream bool, sinceIndex uint64) (Watcher, error) {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	key = path.Clean(path.Join("/", key))
	if sinceIndex == 0 {
		sinceIndex = s.CurrentIndex + 1
	}
	// WatcherHub does not know about the current index, so we need to pass it in
	w, err := s.WatcherHub.watch(key, recursive, stream, sinceIndex, s.CurrentIndex)
	if err != nil {
		return nil, err
	}

	return w, nil
}

// walk 遍历所有nodePath并在每个目录上应用walkFunc
func (s *store) walk(nodePath string, walkFunc func(prev *node, component string) (*node, *v2error.Error)) (*node, *v2error.Error) {
	components := strings.Split(nodePath, "/")

	curr := s.Root
	var err *v2error.Error

	for i := 1; i < len(components); i++ {
		if len(components[i]) == 0 { // 忽略空字符
			return curr, nil
		}

		curr, err = walkFunc(curr, components[i])
		if err != nil {
			return nil, err
		}
	}

	return curr, nil
}

// Update updates the value/ttl of the node.
// If the node is a file, the value and the ttl can be updated.
// If the node is a directory, only the ttl can be updated.
func (s *store) Update(nodePath string, newValue string, expireOpts TTLOptionSet) (*Event, error) {
	var err *v2error.Error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(UpdateSuccess)
			return
		}

		s.Stats.Inc(UpdateFail)
	}()

	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	if s.readonlySet.Contains(nodePath) {
		return nil, v2error.NewError(v2error.EcodeRootROnly, "/", s.CurrentIndex)
	}

	currIndex, nextIndex := s.CurrentIndex, s.CurrentIndex+1

	n, err := s.internalGet(nodePath)
	if err != nil { // if the node does not exist, return error
		return nil, err
	}
	if n.IsDir() && len(newValue) != 0 {
		// if the node is a directory, we cannot update value to non-empty
		return nil, v2error.NewError(v2error.EcodeNotFile, nodePath, currIndex)
	}

	if expireOpts.Refresh {
		newValue = n.Value
	}

	e := newEvent(Update, nodePath, nextIndex, n.CreatedIndex)
	e.EtcdIndex = nextIndex
	e.PrevNode = n.Repr(false, false, s.clock)
	eNode := e.NodeExtern

	if err := n.Write(newValue, nextIndex); err != nil {
		return nil, fmt.Errorf("nodePath %v : %v", nodePath, err)
	}

	if n.IsDir() {
		eNode.Dir = true
	} else {
		// copy the value for safety
		newValueCopy := newValue
		eNode.Value = &newValueCopy
	}

	// update ttl
	n.UpdateTTL(expireOpts.ExpireTime)

	eNode.Expiration, eNode.TTL = n.expirationAndTTL(s.clock) // 过期时间和 剩余时间 TTL

	if !expireOpts.Refresh {
		s.WatcherHub.notify(e)
	} else {
		e.SetRefresh()
		s.WatcherHub.add(e)
	}

	s.CurrentIndex = nextIndex

	return e, nil
}

// DeleteExpiredKeys will delete all expired keys
func (s *store) DeleteExpiredKeys(cutoff time.Time) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	for {
		node := s.ttlKeyHeap.top()
		if node == nil || node.ExpireTime.After(cutoff) {
			break
		}

		s.CurrentIndex++
		e := newEvent(Expire, node.Path, s.CurrentIndex, node.CreatedIndex)
		e.EtcdIndex = s.CurrentIndex
		e.PrevNode = node.Repr(false, false, s.clock)
		if node.IsDir() {
			e.NodeExtern.Dir = true
		}

		callback := func(path string) { // notify function
			// notify the watchers with deleted set true
			s.WatcherHub.notifyWatchers(e, path, true)
		}

		s.ttlKeyHeap.pop()
		node.Remove(true, true, callback)

		s.Stats.Inc(ExpireCount)

		s.WatcherHub.notify(e)
	}
}

// Save saves the static state of the store system.
// It will not be able to save the state of watchers.
// It will not save the parent field of the node. Or there will
// be cyclic dependencies issue for the json package.
func (s *store) Save() ([]byte, error) {
	b, err := json.Marshal(s.Clone())
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) SaveNoCopy() ([]byte, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) Clone() Store {
	s.worldLock.RLock()

	clonedStore := newStore()
	clonedStore.CurrentIndex = s.CurrentIndex
	clonedStore.Root = s.Root.Clone()
	clonedStore.WatcherHub = s.WatcherHub.clone()
	clonedStore.Stats = s.Stats.clone()
	clonedStore.CurrentVersion = s.CurrentVersion

	s.worldLock.RUnlock()
	return clonedStore
}

// Recovery recovers the store system from a static state
// It needs to recover the parent field of the nodes.
// It needs to delete the expired nodes since the saved time and also
// needs to create monitoring goroutines.
func (s *store) Recovery(state []byte) error {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()
	err := json.Unmarshal(state, s)
	if err != nil {
		return err
	}

	s.ttlKeyHeap = newTtlKeyHeap()

	s.Root.recoverAndclean()
	return nil
}

func (s *store) JsonStats() []byte {
	s.Stats.Watchers = uint64(s.WatcherHub.count)
	return s.Stats.toJson()
}

func (s *store) HasTTLKeys() bool {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()
	return s.ttlKeyHeap.Len() != 0
}

// ------------------------------------------  OVER  --------------------------------------------------------

// checkDir 检查目录存不存在,不存在创建
func (s *store) checkDir(parent *node, dirName string) (*node, *v2error.Error) {
	node, ok := parent.Children[dirName]
	if ok {
		if node.IsDir() {
			return node, nil
		}
		return nil, v2error.NewError(v2error.EcodeNotDir, node.Path, s.CurrentIndex)
	}
	n := newDir(s, path.Join(parent.Path, dirName), s.CurrentIndex+1, parent, Permanent)
	parent.Children[dirName] = n
	return n, nil
}

// /0/members/8e9e05c52164694d/raftAttributes创建节点
func (s *store) internalCreate(nodePath string, dir bool, value string, unique, replace bool, expireTime time.Time, action string) (*Event, *v2error.Error) {
	currIndex, nextIndex := s.CurrentIndex, s.CurrentIndex+1

	if unique { // 在节点路径下附加唯一的项目
		nodePath += "/" + fmt.Sprintf("%020s", strconv.FormatUint(nextIndex, 10))
	}

	nodePath = path.Clean(path.Join("/", nodePath))

	// 我们不允许用户修改"/".
	if s.readonlySet.Contains(nodePath) {
		return nil, v2error.NewError(v2error.EcodeRootROnly, "/", currIndex)
	}

	if expireTime.Before(minExpireTime) {
		expireTime = Permanent
	}

	dirName, nodeName := path.Split(nodePath)
	d, err := s.walk(dirName, s.checkDir) // 检查节点目录,以及创建
	if err != nil {
		s.Stats.Inc(SetFail)
		err.Index = currIndex
		return nil, err
	}
	// create, /0/members/8e9e05c52164694d/raftAttributes, 1, 1
	e := newEvent(action, nodePath, nextIndex, nextIndex)
	eNode := e.NodeExtern

	n, _ := d.GetChild(nodeName)

	if n != nil {
		if replace {
			if n.IsDir() {
				return nil, v2error.NewError(v2error.EcodeNotFile, nodePath, currIndex)
			}
			e.PrevNode = n.Repr(false, false, s.clock)

			if err := n.Remove(false, false, nil); err != nil {
				return nil, err
			}
		} else {
			return nil, v2error.NewError(v2error.EcodeNodeExist, nodePath, currIndex)
		}
	}

	if !dir {
		valueCopy := value
		eNode.Value = &valueCopy
		// 生成新的树节点node,作为叶子节点
		n = newKV(s, nodePath, value, nextIndex, d, expireTime)
	} else {
		eNode.Dir = true
		n = newDir(s, nodePath, nextIndex, d, expireTime)
	}

	if err := d.Add(n); err != nil { // 添加父节点中,即挂到map中
		return nil, err
	}

	if !n.IsPermanent() { // 存在有效期
		s.ttlKeyHeap.push(n)
		eNode.Expiration, eNode.TTL = n.expirationAndTTL(s.clock) // 过期时间和 剩余时间 TTL
	}

	s.CurrentIndex = nextIndex

	return e, nil
}

// Version 检索存储的当前版本. <= CurrentIndex
func (s *store) Version() int {
	return s.CurrentVersion
}

// Index 检索存储的当前索引.
func (s *store) Index() uint64 {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()
	return s.CurrentIndex
}

// Get 返回一个get事件.如果递归为真,它将返回节点路径下的所有内容.如果sorted为真,它将按键对内容进行排序.
func (s *store) Get(nodePath string, recursive, sorted bool) (*Event, error) {
	// /0/members
	var err *v2error.Error

	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(GetSuccess)
			return
		}

		s.Stats.Inc(GetFail)
	}()

	n, err := s.internalGet(nodePath) // 没有 /0/members 这个node
	if err != nil {
		return nil, err
	}

	e := newEvent(Get, nodePath, n.ModifiedIndex, n.CreatedIndex)
	e.EtcdIndex = s.CurrentIndex                                 // 给事件分配索引
	e.NodeExtern.loadInternalNode(n, recursive, sorted, s.clock) // 加载node,主要是获取node中数据

	return e, nil
}

// Create 在nodePath创建节点.创建将有助于创建没有ttl的中间目录.如果该节点已经存在,创建将失败. 如果路径上的任何节点是一个文件,创建将失败.
func (s *store) Create(nodePath string, dir bool, value string, unique bool, expireOpts TTLOptionSet) (*Event, error) {
	var err *v2error.Error
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(CreateSuccess)
			return
		}

		s.Stats.Inc(CreateFail)
	}()

	// 创建一个内存节点, 有ttl放入ttlKeyHeap, 返回一个创建事件
	e, err := s.internalCreate(nodePath, dir, value, unique, false, expireOpts.ExpireTime, Create)
	if err != nil {
		return nil, err
	}

	e.EtcdIndex = s.CurrentIndex
	s.WatcherHub.notify(e) // ✅

	return e, nil
}

// InternalGet 获取给定nodePath的节点.
func (s *store) internalGet(nodePath string) (*node, *v2error.Error) {
	nodePath = path.Clean(path.Join("/", nodePath)) // /0/members

	walkFunc := func(parent *node, name string) (*node, *v2error.Error) {
		if !parent.IsDir() {
			err := v2error.NewError(v2error.EcodeNotDir, parent.Path, s.CurrentIndex)
			return nil, err
		}

		child, ok := parent.Children[name]
		if ok {
			return child, nil
		}

		return nil, v2error.NewError(v2error.EcodeKeyNotFound, path.Join(parent.Path, name), s.CurrentIndex)
	}

	n, err := s.walk(nodePath, walkFunc)
	if err != nil {
		return nil, err
	}
	return n, nil
}
