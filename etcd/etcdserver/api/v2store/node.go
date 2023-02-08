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
	"path"
	"sort"
	"time"

	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2error"

	"github.com/jonboulle/clockwork"
)

// 对比函数结果的说明
const (
	CompareMatch         = iota // 匹配
	CompareIndexNotMatch        // 索引不匹配
	CompareValueNotMatch        // 值不匹配
	CompareNotMatch             // 不匹配
)

var Permanent time.Time // 永久性时间,默认零值

// node is the basic element in the store system.
// A key-value pair will have a string value
// A directory will have a children map
type node struct {
	Path          string
	CreatedIndex  uint64
	ModifiedIndex uint64
	Parent        *node `json:"-"` // 不应该对这个字段进行编码！避免循环依赖.
	ExpireTime    time.Time
	Value         string           // 键值对
	Children      map[string]*node // 目录
	store         *store           // 对该节点所连接的商店的引用.
}

// newKV creates a Key-Value pair
func newKV(store *store, nodePath string, value string, createdIndex uint64, parent *node, expireTime time.Time) *node {
	return &node{
		Path:          nodePath,
		CreatedIndex:  createdIndex,
		ModifiedIndex: createdIndex,
		Parent:        parent,
		store:         store,
		ExpireTime:    expireTime,
		Value:         value,
	}
}

// Write function set the value of the node to the given value.
// If the receiver node is a directory, a "Not A File" error will be returned.
func (n *node) Write(value string, index uint64) *v2error.Error {
	if n.IsDir() {
		return v2error.NewError(v2error.EcodeNotFile, "", n.store.CurrentIndex)
	}

	n.Value = value
	n.ModifiedIndex = index

	return nil
}

// Remove 清理node包含的子数据
func (n *node) Remove(dir, recursive bool, callback func(path string)) *v2error.Error {
	if !n.IsDir() { // key-value pair
		_, name := path.Split(n.Path)

		// find its parent and remove the node from the map
		if n.Parent != nil && n.Parent.Children[name] == n {
			delete(n.Parent.Children, name)
		}

		if callback != nil {
			callback(n.Path)
		}

		if !n.IsPermanent() {
			n.store.ttlKeyHeap.remove(n)
		}

		return nil
	}

	if !dir {
		// cannot delete a directory without dir set to true
		return v2error.NewError(v2error.EcodeNotFile, n.Path, n.store.CurrentIndex)
	}

	if len(n.Children) != 0 && !recursive {
		// cannot delete a directory if it is not empty and the operation
		// is not recursive
		return v2error.NewError(v2error.EcodeDirNotEmpty, n.Path, n.store.CurrentIndex)
	}

	for _, child := range n.Children { // delete all children
		child.Remove(true, true, callback)
	}

	// delete self
	_, name := path.Split(n.Path)
	if n.Parent != nil && n.Parent.Children[name] == n {
		delete(n.Parent.Children, name)

		if callback != nil {
			callback(n.Path)
		}

		if !n.IsPermanent() {
			n.store.ttlKeyHeap.remove(n)
		}
	}

	return nil
}

func (n *node) UpdateTTL(expireTime time.Time) {
	if !n.IsPermanent() {
		if expireTime.IsZero() {
			// from ttl to permanent
			n.ExpireTime = expireTime
			// remove from ttl heap
			n.store.ttlKeyHeap.remove(n)
			return
		}

		// update ttl
		n.ExpireTime = expireTime
		// update ttl heap
		n.store.ttlKeyHeap.update(n)
		return
	}

	if expireTime.IsZero() {
		return
	}

	// from permanent to ttl
	n.ExpireTime = expireTime
	// push into ttl heap
	n.store.ttlKeyHeap.push(n)
}

// Compare function compares node index and value with provided ones.
// second result value explains result and equals to one of Compare.. constants
func (n *node) Compare(prevValue string, prevIndex uint64) (ok bool, which int) {
	indexMatch := prevIndex == 0 || n.ModifiedIndex == prevIndex
	valueMatch := prevValue == "" || n.Value == prevValue
	ok = valueMatch && indexMatch
	switch {
	case valueMatch && indexMatch:
		which = CompareMatch
	case indexMatch && !valueMatch:
		which = CompareValueNotMatch
	case valueMatch && !indexMatch:
		which = CompareIndexNotMatch
	default:
		which = CompareNotMatch
	}
	return ok, which
}

// Clone function clone the node recursively and return the new node.
// If the node is a directory, it will clone all the content under this directory.
// If the node is a key-value pair, it will clone the pair.
func (n *node) Clone() *node {
	if !n.IsDir() {
		newkv := newKV(n.store, n.Path, n.Value, n.CreatedIndex, n.Parent, n.ExpireTime)
		newkv.ModifiedIndex = n.ModifiedIndex
		return newkv
	}

	clone := newDir(n.store, n.Path, n.CreatedIndex, n.Parent, n.ExpireTime)
	clone.ModifiedIndex = n.ModifiedIndex

	for key, child := range n.Children {
		clone.Children[key] = child.Clone()
	}

	return clone
}

// recoverAndclean function help to do recovery.
// Two things need to be done: 1. recovery structure; 2. delete expired nodes
//
// If the node is a directory, it will help recover children's parent pointer and recursively
// call this function on its children.
// We check the expire last since we need to recover the whole structure first and add all the
// notifications into the event history.
func (n *node) recoverAndclean() {
	if n.IsDir() {
		for _, child := range n.Children {
			child.Parent = n
			child.store = n.store
			child.recoverAndclean()
		}
	}

	if !n.ExpireTime.IsZero() {
		n.store.ttlKeyHeap.push(n)
	}
}

// --------------------------------------  OVER  -----------------------------------------------

// List 返回当前节点下的所有节点
func (n *node) List() ([]*node, *v2error.Error) {
	if !n.IsDir() {
		return nil, v2error.NewError(v2error.EcodeNotDir, "", n.store.CurrentIndex)
	}

	nodes := make([]*node, len(n.Children))

	i := 0
	for _, node := range n.Children {
		nodes[i] = node
		i++
	}

	return nodes, nil
}

// GetChild 返回目录节点
func (n *node) GetChild(name string) (*node, *v2error.Error) {
	if !n.IsDir() {
		return nil, v2error.NewError(v2error.EcodeNotDir, n.Path, n.store.CurrentIndex)
	}

	child, ok := n.Children[name]

	if ok {
		return child, nil
	}

	return nil, nil
}

// Add 添加一个子节点
func (n *node) Add(child *node) *v2error.Error {
	if !n.IsDir() { // /0/members/8e9e05c52164694d
		return v2error.NewError(v2error.EcodeNotDir, "", n.store.CurrentIndex)
	}

	_, name := path.Split(child.Path)

	if _, ok := n.Children[name]; ok {
		return v2error.NewError(v2error.EcodeNodeExist, "", n.store.CurrentIndex)
	}

	n.Children[name] = child

	return nil
}

// Repr 递归的 封信过期时间、剩余时间,跳过隐藏节点
func (n *node) Repr(recursive, sorted bool, clock clockwork.Clock) *NodeExtern {
	if n.IsDir() {
		node := &NodeExtern{
			Key:           n.Path,
			Dir:           true,
			ModifiedIndex: n.ModifiedIndex,
			CreatedIndex:  n.CreatedIndex,
		}
		node.Expiration, node.TTL = n.expirationAndTTL(clock) // 过期时间和 剩余时间 TTL
		if !recursive {
			return node
		}
		children, _ := n.List()
		node.ExternNodes = make(NodeExterns, len(children))
		i := 0
		for _, child := range children {
			if child.IsHidden() { // 跳过隐藏节点
				continue
			}
			node.ExternNodes[i] = child.Repr(recursive, sorted, clock)
			i++
		}
		node.ExternNodes = node.ExternNodes[:i]
		if sorted {
			sort.Sort(node.ExternNodes)
		}
		return node
	}

	// since n.Value could be changed later, so we need to copy the value out
	value := n.Value
	node := &NodeExtern{
		Key:           n.Path,
		Value:         &value,
		ModifiedIndex: n.ModifiedIndex,
		CreatedIndex:  n.CreatedIndex,
	}
	node.Expiration, node.TTL = n.expirationAndTTL(clock) // 过期时间和 剩余时间 TTL
	return node
}

// 过期时间和 剩余时间 TTL
func (n *node) expirationAndTTL(clock clockwork.Clock) (*time.Time, int64) {
	if !n.IsPermanent() {
		ttlN := n.ExpireTime.Sub(clock.Now()) // 还有多长时间过期
		ttl := ttlN / time.Second
		if (ttlN % time.Second) > 0 {
			ttl++ // 整除+1
		}
		t := n.ExpireTime.UTC()
		return &t, int64(ttl)
	}
	return nil, 0
}

// newDir 创建一个目录
func newDir(store *store, nodePath string, createdIndex uint64, parent *node, expireTime time.Time) *node {
	return &node{
		Path:          nodePath,
		CreatedIndex:  createdIndex,
		ModifiedIndex: createdIndex,
		Parent:        parent,
		ExpireTime:    expireTime,
		Children:      make(map[string]*node),
		store:         store,
	}
}

// IsHidden 判断节点名字是不是以 _ 开头   /0/members/_hidden
func (n *node) IsHidden() bool {
	_, name := path.Split(n.Path)
	return name[0] == '_'
}

// IsPermanent 函数检查该节点是否为永久节点.
func (n *node) IsPermanent() bool {
	// 我们使用一个未初始化的time.Time来表示该节点是一个永久的节点. 未初始化的time.Time应该等于0.
	return n.ExpireTime.IsZero()
}

func (n *node) IsDir() bool {
	return n.Children != nil
}

func (n *node) Read() (string, *v2error.Error) {
	if n.IsDir() {
		return "", v2error.NewError(v2error.EcodeNotFile, "", n.store.CurrentIndex)
	}

	return n.Value, nil
}
