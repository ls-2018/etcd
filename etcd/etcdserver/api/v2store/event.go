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

const (
	Get              = "get"
	Create           = "create"
	Set              = "set"
	Update           = "update"
	Delete           = "delete"
	CompareAndSwap   = "compareAndSwap"
	CompareAndDelete = "compareAndDelete"
	Expire           = "expire"
)

type Event struct {
	Action     string      `json:"action"`
	NodeExtern *NodeExtern `json:"node,omitempty"`
	PrevNode   *NodeExtern `json:"prevNode,omitempty"`
	EtcdIndex  uint64      `json:"-"`
	Refresh    bool        `json:"refresh,omitempty"`
}

// 节点变更事件、包括节点的创建、删除...
func newEvent(action string, key string, modifiedIndex, createdIndex uint64) *Event {
	n := &NodeExtern{
		Key:           key,
		ModifiedIndex: modifiedIndex,
		CreatedIndex:  createdIndex,
	}

	return &Event{
		Action:     action,
		NodeExtern: n,
	}
}

func (e *Event) IsCreated() bool {
	if e.Action == Create {
		return true
	}
	return e.Action == Set && e.PrevNode == nil
}

func (e *Event) Index() uint64 {
	return e.NodeExtern.ModifiedIndex
}

func (e *Event) Clone() *Event {
	return &Event{
		Action:     e.Action,
		EtcdIndex:  e.EtcdIndex,
		NodeExtern: e.NodeExtern.Clone(),
		PrevNode:   e.PrevNode.Clone(),
	}
}

func (e *Event) SetRefresh() {
	e.Refresh = true
}
