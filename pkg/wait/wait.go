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

// Package wait provides utility functions for polling, listening using Go
// channel.
package wait

import (
	"log"
	"sync"
)

const (
	// To avoid lock contention we use an array of list struct (rw mutex & map)
	// for the id argument, we apply mod operation and uses its remainder to
	// index into the array and find the corresponding element.
	defaultListElementLength = 64
)

// Wait 是一个接口提供等待和触发与ID相关的事件的能力.
type Wait interface {
	Register(id uint64) <-chan interface{} // waits返回一个在给定ID上等待的chan.当Trigger以相同的ID被调用时这个chan将被触发.
	Trigger(id uint64, x interface{})      // 触发具有给定ID的等待通道.设置结果
	IsRegistered(id uint64) bool
}

type list struct {
	e []listElement
}

// 64个槽位 接收 响应
type listElement struct {
	l sync.RWMutex
	m map[uint64]chan interface{} // 每个请求,以及待返回的channel
}

func New() Wait {
	res := list{
		e: make([]listElement, defaultListElementLength), // 64
	}
	for i := 0; i < len(res.e); i++ {
		res.e[i].m = make(map[uint64]chan interface{})
	}
	return &res
}

func (w *list) Register(id uint64) <-chan interface{} {
	idx := id % defaultListElementLength // 64
	newCh := make(chan interface{}, 1)
	w.e[idx].l.Lock()
	defer w.e[idx].l.Unlock()
	// 判断 请求存不存在
	if _, ok := w.e[idx].m[id]; !ok {
		w.e[idx].m[id] = newCh
	} else {
		log.Panicf("重复的 id %x", id)
	}
	return newCh
}

func (w *list) Trigger(id uint64, x interface{}) {
	idx := id % defaultListElementLength
	w.e[idx].l.Lock()
	ch := w.e[idx].m[id]
	delete(w.e[idx].m, id)
	w.e[idx].l.Unlock()
	if ch != nil {
		ch <- x
		close(ch)
	}
}

func (w *list) IsRegistered(id uint64) bool {
	idx := id % defaultListElementLength
	w.e[idx].l.RLock()
	defer w.e[idx].l.RUnlock()
	_, ok := w.e[idx].m[id]
	return ok
}

type waitWithResponse struct {
	ch <-chan interface{}
}

func NewWithResponse(ch <-chan interface{}) Wait {
	return &waitWithResponse{ch: ch}
}

func (w *waitWithResponse) Register(id uint64) <-chan interface{} {
	return w.ch
}
func (w *waitWithResponse) Trigger(id uint64, x interface{}) {}
func (w *waitWithResponse) IsRegistered(id uint64) bool {
	panic("waitWithResponse.IsRegistered() 不应该被调用")
}
