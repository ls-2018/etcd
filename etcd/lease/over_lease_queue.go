// Copyright 2018 The etcd Authors
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

package lease

import (
	"container/heap"
	"time"
)

type LeaseWithTime struct {
	id    LeaseID
	time  time.Time
	index int
}

type LeaseQueue []*LeaseWithTime

func (pq LeaseQueue) Len() int { return len(pq) }

func (pq LeaseQueue) Less(i, j int) bool {
	return pq[i].time.Before(pq[j].time)
}

func (pq LeaseQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *LeaseQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*LeaseWithTime)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *LeaseQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

var _ heap.Interface = &LeaseQueue{}

// ExpiredNotifier  一个租约只能保存一个key,`Register`将更新相应的租约时间.
// 用于通知lessor移除过期租约的队列
type ExpiredNotifier struct {
	m     map[LeaseID]*LeaseWithTime
	queue LeaseQueue
}

// 租约到期通知器
func newLeaseExpiredNotifier() *ExpiredNotifier {
	return &ExpiredNotifier{
		m:     make(map[LeaseID]*LeaseWithTime),
		queue: make(LeaseQueue, 0),
	}
}

// Init ok
func (mq *ExpiredNotifier) Init() {
	heap.Init(&mq.queue)
	mq.m = make(map[LeaseID]*LeaseWithTime)
	for _, item := range mq.queue {
		mq.m[item.id] = item
	}
}

// RegisterOrUpdate 注册或更新管理的租约
func (mq *ExpiredNotifier) RegisterOrUpdate(item *LeaseWithTime) {
	if old, ok := mq.m[item.id]; ok {
		old.time = item.time           // 过期时间
		heap.Fix(&mq.queue, old.index) // 当元素值,发生改变, Fix会重新调整顺序
	} else {
		heap.Push(&mq.queue, item) // 创建
		mq.m[item.id] = item
	}
}

func (mq *ExpiredNotifier) Unregister() *LeaseWithTime {
	item := heap.Pop(&mq.queue).(*LeaseWithTime)
	delete(mq.m, item.id)
	return item
}

// Poll 获取第一个要快要过期的租约
func (mq *ExpiredNotifier) Poll() *LeaseWithTime {
	if mq.Len() == 0 {
		return nil
	}
	return mq.queue[0]
}

func (mq *ExpiredNotifier) Len() int {
	return len(mq.m)
}
