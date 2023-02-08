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

// Inflights  在 Raft 中存储的是已发送给 Follower 的 MsgApp 消息,但没有收到 MsgAppResp 的消息 最大Index  值.
// 简单的说就是 Leader 发送一个消息给 Follower,Leader 在对应的 Follower 状态维护结构(progress)中,
// 将这个消息的 ID 记录在 inFlight 中, 当 Follower 收到消息之后,告知 Leader 收到了这个 ID 的消息,Leader 将从 inFlight 中删除,
// 表示 Follower 已经接收,否则如果 Follower 在指定时间内没有响应,Leader 会根据一定策略进行重发.
// 一批一批的发送
type Inflights struct {
	start  int      // 记录最旧的那个未被响应的消息,在buffer中的位置
	count  int      // 已发送,但未响应的消息总数
	size   int      // buffer的最大长度
	buffer []uint64 // 存储ID值           通过一个具有最大长度(size)的数组([]uint64)构造成一个环形数组.
}

// NewInflights sets up an Inflights that allows up to 'size' inflight messages.
func NewInflights(size int) *Inflights {
	return &Inflights{
		size: size,
	}
}

// Clone 返回一个与相同的Inflights但不共享buffer.
func (in *Inflights) Clone() *Inflights {
	ins := *in
	ins.buffer = append([]uint64(nil), in.buffer...)
	return &ins
}

// Add 记录待确认的日志索引, inflight每批发送的消息中 最新的日志索引
func (in *Inflights) Add(inflight uint64) {
	if in.Full() { // ok
		panic("不能添加到一个已满的inflights")
	}
	next := in.start + in.count // 是环形数组,下一个要放的位置
	size := in.size
	if next >= size {
		next -= size
	} // 回执
	if next >= len(in.buffer) { // 判断实际存储有没有足够空间
		in.grow()
	}
	in.buffer[next] = inflight // 放入数据
	in.count++                 // 计数++
}

// grow 按需grow 首次>=size就不在执行
func (in *Inflights) grow() {
	newSize := len(in.buffer) * 2
	if newSize == 0 {
		newSize = 1
	} else if newSize > in.size {
		newSize = in.size
	}
	newBuffer := make([]uint64, newSize)
	copy(newBuffer, in.buffer)
	in.buffer = newBuffer
}

// FreeLE 释放日志索引小于to的日志
func (in *Inflights) FreeLE(to uint64) {
	// to传过来的是索引
	if in.count == 0 || to < in.buffer[in.start] {
		// 窗口左侧
		return
	}

	idx := in.start
	var i int
	// 从 start 开始,直到找到最大且小于 to 的元素位置
	for i = 0; i < in.count; i++ { // 当前有多少个条消息
		if to < in.buffer[idx] { // found the first large inflight
			break
		}
		// 判断有没有越界
		size := in.size
		if idx++; idx >= size {
			idx -= size
		}
	}
	in.count -= i  // 释放的日志条数
	in.start = idx // 日志 跳转的位置
	if in.count == 0 {
		// 如果没有fly日志了,就将start重置为0
		in.start = 0
	}
}

// FreeFirstOne 释放第一条日志
func (in *Inflights) FreeFirstOne() { in.FreeLE(in.buffer[in.start]) }

// Full OK
func (in *Inflights) Full() bool {
	return in.count == in.size
}

// Count 当前未确认的消息数
func (in *Inflights) Count() int { return in.count }

// reset 释放所有未确认的消息
func (in *Inflights) reset() {
	in.count = 0
	in.start = 0
}
