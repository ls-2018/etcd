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

package v2stats

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/ls-2018/etcd_cn/raft"
)

// ServerStats 封装了关于EtcdServer及其与集群其他成员通信的各种统计信息
type ServerStats struct {
	serverStats
	sync.Mutex
}

func NewServerStats(name, id string) *ServerStats {
	ss := &ServerStats{
		serverStats: serverStats{
			Name: name,
			ID:   id,
		},
	}
	now := time.Now()
	ss.StartTime = now
	ss.LeaderInfo.StartTime = now
	ss.sendRateQueue = &statsQueue{back: -1}
	ss.recvRateQueue = &statsQueue{back: -1}
	return ss
}

type serverStats struct {
	Name       string         `json:"name"`      // 该节点的name .
	ID         string         `json:"id"`        // 每个节点的唯一标识符.
	State      raft.StateType `json:"state"`     // 该节点在Raft 协议里的角色, Leader 或Follower .
	StartTime  time.Time      `json:"startTime"` // 该etcd server 的启动时间.
	LeaderInfo struct {       //
		Name      string    `json:"leader"`    //
		Uptime    string    `json:"uptime"`    // 集群当前Leader 的在任时长.
		StartTime time.Time `json:"startTime"` // leader首次通信的时间
	} `json:"leaderInfo"` //
	sendRateQueue        *statsQueue // 发送消息的队列
	SendAppendRequestCnt uint64      `json:"sendAppendRequestCnt"`        // 该节点已发送的append 请求数.
	SendingPkgRate       float64     `json:"sendPkgRate,omitempty"`       // 该节点每秒发送的请求数（ 只有Follower 才有, 并且单 节点集群没有这项数据） .
	SendingBandwidthRate float64     `json:"sendBandwidthRate,omitempty"` // 该节点每秒发送的字节（只有Follower 才有,且单节点集群没有这项数据） .
	recvRateQueue        *statsQueue // 处理接受消息的队列
	RecvAppendRequestCnt uint64      `json:"recvAppendRequestCnt,"`       // 该节点己处理的append 请求数.
	RecvingPkgRate       float64     `json:"recvPkgRate,omitempty"`       // 该节点每秒收到的请求数（只有Follower 才有） .
	RecvingBandwidthRate float64     `json:"recvBandwidthRate,omitempty"` // 该节点每秒收到的字节（只有Follower 才有） .
}

func (ss *ServerStats) JSON() []byte {
	ss.Lock()
	stats := ss.serverStats
	stats.SendingPkgRate, stats.SendingBandwidthRate = stats.sendRateQueue.Rate()
	stats.RecvingPkgRate, stats.RecvingBandwidthRate = stats.recvRateQueue.Rate()
	stats.LeaderInfo.Uptime = time.Since(stats.LeaderInfo.StartTime).String()
	ss.Unlock()
	b, err := json.Marshal(stats)
	// TODO(jonboulle): appropriate error handling?
	if err != nil {
		log.Printf("stats: error marshalling etcd stats: %v", err)
	}
	return b
}

// RecvAppendReq 在收到来自leader的AppendRequest后,更新ServerStats.
func (ss *ServerStats) RecvAppendReq(leader string, reqSize int) {
	ss.Lock()
	defer ss.Unlock()

	now := time.Now()

	ss.State = raft.StateFollower
	if leader != ss.LeaderInfo.Name {
		ss.LeaderInfo.Name = leader
		ss.LeaderInfo.StartTime = now
	}

	ss.recvRateQueue.Insert(
		&RequestStats{
			SendingTime: now,
			Size:        reqSize,
		},
	)
	ss.RecvAppendRequestCnt++
}

// SendAppendReq updates the ServerStats in response to an AppendRequest
// being sent by this etcd
func (ss *ServerStats) SendAppendReq(reqSize int) {
	ss.Lock()
	defer ss.Unlock()

	ss.becomeLeader()

	ss.sendRateQueue.Insert(
		&RequestStats{
			SendingTime: time.Now(),
			Size:        reqSize,
		},
	)

	ss.SendAppendRequestCnt++
}

func (ss *ServerStats) BecomeLeader() {
	ss.Lock()
	defer ss.Unlock()
	ss.becomeLeader()
}

func (ss *ServerStats) becomeLeader() {
	if ss.State != raft.StateLeader {
		ss.State = raft.StateLeader
		ss.LeaderInfo.Name = ss.ID
		ss.LeaderInfo.StartTime = time.Now()
	}
}
