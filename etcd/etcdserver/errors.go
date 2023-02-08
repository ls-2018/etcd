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

package etcdserver

import (
	"errors"
	"fmt"
)

var (
	ErrUnknownMethod                 = errors.New("etcdserver: 未知的请求方法")
	ErrStopped                       = errors.New("etcdserver: etcd停止")
	ErrCanceled                      = errors.New("etcdserver: 请求取消")
	ErrTimeout                       = errors.New("etcdserver: 请求超时")
	ErrTimeoutDueToLeaderFail        = errors.New("etcdserver: 请求超时,可能是由于之前的领导者失败了")
	ErrTimeoutDueToConnectionLost    = errors.New("etcdserver: 请求超时,可能是由于连接丢失")
	ErrTimeoutLeaderTransfer         = errors.New("etcdserver: 请求超时,领导者转移时间过长")
	ErrLeaderChanged                 = errors.New("etcdserver: 领导者转移了")
	ErrNotEnoughStartedMembers       = errors.New("etcdserver: 由于启动的成员不足,重新配置失败")
	ErrLearnerNotReady               = errors.New("etcdserver: 只能提拔与leader同步的learner成员")
	ErrNoLeader                      = errors.New("etcdserver: 没有leader")
	ErrNotLeader                     = errors.New("etcdserver: 不是leader")
	ErrRequestTooLarge               = errors.New("etcdserver: 请求太多")
	ErrNoSpace                       = errors.New("etcdserver: 没有空间")
	ErrTooManyRequests               = errors.New("etcdserver: 太多的请求")
	ErrUnhealthy                     = errors.New("etcdserver: 集群不健康")
	ErrKeyNotFound                   = errors.New("etcdserver: key没找到")
	ErrCorrupt                       = errors.New("etcdserver: 损坏的集群")
	ErrBadLeaderTransferee           = errors.New("etcdserver: bad leader transferee")
	ErrClusterVersionUnavailable     = errors.New("etcdserver: cluster version not found during downgrade")
	ErrWrongDowngradeVersionFormat   = errors.New("etcdserver: wrong downgrade target version format")
	ErrInvalidDowngradeTargetVersion = errors.New("etcdserver: invalid downgrade target version")
	ErrDowngradeInProcess            = errors.New("etcdserver: cluster has a downgrade job in progress")
	ErrNoInflightDowngrade           = errors.New("etcdserver: no inflight downgrade job")
)

type DiscoveryError struct {
	Op  string
	Err error
}

func (e DiscoveryError) Error() string {
	return fmt.Sprintf("failed to %s discovery cluster (%v)", e.Op, e.Err)
}
