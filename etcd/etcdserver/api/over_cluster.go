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

package api

import (
	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/membership"

	"github.com/coreos/go-semver/semver"
)

type Cluster interface {
	ID() types.ID                  // 集群ID
	ClientURLs() []string          // 返回该集群正在监听客户端请求的所有URL的集合.
	Members() []*membership.Member // 集群成员,排序之后的
	Member(id types.ID) *membership.Member
	Version() *semver.Version
}
