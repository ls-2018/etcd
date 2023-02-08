// Copyright 2021 The etcd Authors
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
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2store"
)

// IsMetaStoreOnly 验证给定的`store`是否只包含元信息(成员,版本);可以从后端(storev3)恢复,而不是用户数据.
func IsMetaStoreOnly(store v2store.Store) (bool, error) {
	event, err := store.Get("/", true, false)
	if err != nil {
		return false, err
	}
	for _, n := range event.NodeExtern.ExternNodes {
		if n.Key != storePrefix && n.ExternNodes.Len() > 0 {
			return false, nil
		}
	}

	return true, nil
}
