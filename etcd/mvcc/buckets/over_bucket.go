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

package buckets

import (
	"bytes"

	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"
)

var (
	Key     = backend.Bucket(bucket{id: 1, name: []byte("key"), safeRangeBucket: true})
	Meta    = backend.Bucket(bucket{id: 2, name: []byte("meta"), safeRangeBucket: false})
	Lease   = backend.Bucket(bucket{id: 3, name: []byte("lease"), safeRangeBucket: false})
	Alarm   = backend.Bucket(bucket{id: 4, name: []byte("alarm"), safeRangeBucket: false})
	Cluster = backend.Bucket(bucket{id: 5, name: []byte("cluster"), safeRangeBucket: false})

	Members        = backend.Bucket(bucket{id: 10, name: []byte("members"), safeRangeBucket: false})
	MembersRemoved = backend.Bucket(bucket{id: 11, name: []byte("members_removed"), safeRangeBucket: false})

	Auth      = backend.Bucket(bucket{id: 20, name: []byte("auth"), safeRangeBucket: false})
	AuthUsers = backend.Bucket(bucket{id: 21, name: []byte("authUsers"), safeRangeBucket: false})
	AuthRoles = backend.Bucket(bucket{id: 22, name: []byte("authRoles"), safeRangeBucket: false})

	Test = backend.Bucket(bucket{id: 100, name: []byte("test"), safeRangeBucket: false})
)

type bucket struct {
	id              backend.BucketID
	name            []byte
	safeRangeBucket bool
}

func (b bucket) ID() backend.BucketID    { return b.id }
func (b bucket) Name() []byte            { return b.name }
func (b bucket) String() string          { return string(b.Name()) }
func (b bucket) IsSafeRangeBucket() bool { return b.safeRangeBucket }

var (
	MetaConsistentIndexKeyName = []byte("consistent_index")
	MetaTermKeyName            = []byte("term")
)

// DefaultIgnores 定义在哈希检查中要忽略的桶和键.
func DefaultIgnores(bucket, key []byte) bool {
	// consistent index & term might be changed due to v2 internal sync, which
	// is not controllable by the user.
	return bytes.Compare(bucket, Meta.Name()) == 0 &&
		(bytes.Compare(key, MetaTermKeyName) == 0 || bytes.Compare(key, MetaConsistentIndexKeyName) == 0)
}
