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

// Package pbutil defines interfaces for handling Protocol Buffer objects.
package pbutil

import "fmt"

type Marshaler interface {
	Marshal() (data []byte, err error)
}

type Unmarshaler interface {
	Unmarshal(data []byte) error
}

// MustMarshal OK
func MustMarshal(m Marshaler) []byte {
	d, err := m.Marshal()
	if err != nil {
		panic(fmt.Sprintf("序列化不应该失败 (%v)", err))
	}
	return d
}

func MustUnmarshal(um Unmarshaler, data []byte) {
	if len(data) == 0 {
		return
	}
	if err := um.Unmarshal(data); err != nil {
		panic(fmt.Sprintf("反序列化不应该失败(%v)", err))
	}
}

func MaybeUnmarshal(um Unmarshaler, data []byte) bool {
	if len(data) == 0 {
		return false
	}
	if err := um.Unmarshal(data); err != nil {
		return false
	}
	return true
}

func GetBool(v *bool) (vv bool, set bool) {
	if v == nil {
		return false, false
	}
	return *v, true
}

func Boolp(b bool) *bool { return &b }
