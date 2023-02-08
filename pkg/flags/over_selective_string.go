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

package flags

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

type SelectiveStringValue struct {
	v      string
	valids map[string]struct{}
}

// Set 检验参数是否为允许值中的有效成员 的有效成员,然后再设置基本的标志值.
func (ss *SelectiveStringValue) Set(s string) error {
	if _, ok := ss.valids[s]; ok {
		ss.v = s
		return nil
	}
	return errors.New("无效的值")
}

func (ss *SelectiveStringValue) String() string {
	return ss.v
}

func (ss *SelectiveStringValue) Valids() []string {
	s := make([]string, 0, len(ss.valids))
	for k := range ss.valids {
		s = append(s, k)
	}
	sort.Strings(s)
	return s
}

func NewSelectiveStringValue(valids ...string) *SelectiveStringValue {
	vm := make(map[string]struct{})
	for _, v := range valids {
		vm[v] = struct{}{}
	}
	return &SelectiveStringValue{valids: vm, v: valids[0]}
}

// SelectiveStringsValue 实现了 flag.Value 接口.
type SelectiveStringsValue struct {
	vs     []string
	valids map[string]struct{}
}

func (ss *SelectiveStringsValue) Set(s string) error {
	vs := strings.Split(s, ",")
	for i := range vs {
		if _, ok := ss.valids[vs[i]]; ok {
			ss.vs = append(ss.vs, vs[i])
		} else {
			return fmt.Errorf("invalid value %q", vs[i])
		}
	}
	sort.Strings(ss.vs)
	return nil
}

// OK
func (ss *SelectiveStringsValue) String() string {
	return strings.Join(ss.vs, ",")
}

// Valids OK
func (ss *SelectiveStringsValue) Valids() []string {
	s := make([]string, 0, len(ss.valids))
	for k := range ss.valids {
		s = append(s, k)
	}
	sort.Strings(s)
	return s
}

func NewSelectiveStringsValue(valids ...string) *SelectiveStringsValue {
	vm := make(map[string]struct{})
	for _, v := range valids {
		vm[v] = struct{}{}
	}
	return &SelectiveStringsValue{valids: vm, vs: []string{}}
}
