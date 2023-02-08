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
	"flag"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
)

// UniqueURLs 包含独特的URL,有非URL例外.
type UniqueURLs struct {
	Values  map[string]struct{} // url->struct{}
	uss     []url.URL
	Allowed map[string]struct{} // url,url -> struct{}
}

var _ flag.Value = &UniqueURLs{}

// Set parses  http://127.0.0.1:2380,http://10.1.1.2:80
func (us *UniqueURLs) Set(s string) error {
	if _, ok := us.Values[s]; ok {
		return nil
	}
	if _, ok := us.Allowed[s]; ok {
		us.Values[s] = struct{}{}
		return nil
	}
	ss, err := types.NewURLs(strings.Split(s, ","))
	if err != nil {
		return err
	}
	us.Values = make(map[string]struct{})
	us.uss = make([]url.URL, 0)
	for _, v := range ss {
		us.Values[v.String()] = struct{}{}
		us.uss = append(us.uss, v)
	}
	return nil
}

// String implements "flag.Value" interface.
func (us *UniqueURLs) String() string {
	all := make([]string, 0, len(us.Values))
	for u := range us.Values {
		all = append(all, u)
	}
	sort.Strings(all)
	return strings.Join(all, ",")
}

// NewUniqueURLsWithExceptions 实现 "url.URL "切片作为flag.Value接口.
func NewUniqueURLsWithExceptions(s string, exceptions ...string) *UniqueURLs {
	us := &UniqueURLs{Values: make(map[string]struct{}), Allowed: make(map[string]struct{})}
	for _, v := range exceptions {
		us.Allowed[v] = struct{}{}
	}
	if s == "" {
		return us
	}
	if err := us.Set(s); err != nil {
		panic(fmt.Sprintf("new UniqueURLs不应该失败: %v", err))
	}
	return us
}

// UniqueURLsFromFlag 从该标志获取的url返回一个切片.
func UniqueURLsFromFlag(fs *flag.FlagSet, urlsFlagName string) []url.URL {
	return (*fs.Lookup(urlsFlagName).Value.(*UniqueURLs)).uss
}

// UniqueURLsMapFromFlag returns a map from url strings got from the flag.
func UniqueURLsMapFromFlag(fs *flag.FlagSet, urlsFlagName string) map[string]struct{} {
	return (*fs.Lookup(urlsFlagName).Value.(*UniqueURLs)).Values
}
