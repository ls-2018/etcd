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

package types

import (
	"fmt"
	"sort"
	"strings"
)

// URLsMap 节点名字与通信地址的对应
type URLsMap map[string]URLs

// NewURLsMap 返回URLsMap 【节点名字与通信地址的对应】
// mach0=http://1.1.1.1:2380,mach0=http://2.2.2.2::2380,mach1=http://3.3.3.3:2380,mach2=http://4.4.4.4:2380
// 类型转换
func NewURLsMap(s string) (URLsMap, error) {
	m := parse(s)

	cl := URLsMap{}
	for name, urls := range m {
		us, err := NewURLs(urls)
		if err != nil {
			return nil, err
		}
		cl[name] = us
	}
	return cl, nil
}

// NewURLsMapFromStringMap takes a map of strings and returns a URLsMap. The
// string values in the map can be multiple values separated by the sep string.
func NewURLsMapFromStringMap(m map[string]string, sep string) (URLsMap, error) {
	var err error
	um := URLsMap{}
	for k, v := range m {
		um[k], err = NewURLs(strings.Split(v, sep))
		if err != nil {
			return nil, err
		}
	}
	return um, nil
}

// String 返回mach0=http://1.1.1.1:2380,mach0=http://2.2.2.2::2380,mach1=http://3.3.3.3:2380,mach2=http://4.4.4.4:2380
func (c URLsMap) String() string {
	var pairs []string
	for name, urls := range c {
		for _, url := range urls {
			pairs = append(pairs, fmt.Sprintf("%s=%s", name, url.String()))
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

// URLs 返回所有的URLS
func (c URLsMap) URLs() []string {
	var urls []string
	for _, us := range c {
		for _, u := range us {
			urls = append(urls, u.String())
		}
	}
	sort.Strings(urls)
	return urls
}

func (c URLsMap) Len() int {
	return len(c)
}

// parse 解析给定的字符串,并返回一个列出每个键的指定值的map.
func parse(s string) map[string][]string {
	m := make(map[string][]string)
	for s != "" {
		key := s
		if i := strings.IndexAny(key, ","); i >= 0 {
			key, s = key[:i], key[i+1:]
		} else {
			s = ""
		}
		if key == "" {
			continue
		}
		value := ""
		if i := strings.Index(key, "="); i >= 0 {
			key, value = key[:i], key[i+1:]
		}
		m[key] = append(m[key], value)
	}
	return m
}
