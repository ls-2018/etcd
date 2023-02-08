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

package transport

import (
	"net"
	"net/http"
	"time"
)

// NewTimeoutTransport 返回一个使用给定的TLS信息创建的传输.
// 如果创建的连接上的读/写块超过了它的时间限制.  它将返回超时错误.
// 如果读/写超时被设置,传输将不能重新使用连接.
func NewTimeoutTransport(info TLSInfo, dialtimeoutd, rdtimeoutd, wtimeoutd time.Duration) (*http.Transport, error) {
	tr, err := NewTransport(info, dialtimeoutd)
	if err != nil {
		return nil, err
	}

	if rdtimeoutd != 0 || wtimeoutd != 0 {
		// 超时的连接在闲置后很快就会超时,它不应该被放回http传输系统作为闲置连接供将来使用.
		tr.MaxIdleConnsPerHost = -1
	} else {
		// 允许peer之间有更多的空闲连接,以避免不必要的端口分配.
		tr.MaxIdleConnsPerHost = 1024
	}

	tr.Dial = (&rwTimeoutDialer{
		Dialer: net.Dialer{
			Timeout:   dialtimeoutd,
			KeepAlive: 30 * time.Second,
		},
		rdtimeoutd: rdtimeoutd,
		wtimeoutd:  wtimeoutd,
	}).Dial
	return tr, nil
}
