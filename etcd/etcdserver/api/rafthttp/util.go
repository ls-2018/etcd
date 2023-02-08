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

package rafthttp

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/transport"
	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/offical/api/v3/version"

	"github.com/coreos/go-semver/semver"
	"go.uber.org/zap"
)

var (
	errMemberRemoved  = fmt.Errorf("成员已经从集群中移除")
	errMemberNotFound = fmt.Errorf("成员没有找到")
)

// NewListener returns a listener for raft message transfer between peers.
// It uses timeout listener to identify broken streams promptly.
func NewListener(u url.URL, tlsinfo *transport.TLSInfo) (net.Listener, error) {
	return transport.NewListenerWithOpts(u.Host, u.Scheme, transport.WithTLSInfo(tlsinfo), transport.WithTimeout(ConnReadTimeout, ConnWriteTimeout))
}

// NewRoundTripper 返回一个roundTripper,用于向远程peer的rafthttp监听器发送请求.
func NewRoundTripper(tlsInfo transport.TLSInfo, dialTimeout time.Duration) (http.RoundTripper, error) {
	// 它使用超时传输,与远程超时listeners 配对.它没有设置读/写超时,因为请求中的信息在读出响应之前可能需要很长的时间来写出来.
	return transport.NewTimeoutTransport(tlsInfo, dialTimeout, 0, 0)
}

// newStreamRoundTripper returns a roundTripper used to send stream requests
// to rafthttp listener of remote peers.
// Read/write timeout is set for stream roundTripper to promptly
// find out broken status, which minimizes the number of messages
// sent on broken connection.
func newStreamRoundTripper(tlsInfo transport.TLSInfo, dialTimeout time.Duration) (http.RoundTripper, error) {
	return transport.NewTimeoutTransport(tlsInfo, dialTimeout, ConnReadTimeout, ConnWriteTimeout)
}

// createPostRequest creates a HTTP POST request that sends raft message.
func createPostRequest(lg *zap.Logger, u url.URL, path string, body io.Reader, ct string, urls types.URLs, from, cid types.ID) *http.Request {
	uu := u
	uu.Path = path
	req, err := http.NewRequest("POST", uu.String(), body)
	if err != nil {
		if lg != nil {
			lg.Panic("unexpected new request error", zap.Error(err))
		}
	}
	req.Header.Set("Content-Type", ct)
	req.Header.Set("X-Server-From", from.String())
	req.Header.Set("X-Server-Version", version.Version)
	req.Header.Set("X-Min-Cluster-Version", version.MinClusterVersion)
	req.Header.Set("X-Etcd-Cluster-ID", cid.String())
	setPeerURLsHeader(req, urls)

	return req
}

// checkPostResponse checks the response of the HTTP POST request that sends
// raft message.
func checkPostResponse(lg *zap.Logger, resp *http.Response, body []byte, req *http.Request, to types.ID) error {
	switch resp.StatusCode {
	case http.StatusPreconditionFailed:
		switch strings.TrimSuffix(string(body), "\n") {
		case errIncompatibleVersion.Error():
			if lg != nil {
				lg.Error(
					"request sent was ignored by peer",
					zap.String("remote-peer-id", to.String()),
				)
			}
			return errIncompatibleVersion
		case errClusterIDMismatch.Error():
			if lg != nil {
				lg.Error(
					"request sent was ignored due to cluster ID mismatch",
					zap.String("remote-peer-id", to.String()),
					zap.String("remote-peer-cluster-id", resp.Header.Get("X-Etcd-Cluster-ID")),
					zap.String("local-member-cluster-id", req.Header.Get("X-Etcd-Cluster-ID")),
				)
			}
			return errClusterIDMismatch
		default:
			return fmt.Errorf("unhandled error %q when precondition failed", string(body))
		}
	case http.StatusForbidden:
		return errMemberRemoved
	case http.StatusNoContent:
		return nil
	default:
		return fmt.Errorf("unexpected http status %s while posting to %q", http.StatusText(resp.StatusCode), req.URL.String())
	}
}

// reportCriticalError reports the given error through sending it into
// the given error channel.
// If the error channel is filled up when sending error, it drops the error
// because the fact that error has happened is reported, which is
// good enough.
func reportCriticalError(err error, errc chan<- error) {
	select {
	case errc <- err:
	default:
	}
}

// setPeerURLsHeader reports local urls for peer discovery
func setPeerURLsHeader(req *http.Request, urls types.URLs) {
	if urls == nil {
		// often not set in unit tests
		return
	}
	peerURLs := make([]string, urls.Len())
	for i := range urls {
		peerURLs[i] = urls[i].String()
	}
	req.Header.Set("X-PeerURLs", strings.Join(peerURLs, ","))
}

//  -----------------------------------------  OVER ----------------------------------------------------

// addRemoteFromRequest 根据http请求头添加一个远程对等体
func addRemoteFromRequest(tr Transporter, r *http.Request) {
	if from, err := types.IDFromString(r.Header.Get("X-Server-From")); err == nil {
		if urls := r.Header.Get("X-PeerURLs"); urls != "" {
			tr.AddRemote(from, strings.Split(urls, ","))
		}
	}
}

func serverVersion(h http.Header) *semver.Version {
	verStr := h.Get("X-Server-Version")
	if verStr == "" {
		verStr = "2.0.0"
	}
	return semver.Must(semver.NewVersion(verStr))
}

func minClusterVersion(h http.Header) *semver.Version {
	verStr := h.Get("X-Min-Cluster-Version")
	if verStr == "" {
		verStr = "2.0.0"
	}
	return semver.Must(semver.NewVersion(verStr))
}

// compareMajorMinorVersion 比较两个版本
func compareMajorMinorVersion(a, b *semver.Version) int {
	na := &semver.Version{Major: a.Major, Minor: a.Minor}
	nb := &semver.Version{Major: b.Major, Minor: b.Minor}
	switch {
	case na.LessThan(*nb):
		return -1
	case nb.LessThan(*na):
		return 1
	default:
		return 0
	}
}

// checkVersionCompatibility 检查给定的版本是否与本地的版本兼容
func checkVersionCompatibility(name string, server, minCluster *semver.Version) (localServer *semver.Version, localMinCluster *semver.Version, err error) {
	localServer = semver.Must(semver.NewVersion(version.Version))
	localMinCluster = semver.Must(semver.NewVersion(version.MinClusterVersion))
	if compareMajorMinorVersion(server, localMinCluster) == -1 {
		return localServer, localMinCluster, fmt.Errorf("远端版本太低: remote[%s]=%s, local=%s", name, server, localServer)
	}
	if compareMajorMinorVersion(minCluster, localServer) == 1 {
		return localServer, localMinCluster, fmt.Errorf("本地版本太低: remote[%s]=%s, local=%s", name, server, localServer)
	}
	return localServer, localMinCluster, nil
}
