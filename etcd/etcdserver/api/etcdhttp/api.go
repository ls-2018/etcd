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

package etcdhttp

import (
	"net/http"

	"github.com/ls-2018/etcd_cn/etcd/etcdserver"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/rafthttp"
	"github.com/ls-2018/etcd_cn/etcd/lease/leasehttp"

	"go.uber.org/zap"
)

const (
	peerMembersPath         = "/members"
	peerMemberPromotePrefix = "/members/promote/"
)

// NewPeerHandler 生成 http.Handler 处理客户端请求
func NewPeerHandler(lg *zap.Logger, s etcdserver.ServerPeerV2) http.Handler {
	return newPeerHandler(lg, s, s.RaftHandler(), s.LeaseHandler(), s.HashKVHandler(), s.DowngradeEnabledHandler())
}

func newPeerHandler(lg *zap.Logger, s etcdserver.Server, raftHandler http.Handler,
	leaseHandler http.Handler, hashKVHandler http.Handler, downgradeEnabledHandler http.Handler,
) http.Handler {
	if lg == nil {
		lg = zap.NewNop()
	}
	peerMembersHandler := newPeerMembersHandler(lg, s.Cluster())   // ✅
	peerMemberPromoteHandler := newPeerMemberPromoteHandler(lg, s) // ✅

	mux := http.NewServeMux()
	mux.HandleFunc("/", http.NotFound)
	mux.Handle(rafthttp.RaftPrefix, raftHandler)                  // /raft
	mux.Handle(rafthttp.RaftPrefix+"/", raftHandler)              //
	mux.Handle(peerMembersPath, peerMembersHandler)               // /members
	mux.Handle(peerMemberPromotePrefix, peerMemberPromoteHandler) // /members/promote
	if leaseHandler != nil {
		mux.Handle(leasehttp.LeasePrefix, leaseHandler)         // /leases
		mux.Handle(leasehttp.LeaseInternalPrefix, leaseHandler) // /leases/internal
	}
	if downgradeEnabledHandler != nil {
		mux.Handle(etcdserver.DowngradeEnabledPath, downgradeEnabledHandler) // /downgrade/enabled
	}
	if hashKVHandler != nil {
		mux.Handle(etcdserver.PeerHashKVPath, hashKVHandler) // /members/hashkv
	}
	mux.HandleFunc(versionPath, versionHandler(s.Cluster(), serveVersion))
	return mux
}
