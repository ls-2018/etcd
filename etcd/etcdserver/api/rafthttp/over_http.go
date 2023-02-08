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
	"errors"
	"net/http"
	"path"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"go.uber.org/zap"
)

const (
	connReadLimitByte = 64 * 1024                     //  链接最大的读取数据量
	snapshotLimitByte = 1 * 1024 * 1024 * 1024 * 1024 // 快照大小上限
)

var (
	RaftPrefix             = "/raft"
	ProbingPrefix          = path.Join(RaftPrefix, "probing")
	RaftStreamPrefix       = path.Join(RaftPrefix, "stream")
	RaftSnapshotPrefix     = path.Join(RaftPrefix, "snapshot")
	errIncompatibleVersion = errors.New("不兼容的版本")
	errClusterIDMismatch   = errors.New("cluster ID不匹配")
)

type peerGetter interface {
	Get(id types.ID) Peer
}

type writerToResponse interface {
	WriteTo(w http.ResponseWriter)
}

// checkClusterCompatibilityFromHeader 检查集群版本的兼容性
//.它检查本地成员的版本是否与报头中的版本兼容,以及本地成员的集群ID是否与报头中的ID一致.
func checkClusterCompatibilityFromHeader(lg *zap.Logger, localID types.ID, header http.Header, cid types.ID) error {
	remoteName := header.Get("X-Server-From")
	remoteServer := serverVersion(header)
	remoteVs := ""
	if remoteServer != nil {
		remoteVs = remoteServer.String()
	}

	remoteMinClusterVer := minClusterVersion(header)
	remoteMinClusterVs := ""
	if remoteMinClusterVer != nil {
		remoteMinClusterVs = remoteMinClusterVer.String()
	}

	localServer, localMinCluster, err := checkVersionCompatibility(remoteName, remoteServer, remoteMinClusterVer)

	localVs := ""
	if localServer != nil {
		localVs = localServer.String()
	}
	localMinClusterVs := ""
	if localMinCluster != nil {
		localMinClusterVs = localMinCluster.String()
	}

	if err != nil {
		lg.Warn(
			"检查版本兼容性失败",
			zap.String("local-member-id", localID.String()),
			zap.String("local-member-cluster-id", cid.String()),
			zap.String("local-member-etcd-version", localVs),
			zap.String("local-member-etcd-minimum-cluster-version", localMinClusterVs),
			zap.String("remote-peer-etcd-name", remoteName),
			zap.String("remote-peer-etcd-version", remoteVs),
			zap.String("remote-peer-etcd-minimum-cluster-version", remoteMinClusterVs),
			zap.Error(err),
		)
		return errIncompatibleVersion
	}
	if gcid := header.Get("X-Etcd-Cluster-ID"); gcid != cid.String() {
		lg.Warn(
			"集群ID不匹配",
			zap.String("local-member-id", localID.String()),
			zap.String("local-member-cluster-id", cid.String()),
			zap.String("local-member-etcd-version", localVs),
			zap.String("local-member-etcd-minimum-cluster-version", localMinClusterVs),
			zap.String("remote-peer-etcd-name", remoteName),
			zap.String("remote-peer-etcd-version", remoteVs),
			zap.String("remote-peer-etcd-minimum-cluster-version", remoteMinClusterVs),
			zap.String("remote-peer-cluster-id", gcid),
		)
		return errClusterIDMismatch
	}
	return nil
}

type closeNotifier struct {
	done chan struct{}
}

func newCloseNotifier() *closeNotifier {
	return &closeNotifier{
		done: make(chan struct{}),
	}
}

func (n *closeNotifier) Close() error {
	close(n.done)
	return nil
}

func (n *closeNotifier) closeNotify() <-chan struct{} { return n.done }
