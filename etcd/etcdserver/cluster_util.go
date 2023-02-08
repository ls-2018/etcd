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

package etcdserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/membership"
	"github.com/ls-2018/etcd_cn/offical/api/v3/version"

	"github.com/coreos/go-semver/semver"
	"go.uber.org/zap"
)

// isMemberBootstrapped 试图检查给定的成员是否已经在给定的集群中被引导了.
func isMemberBootstrapped(lg *zap.Logger, cl *membership.RaftCluster, member string, rt http.RoundTripper, timeout time.Duration) bool {
	// 获取非本机的peer urls
	rcl, err := getClusterFromRemotePeers(lg, getRemotePeerURLs(cl, member), timeout, false, rt) // 从远端节点获取到的集群节点信息
	if err != nil {
		// 初始化时,会有err此时member 是节点名字,而cl.member里的是hash之后的值
		return false
	}
	id := cl.MemberByName(member).ID
	m := rcl.Member(id) // 远端的
	if m == nil {
		return false
	}
	if len(m.ClientURLs) > 0 {
		return true
	}
	return false
}

// GetClusterFromRemotePeers takes a set of URLs representing etcd peers, and
// attempts to construct a Cluster by accessing the members endpoint on one of
// these URLs. The first URL to provide a response is used. If no URLs provide
// a response, or a Cluster cannot backend successfully created from a received
// response, an error is returned.
// Each request has a 10-second timeout. Because the upper limit of TTL is 5s,
// 10 second is enough for building connection and finishing request.
func GetClusterFromRemotePeers(lg *zap.Logger, urls []string, rt http.RoundTripper) (*membership.RaftCluster, error) {
	return getClusterFromRemotePeers(lg, urls, 10*time.Second, true, rt)
}

// 从远端节点获取到的集群节点信息
func getClusterFromRemotePeers(lg *zap.Logger, urls []string, timeout time.Duration, logerr bool, rt http.RoundTripper) (*membership.RaftCluster, error) {
	if lg == nil {
		lg = zap.NewNop()
	}
	cc := &http.Client{
		Transport: rt,
		Timeout:   timeout,
	}
	for _, u := range urls {
		addr := u + "/members"
		resp, err := cc.Get(addr)
		if err != nil {
			if logerr {
				lg.Warn("获取集群响应失败", zap.String("address", addr), zap.Error(err))
			}
			continue
		}
		b, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			if logerr {
				lg.Warn("读取集群响应失败", zap.String("address", addr), zap.Error(err))
			}
			continue
		}
		var membs []*membership.Member
		if err = json.Unmarshal(b, &membs); err != nil {
			if logerr {
				lg.Warn("反序列化集群响应失败", zap.String("address", addr), zap.Error(err))
			}
			continue
		}
		id, err := types.IDFromString(resp.Header.Get("X-Etcd-Cluster-ID"))
		if err != nil {
			if logerr {
				lg.Warn(
					"无法解析集群ID",
					zap.String("address", addr),
					zap.String("header", resp.Header.Get("X-Etcd-Cluster-ID")),
					zap.Error(err),
				)
			}
			continue
		}

		if len(membs) > 0 {
			return membership.NewClusterFromMembers(lg, id, membs), nil // Construct struct
		}
		return nil, fmt.Errorf("无法获取raft集群节点信息从远端节点")
	}
	return nil, fmt.Errorf("无法从给定的URL中检索到集群信息")
}

// getRemotePeerURLs 获取非本机的peer urls
func getRemotePeerURLs(cl *membership.RaftCluster, local string) []string {
	us := make([]string, 0)
	for _, m := range cl.Members() {
		if m.Name == local {
			continue
		}
		us = append(us, m.PeerURLs...)
	}
	sort.Strings(us)
	return us
}

// getVersions returns the versions of the members in the given cluster.
// The key of the returned map is the member's ID. The value of the returned map
// is the semver versions string, including etcd and cluster.
// If it fails to get the version of a member, the key will backend nil.
func getVersions(lg *zap.Logger, cl *membership.RaftCluster, local types.ID, rt http.RoundTripper) map[string]*version.Versions {
	members := cl.Members()
	vers := make(map[string]*version.Versions)
	for _, m := range members {
		if m.ID == local {
			cv := "not_decided"
			if cl.Version() != nil {
				cv = cl.Version().String()
			}
			vers[m.ID.String()] = &version.Versions{Server: version.Version, Cluster: cv}
			continue
		}
		ver, err := getVersion(lg, m, rt)
		if err != nil {
			lg.Warn("failed to get version", zap.String("remote-member-id", m.ID.String()), zap.Error(err))
			vers[m.ID.String()] = nil
		} else {
			vers[m.ID.String()] = ver
		}
	}
	return vers
}

// decideClusterVersion decides the cluster version based on the versions map.
// The returned version is the min etcd version in the map, or nil if the min
// version in unknown.
func decideClusterVersion(lg *zap.Logger, vers map[string]*version.Versions) *semver.Version {
	var cv *semver.Version
	lv := semver.Must(semver.NewVersion(version.Version))

	for mid, ver := range vers {
		if ver == nil {
			return nil
		}
		v, err := semver.NewVersion(ver.Server)
		if err != nil {
			lg.Warn(
				"failed to parse etcd version of remote member",
				zap.String("remote-member-id", mid),
				zap.String("remote-member-version", ver.Server),
				zap.Error(err),
			)
			return nil
		}
		if lv.LessThan(*v) {
			lg.Warn(
				"leader found higher-versioned member",
				zap.String("local-member-version", lv.String()),
				zap.String("remote-member-id", mid),
				zap.String("remote-member-version", ver.Server),
			)
		}
		if cv == nil {
			cv = v
		} else if v.LessThan(*cv) {
			cv = v
		}
	}
	return cv
}

// allowedVersionRange decides the available version range of the cluster that local etcd can join in;
// if the downgrade enabled status is true, the version window is [oneMinorHigher, oneMinorHigher]
// if the downgrade is not enabled, the version window is [MinClusterVersion, localVersion]
func allowedVersionRange(downgradeEnabled bool) (minV *semver.Version, maxV *semver.Version) {
	minV = semver.Must(semver.NewVersion(version.MinClusterVersion))
	maxV = semver.Must(semver.NewVersion(version.Version))
	maxV = &semver.Version{Major: maxV.Major, Minor: maxV.Minor}

	if downgradeEnabled {
		// Todo: handle the case that downgrading from higher major version(e.g. downgrade from v4.0 to v3.x)
		maxV.Minor = maxV.Minor + 1
		minV = &semver.Version{Major: maxV.Major, Minor: maxV.Minor}
	}
	return minV, maxV
}

// isCompatibleWithCluster return true if the local member has a compatible version with
// the current running cluster.
// The version is considered as compatible when at least one of the other members in the cluster has a
// cluster version in the range of [MinV, MaxV] and no known members has a cluster version
// out of the range.
// We set this rule since when the local member joins, another member might backend offline.
func isCompatibleWithCluster(lg *zap.Logger, cl *membership.RaftCluster, local types.ID, rt http.RoundTripper) bool {
	vers := getVersions(lg, cl, local, rt)
	minV, maxV := allowedVersionRange(getDowngradeEnabledFromRemotePeers(lg, cl, local, rt))
	return isCompatibleWithVers(lg, vers, local, minV, maxV)
}

func isCompatibleWithVers(lg *zap.Logger, vers map[string]*version.Versions, local types.ID, minV, maxV *semver.Version) bool {
	var ok bool
	for id, v := range vers {
		// ignore comparison with local version
		if id == local.String() {
			continue
		}
		if v == nil {
			continue
		}
		clusterv, err := semver.NewVersion(v.Cluster)
		if err != nil {
			lg.Warn(
				"failed to parse cluster version of remote member",
				zap.String("remote-member-id", id),
				zap.String("remote-member-cluster-version", v.Cluster),
				zap.Error(err),
			)
			continue
		}
		if clusterv.LessThan(*minV) {
			lg.Warn(
				"cluster version of remote member is not compatible; too low",
				zap.String("remote-member-id", id),
				zap.String("remote-member-cluster-version", clusterv.String()),
				zap.String("minimum-cluster-version-supported", minV.String()),
			)
			return false
		}
		if maxV.LessThan(*clusterv) {
			lg.Warn(
				"cluster version of remote member is not compatible; too high",
				zap.String("remote-member-id", id),
				zap.String("remote-member-cluster-version", clusterv.String()),
				zap.String("minimum-cluster-version-supported", minV.String()),
			)
			return false
		}
		ok = true
	}
	return ok
}

// getVersion returns the Versions of the given member via its
// peerURLs. Returns the last error if it fails to get the version.
func getVersion(lg *zap.Logger, m *membership.Member, rt http.RoundTripper) (*version.Versions, error) {
	cc := &http.Client{
		Transport: rt,
	}
	var (
		err  error
		resp *http.Response
	)

	for _, u := range m.PeerURLs {
		addr := u + "/version"
		resp, err = cc.Get(addr)
		if err != nil {
			lg.Warn(
				"failed to reach the peer URL",
				zap.String("address", addr),
				zap.String("remote-member-id", m.ID.String()),
				zap.Error(err),
			)
			continue
		}
		var b []byte
		b, err = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lg.Warn(
				"failed to read body of response",
				zap.String("address", addr),
				zap.String("remote-member-id", m.ID.String()),
				zap.Error(err),
			)
			continue
		}
		var vers version.Versions
		if err = json.Unmarshal(b, &vers); err != nil {
			lg.Warn(
				"failed to unmarshal response",
				zap.String("address", addr),
				zap.String("remote-member-id", m.ID.String()),
				zap.Error(err),
			)
			continue
		}
		return &vers, nil
	}
	return nil, err
}

func promoteMemberHTTP(ctx context.Context, url string, id uint64, peerRt http.RoundTripper) ([]*membership.Member, error) {
	cc := &http.Client{Transport: peerRt}
	// TODO: refactor member http handler code
	// cannot import etcdhttp, so manually construct url
	requestUrl := url + "/members/promote/" + fmt.Sprintf("%d", id)
	req, err := http.NewRequest("POST", requestUrl, nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	resp, err := cc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusRequestTimeout {
		return nil, ErrTimeout
	}
	if resp.StatusCode == http.StatusPreconditionFailed {
		// both ErrMemberNotLearner and ErrLearnerNotReady have same http status code
		if strings.Contains(string(b), ErrLearnerNotReady.Error()) {
			return nil, ErrLearnerNotReady
		}
		if strings.Contains(string(b), membership.ErrMemberNotLearner.Error()) {
			return nil, membership.ErrMemberNotLearner
		}
		return nil, fmt.Errorf("member promote: unknown error(%s)", string(b))
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, membership.ErrIDNotFound
	}

	if resp.StatusCode != http.StatusOK { // all other types of errors
		return nil, fmt.Errorf("member promote: unknown error(%s)", string(b))
	}

	var membs []*membership.Member
	if err := json.Unmarshal(b, &membs); err != nil {
		return nil, err
	}
	return membs, nil
}

// getDowngradeEnabledFromRemotePeers will get the downgrade enabled status of the cluster.
func getDowngradeEnabledFromRemotePeers(lg *zap.Logger, cl *membership.RaftCluster, local types.ID, rt http.RoundTripper) bool {
	members := cl.Members()

	for _, m := range members {
		if m.ID == local {
			continue
		}
		enable, err := getDowngradeEnabled(lg, m, rt)
		if err != nil {
			lg.Warn("failed to get downgrade enabled status", zap.String("remote-member-id", m.ID.String()), zap.Error(err))
		} else {
			// Since the "/downgrade/enabled" serves linearized data,
			// this function can return once it gets a non-error response from the endpoint.
			return enable
		}
	}
	return false
}

// getDowngradeEnabled returns the downgrade enabled status of the given member
// via its peerURLs. Returns the last error if it fails to get it.
func getDowngradeEnabled(lg *zap.Logger, m *membership.Member, rt http.RoundTripper) (bool, error) {
	cc := &http.Client{
		Transport: rt,
	}
	var (
		err  error
		resp *http.Response
	)

	for _, u := range m.PeerURLs {
		addr := u + DowngradeEnabledPath
		resp, err = cc.Get(addr)
		if err != nil {
			lg.Warn(
				"failed to reach the peer URL",
				zap.String("address", addr),
				zap.String("remote-member-id", m.ID.String()),
				zap.Error(err),
			)
			continue
		}
		var b []byte
		b, err = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lg.Warn(
				"failed to read body of response",
				zap.String("address", addr),
				zap.String("remote-member-id", m.ID.String()),
				zap.Error(err),
			)
			continue
		}
		var enable bool
		if enable, err = strconv.ParseBool(string(b)); err != nil {
			lg.Warn(
				"failed to convert response",
				zap.String("address", addr),
				zap.String("remote-member-id", m.ID.String()),
				zap.Error(err),
			)
			continue
		}
		return enable, nil
	}
	return false, err
}

// isMatchedVersions returns true if all etcd versions are equal to target version, otherwise return false.
// It can backend used to decide the whether the cluster finishes downgrading to target version.
func isMatchedVersions(lg *zap.Logger, targetVersion *semver.Version, vers map[string]*version.Versions) bool {
	for mid, ver := range vers {
		if ver == nil {
			return false
		}
		v, err := semver.NewVersion(ver.Cluster)
		if err != nil {
			lg.Warn(
				"failed to parse etcd version of remote member",
				zap.String("remote-member-id", mid),
				zap.String("remote-member-version", ver.Server),
				zap.Error(err),
			)
			return false
		}
		if !targetVersion.Equal(*v) {
			lg.Warn("remotes etcd has mismatching etcd version",
				zap.String("remote-member-id", mid),
				zap.String("current-etcd-version", v.String()),
				zap.String("target-version", targetVersion.String()),
			)
			return false
		}
	}
	return true
}

func convertToClusterVersion(v string) (*semver.Version, error) {
	ver, err := semver.NewVersion(v)
	if err != nil {
		// allow input version format Major.Minor
		ver, err = semver.NewVersion(v + ".0")
		if err != nil {
			return nil, ErrWrongDowngradeVersionFormat
		}
	}
	// cluster version only keeps major.minor, remove patch version
	ver = &semver.Version{Major: ver.Major, Minor: ver.Minor}
	return ver, nil
}
