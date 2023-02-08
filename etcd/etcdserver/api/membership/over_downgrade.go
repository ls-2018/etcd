// Copyright 2020 The etcd Authors
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

package membership

import (
	"github.com/coreos/go-semver/semver"
	"github.com/ls-2018/etcd_cn/offical/api/v3/version"
	"go.uber.org/zap"
)

type DowngradeInfo struct {
	TargetVersion string `json:"target-version"` // 是目标降级版本,如果集群不在降级中,targetVersion将是一个空字符串.
	Enabled       bool   `json:"enabled"`        // 表示集群是否启用了降级功能
}

func (d *DowngradeInfo) GetTargetVersion() *semver.Version {
	return semver.Must(semver.NewVersion(d.TargetVersion))
}

// mustDetectDowngrade 检测版本降级.
func mustDetectDowngrade(lg *zap.Logger, cv *semver.Version, d *DowngradeInfo) {
	lv := semver.Must(semver.NewVersion(version.Version))
	lv = &semver.Version{Major: lv.Major, Minor: lv.Minor}

	// 如果集群启用了降级功能,请对照降级目标版本检查本地版本.
	if d != nil && d.Enabled && d.TargetVersion != "" {
		if lv.Equal(*d.GetTargetVersion()) {
			if cv != nil {
				lg.Info("集群正在降级到目标版本", zap.String("target-cluster-version", d.TargetVersion), zap.String("determined-cluster-version", version.Cluster(cv.String())), zap.String("current-etcd-version", version.Version))
			}
			return
		}
		lg.Fatal("无效的降级;当降级被启用时,etcd版本不允许加入", zap.String("current-etcd-version", version.Version), zap.String("target-cluster-version", d.TargetVersion))
	}

	// 如果集群禁止降级,则根据确定的集群版本检查本地版本,如果本地版本不低于集群版本,则验证通过
	if cv != nil && lv.LessThan(*cv) {
		lg.Fatal("无效的降级;etcd版本低于确定的集群版本", zap.String("current-etcd-version", version.Version), zap.String("determined-cluster-version", version.Cluster(cv.String())))
	}
}

// AllowedDowngradeVersion 允许版本降级
func AllowedDowngradeVersion(ver *semver.Version) *semver.Version {
	return &semver.Version{Major: ver.Major, Minor: ver.Minor - 1}
}

// isValidDowngrade 验证集群是否可以从verFrom降级到verTo 小版本差1
func isValidDowngrade(verFrom *semver.Version, verTo *semver.Version) bool {
	return verTo.Equal(*AllowedDowngradeVersion(verFrom))
}
