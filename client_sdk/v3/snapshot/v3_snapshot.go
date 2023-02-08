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

package snapshot

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"time"

	clientv3 "github.com/ls-2018/etcd_cn/client_sdk/v3"

	"github.com/dustin/go-humanize"
	"github.com/ls-2018/etcd_cn/client_sdk/pkg/fileutil"
	"go.uber.org/zap"
)

// hasChecksum returns "true" if the file size "n"
// has appended sha256 hash digest.
func hasChecksum(n int64) bool {
	// 512 is chosen because it's a minimum disk sector size
	// smaller than (and multiplies to) OS page size in most systems
	return (n % 512) == sha256.Size
}

// Save 从远程etcd获取快照并将数据保存到目标路径.如果上下文 "ctx "被取消或超时,
// 快照保存流将出错（例如,context.Canceled,context.DeadlineExceeded）.
// 请确保在客户端配置中只指定一个端点.必须向选定的节点请求快照API,而保存的快照是选定节点的时间点状态.
func Save(ctx context.Context, lg *zap.Logger, cfg clientv3.Config, dbPath string) error {
	if lg == nil {
		lg = zap.NewExample()
	}
	cfg.Logger = lg.Named("client")
	if len(cfg.Endpoints) != 1 {
		return fmt.Errorf("保存快照时,必须指定一个endpoint %v", cfg.Endpoints)
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return err
	}
	defer cli.Close()

	partpath := dbPath + ".part"
	defer os.RemoveAll(partpath)

	var f *os.File
	f, err = os.OpenFile(partpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileutil.PrivateFileMode)
	if err != nil {
		return fmt.Errorf("不能打开 %s (%v)", partpath, err)
	}
	lg.Info("创建临时快照文件", zap.String("path", partpath))

	now := time.Now()
	var rd io.ReadCloser
	rd, err = cli.Snapshot(ctx)
	if err != nil {
		return err
	}
	lg.Info("获取快照ing", zap.String("endpoint", cfg.Endpoints[0]))
	var size int64
	size, err = io.Copy(f, rd)
	if err != nil {
		return err
	}
	if !hasChecksum(size) {
		return fmt.Errorf("sha256校验和为发现 [bytes: %d]", size)
	}
	if err = fileutil.Fsync(f); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	lg.Info("已获取快照数据", zap.String("endpoint", cfg.Endpoints[0]),
		zap.String("size", humanize.Bytes(uint64(size))),
		zap.String("took", humanize.Time(now)),
	)

	if err = os.Rename(partpath, dbPath); err != nil {
		return fmt.Errorf("重命名失败 %s to %s (%v)", partpath, dbPath, err)
	}
	lg.Info("已保存", zap.String("path", dbPath))
	return nil
}
