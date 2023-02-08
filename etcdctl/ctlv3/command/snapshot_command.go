// Copyright 2016 The etcd Authors
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

package command

import (
	"context"
	"fmt"
	"os"

	snapshot "github.com/ls-2018/etcd_cn/client_sdk/v3/snapshot"
	"github.com/ls-2018/etcd_cn/etcdutl/etcdutl"
	"github.com/ls-2018/etcd_cn/pkg/cobrautl"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const (
	defaultName                     = "default"
	defaultInitialAdvertisePeerURLs = "http://localhost:2380"
)

var (
	restoreCluster      string
	restoreClusterToken string
	restoreDataDir      string
	restoreWalDir       string
	restorePeerURLs     string
	restoreName         string
	skipHashCheck       bool
)

// NewSnapshotCommand returns the cobra command for "snapshot".
func NewSnapshotCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "snapshot <subcommand>",
		Short: "Manages etcd node snapshots",
	}
	cmd.AddCommand(NewSnapshotSaveCommand())
	cmd.AddCommand(NewSnapshotRestoreCommand())
	cmd.AddCommand(newSnapshotStatusCommand())
	return cmd
}

func NewSnapshotSaveCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "save <filename>",
		Short: "将etcd节点后端快照存储到给定的文件",
		Run:   snapshotSaveCommandFunc,
	}
}

func newSnapshotStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status <filename>",
		Short: "[deprecated] 从给定的文件获取快照状态",
		Long: `When --write-out is set to simple, this command prints out comma-separated status lists for each endpoint.
The items in the lists are hash, revision, total keys, total size.

Moved to 'etcdctl snapshot status ...'
`,
		Run: snapshotStatusCommandFunc,
	}
}

func NewSnapshotRestoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore <filename> [options]",
		Short: "将etcd成员快照恢复到etcd目录",
		Run:   snapshotRestoreCommandFunc,
	}
	cmd.Flags().StringVar(&restoreDataDir, "data-dir", "", "数据目录")
	cmd.Flags().StringVar(&restoreWalDir, "wal-dir", "", "wal目录 (use --data-dir if none given)")
	cmd.Flags().StringVar(&restoreCluster, "initial-cluster", initialClusterFromName(defaultName), "初始集群配置")
	cmd.Flags().StringVar(&restoreClusterToken, "initial-cluster-token", "etcd-cluster", "在恢复引导过程中etcd集群的初始群集令牌")
	cmd.Flags().StringVar(&restorePeerURLs, "initial-advertise-peer-urls", defaultInitialAdvertisePeerURLs, "要通告给集群其他部分的该成员的对等url列表")
	cmd.Flags().StringVar(&restoreName, "name", defaultName, "此成员的人类可读的名称")
	cmd.Flags().BoolVar(&skipHashCheck, "skip-hash-check", false, "忽略快照完整性哈希值(从数据目录复制时需要)")

	return cmd
}

func snapshotSaveCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		err := fmt.Errorf("snapshot save expects one argument")
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, err)
	}

	lg, err := zap.NewProduction()
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	cfg := mustClientCfgFromCmd(cmd)

	// if user does not specify "--command-timeout" flag, there will be no timeout for snapshot save command
	ctx, cancel := context.WithCancel(context.Background())
	if isCommandTimeoutFlagSet(cmd) {
		ctx, cancel = commandCtx(cmd)
	}
	defer cancel()

	path := args[0]
	if err := snapshot.Save(ctx, lg, *cfg, path); err != nil {
		cobrautl.ExitWithError(cobrautl.ExitInterrupted, err)
	}
	fmt.Printf("Snapshot saved at %s\n", path)
}

func snapshotStatusCommandFunc(cmd *cobra.Command, args []string) {
	fmt.Fprintf(os.Stderr, "Deprecated: Use `etcdutl snapshot status` instead.\n\n")
	etcdutl.SnapshotStatusCommandFunc(cmd, args)
}

func snapshotRestoreCommandFunc(cmd *cobra.Command, args []string) {
	fmt.Fprintf(os.Stderr, "弃用: 使用 `etcdutl snapshot restore` \n\n")
	etcdutl.SnapshotRestoreCommandFunc(restoreCluster, restoreClusterToken, restoreDataDir, restoreWalDir, restorePeerURLs, restoreName, skipHashCheck, args)
}

func initialClusterFromName(name string) string {
	n := name
	if name == "" {
		n = defaultName
	}
	return fmt.Sprintf("%s=http://localhost:2380", n)
}
