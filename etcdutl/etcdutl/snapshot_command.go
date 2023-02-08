// Copyright 2021 The etcd Authors
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

package etcdutl

import (
	"fmt"
	"strings"

	"github.com/ls-2018/etcd_cn/etcd/datadir"
	"github.com/ls-2018/etcd_cn/etcdutl/snapshot"
	"github.com/ls-2018/etcd_cn/pkg/cobrautl"

	"github.com/spf13/cobra"
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
	cmd.AddCommand(NewSnapshotRestoreCommand()) // restore
	cmd.AddCommand(newSnapshotStatusCommand())  // status
	return cmd
}

func newSnapshotStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status <filename>",
		Short: "从给定的文件获取快照状态",
		Long:  ``,
		Run:   SnapshotStatusCommandFunc,
	}
}

func NewSnapshotRestoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore <filename> --data-dir {output dir} [options]",
		Short: "将etcd成员快照恢复到etcd目录",
		Run:   snapshotRestoreCommandFunc,
	}
	cmd.Flags().StringVar(&restoreDataDir, "data-dir", "", "Path to the output data directory")
	cmd.Flags().StringVar(&restoreWalDir, "wal-dir", "", "Path to the WAL directory (use --data-dir if none given)")
	cmd.Flags().StringVar(&restoreCluster, "initial-cluster", initialClusterFromName(defaultName), "Initial cluster configuration for restore bootstrap")
	cmd.Flags().StringVar(&restoreClusterToken, "initial-cluster-token", "etcd-cluster", "Initial cluster token for the etcd cluster during restore bootstrap")
	cmd.Flags().StringVar(&restorePeerURLs, "initial-advertise-peer-urls", defaultInitialAdvertisePeerURLs, "List of this member's peer URLs to advertise to the rest of the cluster")
	cmd.Flags().StringVar(&restoreName, "name", defaultName, "Human-readable name for this member")
	cmd.Flags().BoolVar(&skipHashCheck, "skip-hash-check", false, "Ignore snapshot integrity hash value (required if copied from data directory)")

	cmd.MarkFlagRequired("data-dir")

	return cmd
}

func SnapshotStatusCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		err := fmt.Errorf("snapshot status requires exactly one argument")
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, err)
	}
	printer := initPrinterFromCmd(cmd)

	lg := GetLogger()
	sp := snapshot.NewV3(lg)
	ds, err := sp.Status(args[0])
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	printer.DBStatus(ds)
}

func snapshotRestoreCommandFunc(_ *cobra.Command, args []string) {
	SnapshotRestoreCommandFunc(restoreCluster, restoreClusterToken, restoreDataDir, restoreWalDir, restorePeerURLs, restoreName, skipHashCheck, args)
}

func SnapshotRestoreCommandFunc(restoreCluster string,
	restoreClusterToken string,
	restoreDataDir string,
	restoreWalDir string,
	restorePeerURLs string,
	restoreName string,
	skipHashCheck bool,
	args []string,
) {
	if len(args) != 1 {
		err := fmt.Errorf("snapshot restore requires exactly one argument")
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, err)
	}

	dataDir := restoreDataDir
	if dataDir == "" {
		dataDir = restoreName + ".etcd"
	}

	walDir := restoreWalDir
	if walDir == "" {
		walDir = datadir.ToWalDir(dataDir)
	}

	lg := GetLogger()
	sp := snapshot.NewV3(lg)

	if err := sp.Restore(snapshot.RestoreConfig{
		SnapshotPath:        args[0],
		Name:                restoreName,
		OutputDataDir:       dataDir,
		OutputWALDir:        walDir,
		PeerURLs:            strings.Split(restorePeerURLs, ","),
		InitialCluster:      restoreCluster,
		InitialClusterToken: restoreClusterToken,
		SkipHashCheck:       skipHashCheck,
	}); err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
}

func initialClusterFromName(name string) string {
	n := name
	if name == "" {
		n = defaultName
	}
	return fmt.Sprintf("%s=http://localhost:2380", n)
}
