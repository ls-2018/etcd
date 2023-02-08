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
	"os"
	"time"

	"github.com/ls-2018/etcd_cn/etcd/datadir"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"
	"github.com/ls-2018/etcd_cn/pkg/cobrautl"
	"github.com/spf13/cobra"
)

var defragDataDir string

func NewDefragCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "defrag",
		Short: "清理etcd内存碎片",
		Run:   defragCommandFunc,
	}
	cmd.Flags().StringVar(&defragDataDir, "data-dir", "", "")
	cmd.MarkFlagRequired("data-dir")
	return cmd
}

func defragCommandFunc(cmd *cobra.Command, args []string) {
	err := DefragData(defragDataDir)
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, fmt.Errorf("对etcd数据进行碎片整理失败[%s] (%v)", defragDataDir, err))
	}
}

func DefragData(dataDir string) error {
	var be backend.Backend
	lg := GetLogger()
	bch := make(chan struct{})
	dbDir := datadir.ToBackendFileName(dataDir)
	go func() {
		defer close(bch)
		cfg := backend.DefaultBackendConfig()
		cfg.Logger = lg
		cfg.Path = dbDir
		be = backend.New(cfg)
	}()
	select {
	case <-bch:
	case <-time.After(time.Second):
		fmt.Fprintf(os.Stderr, "等待etcd关闭并释放其对%q的锁定.要对正在运行的etcd实例进行碎片整理请省略-data-dir. \n", dbDir)
		<-bch
	}
	return be.Defrag()
}
