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
// limitations under the License

package command

import (
	"fmt"
	"strconv"

	clientv3 "github.com/ls-2018/etcd_cn/client_sdk/v3"

	"github.com/ls-2018/etcd_cn/pkg/cobrautl"
	"github.com/spf13/cobra"
)

var compactPhysical bool

// NewCompactionCommand returns the cobra command for "compaction".
func NewCompactionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compaction [options] <revision>",
		Short: "压缩etcd中的事件历史记录",
		Run:   compactionCommandFunc,
	}
	cmd.Flags().BoolVar(&compactPhysical, "physical", false, "'true' 用于等待压缩从物理上删除所有旧修订")
	return cmd
}

// compactionCommandFunc executes the "compaction" command.
func compactionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("compaction command needs 1 argument"))
	}

	rev, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	var opts []clientv3.CompactOption
	if compactPhysical {
		opts = append(opts, clientv3.WithCompactPhysical())
	}

	c := mustClientFromCmd(cmd)
	ctx, cancel := commandCtx(cmd)
	_, cerr := c.Compact(ctx, rev, opts...)
	cancel()
	if cerr != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, cerr)
	}
	fmt.Println("已压缩了修订版本:", rev)
}
