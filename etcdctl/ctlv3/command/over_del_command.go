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

package command

import (
	"fmt"

	clientv3 "github.com/ls-2018/etcd_cn/client_sdk/v3"
	"github.com/ls-2018/etcd_cn/pkg/cobrautl"
	"github.com/spf13/cobra"
)

var (
	delPrefix  bool
	delPrevKV  bool
	delFromKey bool
)

// NewDelCommand returns the cobra command for "del".
func NewDelCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "del [options] <key> [range_end]",
		Short: "移除指定的键或键的范围 [key, range_end)",
		Run:   delCommandFunc,
	}

	cmd.Flags().BoolVar(&delPrefix, "prefix", false, "通过匹配前缀删除键")
	cmd.Flags().BoolVar(&delPrevKV, "prev-kv", false, "返回删除的k,v 键值对")
	cmd.Flags().BoolVar(&delFromKey, "from-key", false, "使用字节比较法删除大于或等于给定键的键.")
	return cmd
}

// delCommandFunc executes the "del" command.
func delCommandFunc(cmd *cobra.Command, args []string) {
	key, opts := getDelOp(args)
	ctx, cancel := commandCtx(cmd)
	resp, err := mustClientFromCmd(cmd).Delete(ctx, key, opts...)
	cancel()
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	display.Del(*resp)
}

func getDelOp(args []string) (string, []clientv3.OpOption) {
	if len(args) == 0 || len(args) > 2 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("del command needs one argument as key and an optional argument as range_end"))
	}

	if delPrefix && delFromKey {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("`--prefix` and `--from-key` cannot be set at the same time, choose one"))
	}

	opts := []clientv3.OpOption{}
	key := args[0]
	if len(args) > 1 {
		if delPrefix || delFromKey {
			cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("too many arguments, only accept one argument when `--prefix` or `--from-key` is set"))
		}
		opts = append(opts, clientv3.WithRange(args[1]))
	}

	if delPrefix {
		if len(key) == 0 {
			key = "\x00"
			opts = append(opts, clientv3.WithFromKey())
		} else {
			opts = append(opts, clientv3.WithPrefix())
		}
	}
	if delPrevKV {
		opts = append(opts, clientv3.WithPrevKV())
	}

	if delFromKey {
		if len(key) == 0 {
			key = "\x00"
		}
		opts = append(opts, clientv3.WithFromKey())
	}

	return key, opts
}
