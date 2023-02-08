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
	"strings"

	clientv3 "github.com/ls-2018/etcd_cn/client_sdk/v3"

	"github.com/ls-2018/etcd_cn/pkg/cobrautl"
	"github.com/spf13/cobra"
)

var (
	getConsistency string
	getLimit       int64
	getSortOrder   string
	getSortTarget  string
	getPrefix      bool
	getFromKey     bool
	getRev         int64
	getKeysOnly    bool
	getCountOnly   bool
	printValueOnly bool
)

func NewGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get [options] <key> [range_end]",
		Short: "获取键或键的范围",
		Run:   getCommandFunc,
	}

	cmd.Flags().StringVar(&getConsistency, "consistency", "l", "Linearizable(l) or Serializable(s)")
	cmd.Flags().StringVar(&getSortOrder, "order", "", "对结果排序; ASCEND or DESCEND (ASCEND by default)")
	cmd.Flags().StringVar(&getSortTarget, "sort-by", "", "使用那个字段排序; CREATE, KEY, MODIFY, VALUE, or VERSION")
	cmd.Flags().Int64Var(&getLimit, "limit", 0, "结果的最大数量")
	cmd.Flags().BoolVar(&getPrefix, "prefix", false, "返回前缀匹配的keys")
	cmd.Flags().BoolVar(&getFromKey, "from-key", false, "使用byte compare获取 >= 给定键的键")
	cmd.Flags().Int64Var(&getRev, "rev", 0, "指定修订版本")
	cmd.Flags().BoolVar(&getKeysOnly, "keys-only", false, "只获取keys")
	cmd.Flags().BoolVar(&getCountOnly, "count-only", false, "只获取匹配的数量")
	cmd.Flags().BoolVar(&printValueOnly, "print-value-only", false, `仅在使用“simple"输出格式时写入值`)
	return cmd
}

func getCommandFunc(cmd *cobra.Command, args []string) {
	key, opts := getGetOp(args)
	ctx, cancel := commandCtx(cmd)
	resp, err := mustClientFromCmd(cmd).Get(ctx, key, opts...)
	cancel()
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	if getCountOnly {
		if _, fields := display.(*fieldsPrinter); !fields {
			cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("--count-only is only for `--write-out=fields`"))
		}
	}

	if printValueOnly {
		dp, simple := (display).(*simplePrinter)
		if !simple {
			cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("print-value-only is only for `--write-out=simple`"))
		}
		dp.valueOnly = true
	}
	display.Get(*resp)
}

func getGetOp(args []string) (string, []clientv3.OpOption) {
	if len(args) == 0 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("get command needs one argument as key and an optional argument as range_end"))
	}

	if getPrefix && getFromKey {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("`--prefix` and `--from-key` cannot be set at the same time, choose one"))
	}

	if getKeysOnly && getCountOnly {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("`--keys-only` and `--count-only` cannot be set at the same time, choose one"))
	}

	var opts []clientv3.OpOption
	fmt.Println("getConsistency", getConsistency)
	switch getConsistency {
	case "s":
		opts = append(opts, clientv3.WithSerializable())
	case "l":
	//	 默认就是串行化读
	default:
		cobrautl.ExitWithError(cobrautl.ExitBadFeature, fmt.Errorf("未知的 consistency 标志 %q", getConsistency))
	}

	key := args[0]
	if len(args) > 1 {
		if getPrefix || getFromKey {
			cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("too many arguments, only accept one argument when `--prefix` or `--from-key` is set"))
		}
		opts = append(opts, clientv3.WithRange(args[1]))
	}

	opts = append(opts, clientv3.WithLimit(getLimit))
	if getRev > 0 {
		opts = append(opts, clientv3.WithRev(getRev))
	}

	sortByOrder := clientv3.SortNone
	sortOrder := strings.ToUpper(getSortOrder)
	switch {
	case sortOrder == "ASCEND":
		sortByOrder = clientv3.SortAscend
	case sortOrder == "DESCEND":
		sortByOrder = clientv3.SortDescend
	case sortOrder == "":
		// nothing
	default:
		cobrautl.ExitWithError(cobrautl.ExitBadFeature, fmt.Errorf("bad sort order %v", getSortOrder))
	}

	sortByTarget := clientv3.SortByKey
	sortTarget := strings.ToUpper(getSortTarget)
	switch {
	case sortTarget == "CREATE":
		sortByTarget = clientv3.SortByCreateRevision
	case sortTarget == "KEY":
		sortByTarget = clientv3.SortByKey
	case sortTarget == "MODIFY":
		sortByTarget = clientv3.SortByModRevision
	case sortTarget == "VALUE":
		sortByTarget = clientv3.SortByValue
	case sortTarget == "VERSION":
		sortByTarget = clientv3.SortByVersion
	case sortTarget == "":
		// nothing
	default:
		cobrautl.ExitWithError(cobrautl.ExitBadFeature, fmt.Errorf("bad sort target %v", getSortTarget))
	}

	opts = append(opts, clientv3.WithSort(sortByTarget, sortByOrder))

	if getPrefix {
		if len(key) == 0 {
			key = "\x00"
			opts = append(opts, clientv3.WithFromKey())
		} else {
			opts = append(opts, clientv3.WithPrefix())
		}
	}

	if getFromKey {
		if len(key) == 0 {
			key = "\x00"
		}
		opts = append(opts, clientv3.WithFromKey())
	}

	if getKeysOnly {
		opts = append(opts, clientv3.WithKeysOnly())
	}

	if getCountOnly {
		opts = append(opts, clientv3.WithCountOnly())
	}

	return key, opts
}
