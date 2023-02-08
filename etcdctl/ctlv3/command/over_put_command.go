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
	"os"
	"strconv"

	clientv3 "github.com/ls-2018/etcd_cn/client_sdk/v3"

	"github.com/ls-2018/etcd_cn/pkg/cobrautl"
	"github.com/spf13/cobra"
)

var (
	leaseStr       string
	putPrevKV      bool
	putIgnoreVal   bool
	putIgnoreLease bool
)

// NewPutCommand returns the cobra command for "put".
func NewPutCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "put",
		Short: "将给定的键放入存储中",
		Long:  `将给定的键放入存储中`,
		Run:   putCommandFunc,
	}
	cmd.Flags().StringVar(&leaseStr, "lease", "0", "将租约附加到key  (in hexadecimal) ")
	cmd.Flags().BoolVar(&putPrevKV, "prev-kv", false, "返回键值对之前的版本")
	cmd.Flags().BoolVar(&putIgnoreVal, "ignore-value", false, "更新当前的值")
	cmd.Flags().BoolVar(&putIgnoreLease, "ignore-lease", false, "更新租约")
	return cmd
}

func putCommandFunc(cmd *cobra.Command, args []string) {
	key, value, opts := getPutOp(args)

	ctx, cancel := commandCtx(cmd)
	resp, err := mustClientFromCmd(cmd).Put(ctx, key, value, opts...)
	cancel()
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	display.Put(*resp)
}

func getPutOp(args []string) (string, string, []clientv3.OpOption) {
	if len(args) == 0 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("put command needs 1 argument and input from stdin or 2 arguments"))
	}

	key := args[0]
	if putIgnoreVal && len(args) > 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("put command needs only 1 argument when 'ignore-value' is set"))
	}

	var value string
	var err error
	if !putIgnoreVal {
		value, err = argOrStdin(args, os.Stdin, 1)
		if err != nil {
			cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("put command needs 1 argument and input from stdin or 2 arguments"))
		}
	}

	id, err := strconv.ParseInt(leaseStr, 16, 64)
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("bad lease ID (%v), expecting ID in Hex", err))
	}

	var opts []clientv3.OpOption
	if id != 0 {
		opts = append(opts, clientv3.WithLease(clientv3.LeaseID(id)))
	}
	if putPrevKV {
		opts = append(opts, clientv3.WithPrevKV())
	}
	if putIgnoreVal {
		opts = append(opts, clientv3.WithIgnoreValue())
	}
	if putIgnoreLease {
		opts = append(opts, clientv3.WithIgnoreLease())
	}

	return key, value, opts
}
