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
	"strconv"

	v3 "github.com/ls-2018/etcd_cn/client_sdk/v3"
	"github.com/ls-2018/etcd_cn/pkg/cobrautl"

	"github.com/spf13/cobra"
)

// NewLeaseCommand returns the cobra command for "lease".
func NewLeaseCommand() *cobra.Command {
	lc := &cobra.Command{
		Use:   "lease <subcommand>",
		Short: "租约相关命令",
	}

	lc.AddCommand(NewLeaseGrantCommand())
	lc.AddCommand(NewLeaseRevokeCommand())
	lc.AddCommand(NewLeaseTimeToLiveCommand())
	lc.AddCommand(NewLeaseListCommand())
	lc.AddCommand(NewLeaseKeepAliveCommand())

	return lc
}

// NewLeaseGrantCommand returns the cobra command for "lease grant".
func NewLeaseGrantCommand() *cobra.Command {
	lc := &cobra.Command{
		Use:   "grant <ttl>",
		Short: "创建租约",

		Run: leaseGrantCommandFunc,
	}

	return lc
}

func leaseGrantCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("lease grant命令需要TTL参数"))
	}

	ttl, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("错误的ttl (%v)", err))
	}

	ctx, cancel := commandCtx(cmd)
	resp, err := mustClientFromCmd(cmd).Grant(ctx, ttl)
	cancel()
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, fmt.Errorf("创建租约失败 (%v)", err))
	}
	display.Grant(*resp)
}

// NewLeaseRevokeCommand returns the cobra command for "lease revoke".
func NewLeaseRevokeCommand() *cobra.Command {
	lc := &cobra.Command{
		Use:   "revoke <leaseID>",
		Short: "移除租约",

		Run: leaseRevokeCommandFunc,
	}

	return lc
}

// leaseRevokeCommandFunc executes the "lease grant" command.
func leaseRevokeCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("lease revoke command needs 1 argument"))
	}

	id := leaseFromArgs(args[0])
	ctx, cancel := commandCtx(cmd)
	resp, err := mustClientFromCmd(cmd).Revoke(ctx, id)
	cancel()
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, fmt.Errorf("failed to revoke lease (%v)", err))
	}
	display.Revoke(id, *resp)
}

var timeToLiveKeys bool

// NewLeaseTimeToLiveCommand returns the cobra command for "lease timetolive".
func NewLeaseTimeToLiveCommand() *cobra.Command {
	lc := &cobra.Command{
		Use:   "timetolive <leaseID> [options]",
		Short: "获取租约信息",

		Run: leaseTimeToLiveCommandFunc,
	}
	lc.Flags().BoolVar(&timeToLiveKeys, "keys", false, "获取租约附加到了哪些key上")

	return lc
}

// leaseTimeToLiveCommandFunc executes the "lease timetolive" command.
func leaseTimeToLiveCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("lease timetolive command needs lease ID as argument"))
	}
	var opts []v3.LeaseOption
	if timeToLiveKeys {
		opts = append(opts, v3.WithAttachedKeys())
	}
	resp, rerr := mustClientFromCmd(cmd).TimeToLive(context.TODO(), leaseFromArgs(args[0]), opts...)
	if rerr != nil {
		cobrautl.ExitWithError(cobrautl.ExitBadConnection, rerr)
	}
	display.TimeToLive(*resp, timeToLiveKeys)
}

// NewLeaseListCommand returns the cobra command for "lease list".
func NewLeaseListCommand() *cobra.Command {
	lc := &cobra.Command{
		Use:   "list",
		Short: "显示所有租约",
		Run:   leaseListCommandFunc,
	}
	return lc
}

// leaseListCommandFunc executes the "lease list" command.
func leaseListCommandFunc(cmd *cobra.Command, args []string) {
	resp, rerr := mustClientFromCmd(cmd).Leases(context.TODO())
	if rerr != nil {
		cobrautl.ExitWithError(cobrautl.ExitBadConnection, rerr)
	}
	display.Leases(*resp)
}

var leaseKeepAliveOnce bool

// NewLeaseKeepAliveCommand returns the cobra command for "lease keep-alive".
func NewLeaseKeepAliveCommand() *cobra.Command {
	lc := &cobra.Command{
		Use:   "keep-alive [options] <leaseID>",
		Short: "重续租约 [renew]",

		Run: leaseKeepAliveCommandFunc,
	}

	lc.Flags().BoolVar(&leaseKeepAliveOnce, "once", false, "Resets the keep-alive time to its original value and cobrautl.Exits immediately")

	return lc
}

// leaseKeepAliveCommandFunc executes the "lease keep-alive" command.
func leaseKeepAliveCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("lease keep-alive命令需要lease ID作为参数"))
	}

	id := leaseFromArgs(args[0])

	if leaseKeepAliveOnce {
		respc, kerr := mustClientFromCmd(cmd).KeepAliveOnce(context.TODO(), id)
		if kerr != nil {
			cobrautl.ExitWithError(cobrautl.ExitBadConnection, kerr)
		}
		display.KeepAlive(*respc)
		return
	}

	respc, kerr := mustClientFromCmd(cmd).KeepAlive(context.TODO(), id)
	if kerr != nil {
		cobrautl.ExitWithError(cobrautl.ExitBadConnection, kerr)
	}
	for resp := range respc {
		display.KeepAlive(*resp)
	}

	if _, ok := (display).(*simplePrinter); ok {
		fmt.Printf("租约 %016x 过期或移除.\n", id)
	}
}

func leaseFromArgs(arg string) v3.LeaseID {
	id, err := strconv.ParseInt(arg, 16, 64)
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("bad lease ID arg (%v), expecting ID in Hex", err))
	}
	return v3.LeaseID(id)
}
