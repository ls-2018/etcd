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
	"fmt"

	"github.com/ls-2018/etcd_cn/offical/api/v3/v3rpc/rpctypes"
	"github.com/ls-2018/etcd_cn/pkg/cobrautl"
	"github.com/spf13/cobra"
)

// NewAuthCommand returns the cobra command for "auth".
func NewAuthCommand() *cobra.Command {
	ac := &cobra.Command{
		Use:   "auth <enable or disable>",
		Short: "启用或禁用身份验证",
	}

	ac.AddCommand(newAuthEnableCommand())
	ac.AddCommand(newAuthDisableCommand())
	ac.AddCommand(newAuthStatusCommand())

	return ac
}

func newAuthStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "返回验证状态",
		Run:   authStatusCommandFunc,
	}
}

// authStatusCommandFunc executes the "auth status" command.
func authStatusCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("auth status命令不接受任何参数"))
	}

	ctx, cancel := commandCtx(cmd)
	result, err := mustClientFromCmd(cmd).Auth.AuthStatus(ctx)
	cancel()
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	display.AuthStatus(*result)
}

func newAuthEnableCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "enable",
		Short: "启用身份验证",
		Run:   authEnableCommandFunc,
	}
}

// authEnableCommandFunc executes the "auth enable" command.
func authEnableCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("auth enable命令不接受任何参数"))
	}

	ctx, cancel := commandCtx(cmd)
	cli := mustClientFromCmd(cmd)
	var err error
	for err == nil {
		if _, err = cli.AuthEnable(ctx); err == nil {
			break
		}
		if err == rpctypes.ErrRootRoleNotExist {
			if _, err = cli.RoleAdd(ctx, "root"); err != nil {
				break
			}
			if _, err = cli.UserGrantRole(ctx, "root", "root"); err != nil {
				break
			}
		}
	}
	cancel()
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	fmt.Println("身份验证启用")
}

func newAuthDisableCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "disable",
		Short: "禁用身份验证",
		Run:   authDisableCommandFunc,
	}
}

// authDisableCommandFunc executes the "auth disable" command.
func authDisableCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("auth disable命令不接受任何参数"))
	}

	ctx, cancel := commandCtx(cmd)
	_, err := mustClientFromCmd(cmd).Auth.AuthDisable(ctx)
	cancel()
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	fmt.Println("身份验证禁用")
}
