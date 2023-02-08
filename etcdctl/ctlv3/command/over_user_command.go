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
	"strings"

	clientv3 "github.com/ls-2018/etcd_cn/client_sdk/v3"

	"github.com/bgentry/speakeasy"
	"github.com/ls-2018/etcd_cn/pkg/cobrautl"
	"github.com/spf13/cobra"
)

var userShowDetail bool

// NewUserCommand returns the cobra command for "user".
func NewUserCommand() *cobra.Command {
	ac := &cobra.Command{
		Use:   "user <subcommand>",
		Short: "User related commands",
	}

	ac.AddCommand(newUserAddCommand())
	ac.AddCommand(newUserDeleteCommand())
	ac.AddCommand(newUserGetCommand())
	ac.AddCommand(newUserListCommand())
	ac.AddCommand(newUserChangePasswordCommand())
	ac.AddCommand(newUserGrantRoleCommand())
	ac.AddCommand(newUserRevokeRoleCommand())

	return ac
}

var (
	passwordInteractive bool
	passwordFromFlag    string
	noPassword          bool
)

func newUserAddCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "add <user name or user:password> [options]",
		Short: "添加新用户",
		Run:   userAddCommandFunc,
	}

	cmd.Flags().BoolVar(&passwordInteractive, "interactive", true, "从stdin读取密码,而不是交互终端")
	cmd.Flags().StringVar(&passwordFromFlag, "new-user-password", "", "从命令行标志提供密码")
	cmd.Flags().BoolVar(&noPassword, "no-password", false, "创建一个没有密码的用户(仅基于CN的身份验证)")

	return &cmd
}

func newUserDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <user name>",
		Short: "删除一个用户",
		Run:   userDeleteCommandFunc,
	}
}

func newUserGetCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "get <user name> [options]",
		Short: "获取用户详情",
		Run:   userGetCommandFunc,
	}

	cmd.Flags().BoolVar(&userShowDetail, "detail", false, "显示授予用户的角色的权限")

	return &cmd
}

func newUserListCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "显示所有用户",
		Run:   userListCommandFunc,
	}
}

func newUserChangePasswordCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "passwd <user name> [options]",
		Short: "更改用户密码",
		Run:   userChangePasswordCommandFunc,
	}

	cmd.Flags().BoolVar(&passwordInteractive, "interactive", true, "如果为true,从stdin读取密码,而不是交互终端")

	return &cmd
}

func newUserGrantRoleCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "grant-role <user name> <role name>",
		Short: "授予用户权限",
		Run:   userGrantRoleCommandFunc,
	}
}

func newUserRevokeRoleCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "revoke-role <user name> <role name>",
		Short: "移除用户权限",
		Run:   userRevokeRoleCommandFunc,
	}
}

func userAddCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("用户add命令需要用户名作为参数"))
	}

	var password string
	var user string

	options := &clientv3.UserAddOptions{
		NoPassword: false,
	}

	if !noPassword { // 创建一个没有密码的用户(仅基于CN的身份验证)
		if passwordFromFlag != "" {
			user = args[0]
			password = passwordFromFlag
		} else {
			splitted := strings.SplitN(args[0], ":", 2)
			if len(splitted) < 2 {
				user = args[0]
				if !passwordInteractive {
					fmt.Scanf("%s", &password)
				} else {
					password = readPasswordInteractive(args[0])
				}
			} else {
				user = splitted[0]
				password = splitted[1]
				if len(user) == 0 {
					cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("用户名不允许为空"))
				}
			}
		}
	} else {
		user = args[0]
		options.NoPassword = true
	}

	resp, err := mustClientFromCmd(cmd).Auth.UserAddWithOptions(context.TODO(), user, password, options)
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	display.UserAdd(user, *resp)
}

// userDeleteCommandFunc executes the "user delete" command.
func userDeleteCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("用户删除命令需要用户名作为参数"))
	}

	resp, err := mustClientFromCmd(cmd).Auth.UserDelete(context.TODO(), args[0])
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	display.UserDelete(args[0], *resp)
}

// userGetCommandFunc executes the "user get" command.
func userGetCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("用户get命令需要用户名作为参数"))
	}

	name := args[0]
	client := mustClientFromCmd(cmd)
	resp, err := client.Auth.UserGet(context.TODO(), name)
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	if userShowDetail {
		fmt.Printf("User: %s\n", name)
		for _, role := range resp.Roles {
			fmt.Printf("\n")
			roleResp, err := client.Auth.RoleGet(context.TODO(), role)
			if err != nil {
				cobrautl.ExitWithError(cobrautl.ExitError, err)
			}
			display.RoleGet(role, *roleResp)
		}
	} else {
		display.UserGet(name, *resp)
	}
}

// userListCommandFunc executes the "user list" command.
func userListCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("user list命令不需要参数"))
	}

	resp, err := mustClientFromCmd(cmd).Auth.UserList(context.TODO())
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	display.UserList(*resp)
}

// userChangePasswordCommandFunc executes the "user passwd" command.
func userChangePasswordCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("用户passwd命令需要用户名作为参数"))
	}

	var password string

	if !passwordInteractive {
		fmt.Scanf("%s", &password)
	} else {
		password = readPasswordInteractive(args[0])
	}

	resp, err := mustClientFromCmd(cmd).Auth.UserChangePassword(context.TODO(), args[0], password)
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	display.UserChangePassword(*resp)
}

// userGrantRoleCommandFunc executes the "user grant-role" command.
func userGrantRoleCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("user grant命令需要用户名和角色名作为参数"))
	}

	resp, err := mustClientFromCmd(cmd).Auth.UserGrantRole(context.TODO(), args[0], args[1])
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	display.UserGrantRole(args[0], args[1], *resp)
}

// userRevokeRoleCommandFunc executes the "user revoke-role" command.
func userRevokeRoleCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("用户revoke-role需要用户名和角色名作为参数"))
	}

	resp, err := mustClientFromCmd(cmd).Auth.UserRevokeRole(context.TODO(), args[0], args[1])
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	display.UserRevokeRole(args[0], args[1], *resp)
}

func readPasswordInteractive(name string) string {
	prompt1 := fmt.Sprintf("%s密码: ", name)
	password1, err1 := speakeasy.Ask(prompt1)
	if err1 != nil {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("确认密码失败: %s", err1))
	}

	if len(password1) == 0 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("空密码"))
	}

	prompt2 := fmt.Sprintf("再次输入密码确认%s:", name)
	password2, err2 := speakeasy.Ask(prompt2)
	if err2 != nil {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("确认密码失败 %s", err2))
	}

	if password1 != password2 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("提供的密码不一致"))
	}

	return password1
}
