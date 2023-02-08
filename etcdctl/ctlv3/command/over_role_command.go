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

	clientv3 "github.com/ls-2018/etcd_cn/client_sdk/v3"

	"github.com/ls-2018/etcd_cn/pkg/cobrautl"
	"github.com/spf13/cobra"
)

var (
	rolePermPrefix  bool
	rolePermFromKey bool
)

// NewRoleCommand returns the cobra command for "role".
func NewRoleCommand() *cobra.Command {
	ac := &cobra.Command{
		Use:   "role <subcommand>",
		Short: "Role related commands",
	}

	ac.AddCommand(newRoleAddCommand())
	ac.AddCommand(newRoleDeleteCommand())
	ac.AddCommand(newRoleGetCommand())
	ac.AddCommand(newRoleListCommand())
	ac.AddCommand(newRoleGrantPermissionCommand())
	ac.AddCommand(newRoleRevokePermissionCommand())

	return ac
}

func newRoleAddCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "add <role name>",
		Short: "添加一个角色",
		Run:   roleAddCommandFunc,
	}
}

func newRoleDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <role name>",
		Short: "删除一个角色",
		Run:   roleDeleteCommandFunc,
	}
}

func newRoleGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "get <role name>",
		Short: "获取一个角色的详细信息",
		Run:   roleGetCommandFunc,
	}
}

func newRoleListCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "显示所有角色",
		Run:   roleListCommandFunc,
	}
}

func newRoleGrantPermissionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "grant-permission [options] <role name> <permission type> <key> [endkey]",
		Short: "给角色授予一个权限",
		Run:   roleGrantPermissionCommandFunc,
	}

	cmd.Flags().BoolVar(&rolePermPrefix, "prefix", false, "授予前缀权限")
	cmd.Flags().BoolVar(&rolePermFromKey, "from-key", false, "使用byte compare授予大于或等于给定键的权限")

	return cmd
}

func newRoleRevokePermissionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revoke-permission <role name> <key> [endkey]",
		Short: "移除角色权限里的一个key",
		Run:   roleRevokePermissionCommandFunc,
	}

	cmd.Flags().BoolVar(&rolePermPrefix, "prefix", false, "取消前缀权限")
	cmd.Flags().BoolVar(&rolePermFromKey, "from-key", false, "使用byte compare撤销大于或等于给定键的权限")

	return cmd
}

func roleAddCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("role add命令需要角色名作为参数"))
	}

	resp, err := mustClientFromCmd(cmd).Auth.RoleAdd(context.TODO(), args[0])
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	display.RoleAdd(args[0], *resp)
}

func roleDeleteCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("role delete command requires role name as its argument"))
	}

	resp, err := mustClientFromCmd(cmd).Auth.RoleDelete(context.TODO(), args[0])
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	display.RoleDelete(args[0], *resp)
}

func roleGetCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("role get命令需要角色名作为参数"))
	}

	name := args[0]
	resp, err := mustClientFromCmd(cmd).Auth.RoleGet(context.TODO(), name)
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	display.RoleGet(name, *resp)
}

// roleListCommandFunc executes the "role list" command.
func roleListCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("role list command requires no arguments"))
	}

	resp, err := mustClientFromCmd(cmd).Auth.RoleList(context.TODO())
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	display.RoleList(*resp)
}

func roleGrantPermissionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 3 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("role grant命令需要角色名、权限类型和关键字[endkey]作为参数"))
	}

	perm, err := clientv3.StrToPermissionType(args[1]) // read write readwrite
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, err)
	}

	key, rangeEnd := permRange(args[2:])
	resp, err := mustClientFromCmd(cmd).Auth.RoleGrantPermission(context.TODO(), args[0], key, rangeEnd, perm)
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	display.RoleGrantPermission(args[0], *resp)
}

func roleRevokePermissionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 2 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("role revoke-permission命令需要角色名和关键字[endkey]作为参数"))
	}

	key, rangeEnd := permRange(args[1:])
	resp, err := mustClientFromCmd(cmd).Auth.RoleRevokePermission(context.TODO(), args[0], key, rangeEnd)
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	display.RoleRevokePermission(args[0], args[1], rangeEnd, *resp)
}

func permRange(args []string) (string, string) {
	key := args[0]
	var rangeEnd string
	if len(key) == 0 {
		if rolePermPrefix && rolePermFromKey {
			cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("--from-key and --prefix flags 是互相排斥的 "))
		}

		// Range permission is expressed as adt.BytesAffineInterval,
		// so the empty prefix which should be matched with every key must be like this ["\x00", <end>).
		key = "\x00"
		if rolePermPrefix || rolePermFromKey {
			// For the both cases of prefix and from-key, a permission with an empty key
			// should allow access to the entire key space.
			// 0x00 will be treated as open ended in etcd side.
			rangeEnd = "\x00"
		}
	} else {
		var err error
		rangeEnd, err = rangeEndFromPermFlags(args[0:])
		if err != nil {
			cobrautl.ExitWithError(cobrautl.ExitBadArgs, err)
		}
	}
	return key, rangeEnd
}

func rangeEndFromPermFlags(args []string) (string, error) {
	if len(args) == 1 {
		if rolePermPrefix {
			if rolePermFromKey {
				return "", fmt.Errorf("--from-key and --prefix flags are mutually exclusive")
			}
			return clientv3.GetPrefixRangeEnd(args[0]), nil
		}
		if rolePermFromKey {
			return "\x00", nil
		}
		// single key case
		return "", nil
	}
	if rolePermPrefix {
		return "", fmt.Errorf("unexpected endkey argument with --prefix flag")
	}
	if rolePermFromKey {
		return "", fmt.Errorf("unexpected endkey argument with --from-key flag")
	}
	return args[1], nil
}
