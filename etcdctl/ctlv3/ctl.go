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

//  ctlv3 包含用于v3 API的etcdctl的主入口点.
package ctlv3

import (
	"time"

	"github.com/ls-2018/etcd_cn/etcdctl/ctlv3/command"
	"github.com/ls-2018/etcd_cn/offical/api/v3/version"
	"github.com/ls-2018/etcd_cn/pkg/cobrautl"

	"github.com/spf13/cobra"
)

const (
	cliName        = "etcdctl"
	cliDescription = "etcd3的一个简单的命令行客户机."

	defaultDialTimeout      = 2 * time.Second
	defaultCommandTimeOut   = 5 * time.Second
	defaultKeepAliveTime    = 2 * time.Second
	defaultKeepAliveTimeOut = 6 * time.Second
)

var globalFlags = command.GlobalFlags{}

var rootCmd = &cobra.Command{
	Use:        cliName,
	Short:      cliDescription,
	SuggestFor: []string{"etcdctl"},
}

func init() {
	rootCmd.PersistentFlags().StringSliceVar(&globalFlags.Endpoints, "endpoints", []string{"127.0.0.1:2379"}, "gRPC端点")
	rootCmd.PersistentFlags().BoolVar(&globalFlags.Debug, "debug", false, "启用客户端调试日志记录")

	rootCmd.PersistentFlags().StringVarP(&globalFlags.OutputFormat, "write-out", "w", "simple", "设置输出格式 (fields, json, protobuf, simple, table)")
	rootCmd.PersistentFlags().BoolVar(&globalFlags.IsHex, "hex", false, "以十六进制编码的字符串输出字节串")

	rootCmd.PersistentFlags().DurationVar(&globalFlags.DialTimeout, "dial-timeout", defaultDialTimeout, "拨号客户端连接超时")
	rootCmd.PersistentFlags().DurationVar(&globalFlags.CommandTimeOut, "command-timeout", defaultCommandTimeOut, "运行命令的超时（不包括拨号超时）.")
	rootCmd.PersistentFlags().DurationVar(&globalFlags.KeepAliveTime, "keepalive-time", defaultKeepAliveTime, "客户端连接的存活时间")
	rootCmd.PersistentFlags().DurationVar(&globalFlags.KeepAliveTimeout, "keepalive-timeout", defaultKeepAliveTimeOut, "客户端连接的Keepalive超时")

	// TODO: secure by default when etcd enables secure gRPC by default.
	rootCmd.PersistentFlags().BoolVar(&globalFlags.Insecure, "insecure-transport", true, "为客户端连接禁用传输安全性")
	rootCmd.PersistentFlags().BoolVar(&globalFlags.InsecureDiscovery, "insecure-discovery", true, "接受描述集群端点的不安全的SRV记录")
	rootCmd.PersistentFlags().BoolVar(&globalFlags.InsecureSkipVerify, "insecure-skip-tls-verify", false, "跳过 etcd 证书验证 (注意:该选项仅用于测试目的.）")
	rootCmd.PersistentFlags().StringVar(&globalFlags.TLS.CertFile, "cert", "", "识别使用该TLS证书文件的安全客户端")
	rootCmd.PersistentFlags().StringVar(&globalFlags.TLS.KeyFile, "key", "", "识别使用该TLS密钥文件的安全客户端")
	rootCmd.PersistentFlags().StringVar(&globalFlags.TLS.TrustedCAFile, "cacert", "", "使用此CA包验证启用tls的安全服务器的证书")
	rootCmd.PersistentFlags().StringVar(&globalFlags.User, "user", "", "username[:password]  (如果没有提供密码,则提示)")
	rootCmd.PersistentFlags().StringVar(&globalFlags.Password, "password", "", "身份验证的密码(如果使用了这个选项,——user选项不应该包含密码)")
	rootCmd.PersistentFlags().StringVarP(&globalFlags.TLS.ServerName, "discovery-srv", "d", "", "查询描述集群端点的SRV记录的域名")
	rootCmd.PersistentFlags().StringVarP(&globalFlags.DNSClusterServiceName, "discovery-srv-name", "", "", "使用DNS发现时需要查询的服务名称")

	rootCmd.AddCommand(
		command.NewGetCommand(),
		command.NewPutCommand(), // ✅
		command.NewDelCommand(),
		command.NewTxnCommand(),
		command.NewCompactionCommand(),
		command.NewAlarmCommand(),
		command.NewDefragCommand(),
		command.NewEndpointCommand(),
		command.NewMoveLeaderCommand(),
		command.NewWatchCommand(),
		command.NewVersionCommand(),
		command.NewLeaseCommand(),
		command.NewMemberCommand(),
		command.NewSnapshotCommand(),
		command.NewMakeMirrorCommand(),
		command.NewLockCommand(),
		command.NewElectCommand(),
		command.NewAuthCommand(),
		command.NewUserCommand(),
		command.NewRoleCommand(),
		command.NewCheckCommand(),
	)
}

func usageFunc(c *cobra.Command) error {
	return cobrautl.UsageFunc(c, version.Version, version.APIVersion)
}

func Start() error {
	rootCmd.SetUsageFunc(usageFunc)
	// Make help just show the usage
	rootCmd.SetHelpTemplate(`{{.UsageString}}`)
	return rootCmd.Execute()
}

func MustStart() {
	if err := Start(); err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
}

func init() {
	cobra.EnablePrefixMatching = true
}
