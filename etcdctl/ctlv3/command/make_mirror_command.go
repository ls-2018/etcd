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
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bgentry/speakeasy"
	clientv3 "github.com/ls-2018/etcd_cn/client_sdk/v3"
	"github.com/ls-2018/etcd_cn/pkg/cobrautl"

	"github.com/ls-2018/etcd_cn/client_sdk/v3/mirror"
	"github.com/ls-2018/etcd_cn/offical/api/v3/mvccpb"
	"github.com/ls-2018/etcd_cn/offical/api/v3/v3rpc/rpctypes"

	"github.com/spf13/cobra"
)

var (
	mminsecureTr   bool
	mmcert         string
	mmkey          string
	mmcacert       string
	mmprefix       string
	mmdestprefix   string
	mmuser         string
	mmpassword     string
	mmnodestprefix bool
)

// NewMakeMirrorCommand returns the cobra command for "makeMirror".
func NewMakeMirrorCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "make-mirror [options] <destination>",
		Short: "在目标etcd集群上创建镜像",
		Run:   makeMirrorCommandFunc,
	}

	c.Flags().StringVar(&mmprefix, "prefix", "", "为那个前缀打快照")
	c.Flags().StringVar(&mmdestprefix, "dest-prefix", "", "将一个source前缀 镜像到 目标集群中的另一个前缀")
	c.Flags().BoolVar(&mmnodestprefix, "no-dest-prefix", false, "kv镜像到另一个集群的根目录下")
	c.Flags().StringVar(&mmcert, "dest-cert", "", "使用此TLS证书文件为目标集群识别安全客户端")
	c.Flags().StringVar(&mmkey, "dest-key", "", "使用此TLS私钥文件为目标集群识别安全客户端")
	c.Flags().StringVar(&mmcacert, "dest-cacert", "", "使用此CA包验证启用TLS的安全服务器的证书")
	c.Flags().BoolVar(&mminsecureTr, "dest-insecure-transport", true, "为客户端连接禁用传输安全性")
	c.Flags().StringVar(&mmuser, "dest-user", "", "目标集群的 username[:password]")
	c.Flags().StringVar(&mmpassword, "dest-password", "", "目标集群的密码")

	return c
}

func authDestCfg() *authCfg {
	if mmuser == "" {
		return nil
	}

	var cfg authCfg

	if mmpassword == "" {
		splitted := strings.SplitN(mmuser, ":", 2)
		if len(splitted) < 2 {
			var err error
			cfg.username = mmuser
			cfg.password, err = speakeasy.Ask("Destination Password: ")
			if err != nil {
				cobrautl.ExitWithError(cobrautl.ExitError, err)
			}
		} else {
			cfg.username = splitted[0]
			cfg.password = splitted[1]
		}
	} else {
		cfg.username = mmuser
		cfg.password = mmpassword
	}

	return &cfg
}

func makeMirrorCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, errors.New("make-mirror takes one destination argument"))
	}

	dialTimeout := dialTimeoutFromCmd(cmd)
	keepAliveTime := keepAliveTimeFromCmd(cmd)
	keepAliveTimeout := keepAliveTimeoutFromCmd(cmd)
	sec := &secureCfg{
		cert:              mmcert,
		key:               mmkey,
		cacert:            mmcacert,
		insecureTransport: mminsecureTr,
	}

	auth := authDestCfg()

	cc := &clientConfig{
		endpoints:        []string{args[0]},
		dialTimeout:      dialTimeout,
		keepAliveTime:    keepAliveTime,
		keepAliveTimeout: keepAliveTimeout,
		scfg:             sec,
		acfg:             auth,
	}
	dc := cc.mustClient() // 目标集群
	c := mustClientFromCmd(cmd)

	err := makeMirror(context.TODO(), c, dc)
	cobrautl.ExitWithError(cobrautl.ExitError, err)
}

func makeMirror(ctx context.Context, c *clientv3.Client, dc *clientv3.Client) error {
	total := int64(0)

	go func() {
		for {
			time.Sleep(30 * time.Second)
			fmt.Println("total--->:", atomic.LoadInt64(&total))
		}
	}()

	s := mirror.NewSyncer(c, mmprefix, 0)

	rc, errc := s.SyncBase(ctx)

	// 如果指定并删除目的前缀,则返回错误
	if mmnodestprefix && len(mmdestprefix) > 0 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("`--dest-prefix` and `--no-dest-prefix` cannot be set at the same time, choose one"))
	}

	// if remove destination prefix is false and destination prefix is empty set the value of destination prefix same as prefix
	if !mmnodestprefix && len(mmdestprefix) == 0 {
		mmdestprefix = mmprefix
	}

	for r := range rc {
		for _, kv := range r.Kvs {
			_, err := dc.Put(ctx, modifyPrefix(kv.Key), kv.Value)
			if err != nil {
				return err
			}
			atomic.AddInt64(&total, 1)
		}
	}

	err := <-errc
	if err != nil {
		return err
	}

	wc := s.SyncUpdates(ctx)

	for wr := range wc {
		if wr.CompactRevision != 0 {
			return rpctypes.ErrCompacted
		}

		var lastRev int64
		var ops []clientv3.Op

		for _, ev := range wr.Events {
			nextRev := ev.Kv.ModRevision
			if lastRev != 0 && nextRev > lastRev {
				_, err := dc.Txn(ctx).Then(ops...).Commit()
				if err != nil {
					return err
				}
				ops = []clientv3.Op{}
			}
			lastRev = nextRev
			switch ev.Type {
			case mvccpb.PUT:
				ops = append(ops, clientv3.OpPut(modifyPrefix(ev.Kv.Key), ev.Kv.Value))
				atomic.AddInt64(&total, 1)
			case mvccpb.DELETE:
				ops = append(ops, clientv3.OpDelete(modifyPrefix(ev.Kv.Key)))
				atomic.AddInt64(&total, 1)
			default:
				panic("unexpected event type")
			}
		}

		if len(ops) != 0 {
			_, err := dc.Txn(ctx).Then(ops...).Commit()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func modifyPrefix(key string) string {
	return strings.Replace(key, mmprefix, mmdestprefix, 1)
}
