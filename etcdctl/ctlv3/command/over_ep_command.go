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
	"sync"
	"time"

	v3 "github.com/ls-2018/etcd_cn/client_sdk/v3"
	"github.com/ls-2018/etcd_cn/offical/api/v3/v3rpc/rpctypes"
	"github.com/ls-2018/etcd_cn/offical/etcdserverpb"
	"github.com/ls-2018/etcd_cn/pkg/cobrautl"
	"github.com/ls-2018/etcd_cn/pkg/flags"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	epClusterEndpoints bool
	epHashKVRev        int64
)

// NewEndpointCommand returns the cobra command for "endpoint".
func NewEndpointCommand() *cobra.Command {
	ec := &cobra.Command{
		Use:   "endpoint <subcommand>",
		Short: "Endpoint related commands",
	}

	ec.PersistentFlags().BoolVar(&epClusterEndpoints, "cluster", false, "use all endpoints from the cluster member list")
	ec.AddCommand(newEpHealthCommand())
	ec.AddCommand(newEpStatusCommand())
	ec.AddCommand(newEpHashKVCommand())

	return ec
}

func newEpHealthCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "health",
		Short: "检查端点的健康程度",
		Run:   epHealthCommandFunc,
	}

	return cmd
}

func newEpStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "打印出指定端点的状态",
		Long:  ``,
		Run:   epStatusCommandFunc,
	}
}

func newEpHashKVCommand() *cobra.Command {
	hc := &cobra.Command{
		Use:   "hashkv",
		Short: "输出每个端点的KV历史哈希值",
		Run:   epHashKVCommandFunc,
	}
	hc.PersistentFlags().Int64Var(&epHashKVRev, "rev", 0, "maximum revision to hash (default: all revisions)")
	return hc
}

type epHealth struct {
	Ep     string `json:"endpoint"`
	Health bool   `json:"health"`
	Took   string `json:"took"`
	Error  string `json:"error,omitempty"`
}

func epHealthCommandFunc(cmd *cobra.Command, args []string) {
	lg, err := zap.NewProduction()
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	flags.SetPflagsFromEnv(lg, "ETCDCTL", cmd.InheritedFlags())
	initDisplayFromCmd(cmd)

	sec := secureCfgFromCmd(cmd)
	dt := dialTimeoutFromCmd(cmd)
	ka := keepAliveTimeFromCmd(cmd)
	kat := keepAliveTimeoutFromCmd(cmd)
	auth := authCfgFromCmd(cmd)
	var cfgs []*v3.Config
	for _, ep := range endpointsFromCluster(cmd) {
		cfg, err := newClientCfg([]string{ep}, dt, ka, kat, sec, auth)
		if err != nil {
			cobrautl.ExitWithError(cobrautl.ExitBadArgs, err)
		}
		cfgs = append(cfgs, cfg)
	}

	var wg sync.WaitGroup
	hch := make(chan epHealth, len(cfgs))
	for _, cfg := range cfgs {
		wg.Add(1)
		go func(cfg *v3.Config) {
			defer wg.Done()
			ep := cfg.Endpoints[0]
			cfg.Logger = lg.Named("client")
			cli, err := v3.New(*cfg)
			if err != nil {
				hch <- epHealth{Ep: ep, Health: false, Error: err.Error()}
				return
			}
			st := time.Now()
			// 得到一个随机的key.只要我们能够获得响应而没有错误,端点就是健康状态.
			ctx, cancel := commandCtx(cmd)
			_, err = cli.Get(ctx, "health")
			eh := epHealth{Ep: ep, Health: false, Took: time.Since(st).String()}
			// 权限拒绝是可以的,因为提案通过协商一致得到它
			if err == nil || err == rpctypes.ErrPermissionDenied {
				eh.Health = true
			} else {
				eh.Error = err.Error()
			}

			if eh.Health {
				resp, err := cli.AlarmList(ctx)
				if err == nil && len(resp.Alarms) > 0 {
					eh.Health = false
					eh.Error = "存在警报(s): "
					for _, v := range resp.Alarms {
						switch v.Alarm {
						case etcdserverpb.AlarmType_NOSPACE:
							eh.Error = eh.Error + "NOSPACE "
						case etcdserverpb.AlarmType_CORRUPT:
							eh.Error = eh.Error + "CORRUPT "
						default:
							eh.Error = eh.Error + "UNKNOWN "
						}
					}
				} else if err != nil {
					eh.Health = false
					eh.Error = "无法获取alarm信息"
				}
			}
			cancel()
			hch <- eh
		}(cfg)
	}

	wg.Wait()
	close(hch)

	errs := false
	var healthList []epHealth
	for h := range hch {
		healthList = append(healthList, h)
		if h.Error != "" {
			errs = true
		}
	}
	display.EndpointHealth(healthList)
	if errs {
		cobrautl.ExitWithError(cobrautl.ExitError, fmt.Errorf("unhealthy cluster"))
	}
}

type epStatus struct {
	Ep   string             `json:"Endpoint"`
	Resp *v3.StatusResponse `json:"Status"`
}

func epStatusCommandFunc(cmd *cobra.Command, args []string) {
	c := mustClientFromCmd(cmd)

	var statusList []epStatus
	var err error
	for _, ep := range endpointsFromCluster(cmd) {
		ctx, cancel := commandCtx(cmd)
		resp, serr := c.Status(ctx, ep)
		cancel()
		if serr != nil {
			err = serr
			fmt.Fprintf(os.Stderr, "获取端点状态失败%s (%v)\n", ep, serr)
			continue
		}
		statusList = append(statusList, epStatus{Ep: ep, Resp: resp})
	}

	display.EndpointStatus(statusList)

	if err != nil {
		os.Exit(cobrautl.ExitError)
	}
}

type epHashKV struct {
	Ep   string             `json:"Endpoint"`
	Resp *v3.HashKVResponse `json:"HashKV"`
}

func epHashKVCommandFunc(cmd *cobra.Command, args []string) {
	c := mustClientFromCmd(cmd)

	hashList := []epHashKV{}
	var err error
	for _, ep := range endpointsFromCluster(cmd) {
		ctx, cancel := commandCtx(cmd)
		resp, serr := c.HashKV(ctx, ep, epHashKVRev)
		cancel()
		if serr != nil {
			err = serr
			fmt.Fprintf(os.Stderr, "Failed to get the hash of endpoint %s (%v)\n", ep, serr)
			continue
		}
		hashList = append(hashList, epHashKV{Ep: ep, Resp: resp})
	}

	display.EndpointHashKV(hashList)

	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
}

func endpointsFromCluster(cmd *cobra.Command) []string {
	if !epClusterEndpoints {
		endpoints, err := cmd.Flags().GetStringSlice("endpoints")
		if err != nil {
			cobrautl.ExitWithError(cobrautl.ExitError, err)
		}
		return endpoints
	}

	sec := secureCfgFromCmd(cmd)
	dt := dialTimeoutFromCmd(cmd)
	ka := keepAliveTimeFromCmd(cmd)
	kat := keepAliveTimeoutFromCmd(cmd)
	eps, err := endpointsFromCmd(cmd)
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	// exclude auth for not asking needless password (MemberList() doesn't need authentication)

	cfg, err := newClientCfg(eps, dt, ka, kat, sec, nil)
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	c, err := v3.New(*cfg)
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	ctx, cancel := commandCtx(cmd)
	defer func() {
		c.Close()
		cancel()
	}()
	membs, err := c.MemberList(ctx)
	if err != nil {
		err = fmt.Errorf("failed to fetch endpoints from etcd cluster member list: %v", err)
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	var ret []string
	for _, m := range membs.Members {
		ret = append(ret, m.ClientURLs...)
	}
	return ret
}
