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

package etcdmain

import (
	"fmt"
	"strconv"

	cconfig "github.com/ls-2018/etcd_cn/etcd/config"
	"github.com/ls-2018/etcd_cn/etcd/embed"
	"golang.org/x/crypto/bcrypt"
)

var (
	usageline = `Usage:

  etcd [flags]
    Start an etcd etcd.

  etcd --version
    Show the version of etcd.

  etcd -h | --help
    Show the help information about etcd.

  etcd --config-file
    Path to the etcd configuration file. Note that if a configuration file is provided, other command line flags and environment variables will be ignored.

  etcd gateway
    启动 L4 TCP网关代理

  etcd grpc-proxy
    L7 grpc 代理
`
	flagsline = `
Member:
  --name 'default'
    本节点.人类可读的名字
  --data-dir '${name}.etcd'
    服务运行数据保存的路径. ${name}.etcd
  --wal-dir ''
    专用wal目录的路径.默认值:--data-dir的路径下
  --snapshot-count '100000'
    触发快照到磁盘的已提交事务数.
  --heartbeat-interval '100'
    心跳间隔 100ms
  --election-timeout '1000'
    选举超时
  --initial-election-tick-advance 'true'
    是否提前初始化选举时钟启动,以便更快的选举
  --listen-peer-urls 'http://localhost:2380'
    和成员之间通信的地址.用于监听其他etcd member的url
  --listen-client-urls 'http://localhost:2379'
    List of URLs to listen on for client traffic.
  --max-snapshots '` + strconv.Itoa(embed.DefaultMaxSnapshots) + `'
    要保留的最大快照文件数(0表示不受限制).5
  --max-wals '` + strconv.Itoa(embed.DefaultMaxWALs) + `'
    要保留的最大wal文件数(0表示不受限制). 5
  --quota-backend-bytes '0'
    当后端大小超过给定配额时(0默认为低空间配额).引发警报.
  --backend-bbolt-freelist-type 'map'
    BackendFreelistType指定boltdb后端使用的freelist的类型(array and map是支持的类型). map 
  --backend-batch-interval ''
    BackendBatchInterval是提交后端事务前的最长时间.
  --backend-batch-limit '0'
    BackendBatchLimit是提交后端事务前的最大操作数.
  --max-txn-ops '128'
    事务中允许的最大操作数.
  --max-request-bytes '1572864'
    服务器将接受的最大客户端请求大小(字节).
  --grpc-keepalive-min-time '5s'
    客户端在ping服务器之前应等待的最短持续时间间隔.
  --grpc-keepalive-interval '2h'
    服务器到客户端ping的频率持续时间.以检查连接是否处于活动状态(0表示禁用).
  --grpc-keepalive-timeout '20s'
    关闭非响应连接之前的额外持续等待时间(0表示禁用).20s
  --socket-reuse-port 'false'
    启用在listener上设置套接字选项SO_REUSEPORT.允许重新绑定一个已经在使用的端口.false
  --socket-reuse-address 'false'
    启用在listener上设置套接字选项SO_REUSEADDR 允许重新绑定一个已经在使用的端口 在TIME_WAIT 状态.

Clustering:
  --initial-advertise-peer-urls 'http://localhost:2380'
    集群成员的 URL地址.且会通告群集的其余成员节点.
  --initial-cluster 'default=http://localhost:2380'
    集群中所有节点的信息.
  --initial-cluster-state 'new'
    初始集群状态 ('new' or 'existing').
  --initial-cluster-token 'etcd-cluster'
    创建集群的 token.这个值每个集群保持唯一.
  --advertise-client-urls 'http://localhost:2379'
    监听client的请求
    The client URLs advertised should be accessible to machines that talk to etcd cluster. etcd client libraries parse these URLs to connect to the cluster.
  --discovery ''
    用于引导群集的发现URL.
  --discovery-fallback 'proxy'
    发现服务失败时的预期行为("退出"或"代理")."proxy"仅支持v2 API. %q
  --discovery-proxy ''
    用于流量到发现服务的HTTP代理.
  --discovery-srv ''
    DNS srv域用于引导群集.
  --discovery-srv-name ''
    使用DNS引导时查询的DNS srv名称的后缀.
  --strict-reconfig-check '` + strconv.FormatBool(embed.DefaultStrictReconfigCheck) + `'
    拒绝可能导致仲裁丢失的重新配置请求.true
  --pre-vote 'true'
    Enable to run an additional Raft election phase.
  --auto-compaction-retention '0'
    Auto compaction retention length. 0 means disable auto compaction.
  --auto-compaction-mode 'periodic'
    Interpret 'auto-compaction-retention' one of: periodic|revision. 'periodic' for duration based retention, defaulting to hours if no time unit is provided (e.g. '5m'). 'revision' for revision number based retention.
  --v2-deprecation '` + string(cconfig.V2_DEPR_DEFAULT) + `'
    Phase of v2store deprecation. Allows to opt-in for higher compatibility mode.
    Supported values:
      'not-yet'                // Issues a warning if v2store have meaningful content (default in v3.5)
      'write-only'             // Custom v2 state is not allowed (planned default in v3.6)
      'write-only-drop-data'   // Custom v2 state will get DELETED !
      'gone'                   // v2store is not maintained any longer. (planned default in v3.7)

Security:
  --cert-file ''
    客户端证书
  --key-file ''
    客户端私钥
  --client-cert-auth 'false'
    启用客户端证书验证;默认false
  --client-crl-file ''
    客户端证书吊销列表文件的路径.
  --client-cert-allowed-hostname ''
    允许客户端证书认证使用TLS主机名
  --trusted-ca-file ''
    客户端etcd通信 的可信CA证书文件
  --auto-tls 'false'
    节点之间使用生成的证书通信;默认false
  --peer-cert-file ''
    证书路径
  --peer-key-file ''
    私钥路径
  --peer-client-cert-auth 'false'
    启用server客户端证书验证;默认false
  --peer-trusted-ca-file ''
    服务器端ca证书
  --peer-cert-allowed-cn ''
    允许的server客户端证书CommonName
  --peer-cert-allowed-hostname ''
    允许的server客户端证书hostname
  --peer-auto-tls 'false'
    节点之间使用生成的证书通信;默认false
  --self-signed-cert-validity '1'
    客户端证书和同级证书的有效期,单位为年 ;etcd自动生成的 如果指定了ClientAutoTLS and PeerAutoTLS,
  --peer-crl-file ''
    服务端证书吊销列表文件的路径.
  --cipher-suites ''
    客户端/etcds之间支持的TLS加密套件的逗号分隔列表(空将由Go自动填充).
  --cors '*'
    Comma-separated whitelist of origins for CORS, or cross-origin resource sharing, (empty or * means allow all).
  --host-whitelist '*'
    Acceptable hostnames from HTTP client requests, if etcd is not secure (empty or * means allow all).

Auth:
  --auth-token 'simple'
    指定验证令牌的具体选项. ('simple' or 'jwt')
  --bcrypt-cost ` + fmt.Sprintf("%d", bcrypt.DefaultCost) + `
    为散列身份验证密码指定bcrypt算法的成本/强度.有效值介于4和31之间.
  --auth-token-ttl 300
    token过期时间

Profiling and Monitoring:
  --enable-pprof 'false'
    通过HTTP服务器启用运行时分析数据.地址位于客户端URL +"/ debug / pprof /"
  --metrics 'basic'
    设置导出的指标的详细程度,指定"扩展"以包括直方图指标(extensive,basic)
  --listen-metrics-urls ''
    List of URLs to listen on for the metrics and health endpoints.

Logging:
  --logger 'zap'
    Currently only supports 'zap' for structured logging.
  --log-outputs 'default'
    指定'stdout'或'stderr'以跳过日志记录,即使在systemd或逗号分隔的输出目标列表下运行也是如此.
  --log-level 'info'
    日志等级,只支持 debug, info, warn, error, panic, or fatal. Default 'info'.
  --enable-log-rotation 'false'
    启用单个日志输出文件目标的日志旋转.
  --log-rotation-config-json '{"maxsize": 100, "maxage": 0, "maxbackups": 0, "localtime": false, "compress": false}'
    是用于日志轮换的默认配置. 默认情况下,日志轮换是禁用的.  MaxSize(MB), MaxAge(days,0=no limit), MaxBackups(0=no limit), LocalTime(use computers local time), Compress(gzip)". 

Experimental distributed tracing:
  --experimental-enable-distributed-tracing 'false'
    Enable experimental distributed tracing.
  --experimental-distributed-tracing-address 'localhost:4317'
    Distributed tracing collector address.
  --experimental-distributed-tracing-service-name 'etcd'
    Distributed tracing service name,必须是same across all etcd instances.
  --experimental-distributed-tracing-instance-id ''
    Distributed tracing instance ID,必须是unique per each etcd instance.

v2 Proxy (to be deprecated in v3.6):
  --proxy 'off'
    代理模式设置  ('off', 'readonly' or 'on').
  --proxy-failure-wait 5000
    在重新考虑代理请求之前.endpoints 将处于失败状态的时间(以毫秒为单位).
  --proxy-refresh-interval 30000
    endpoints 刷新间隔的时间(以毫秒为单位).
  --proxy-dial-timeout 1000
    拨号超时的时间(以毫秒为单位)或0表示禁用超时
  --proxy-write-timeout 5000
    写入超时的时间(以毫秒为单位)或0以禁用超时.
  --proxy-read-timeout 0
    读取超时的时间(以毫秒为单位)或0以禁用超时.

Experimental feature:
  --experimental-initial-corrupt-check 'false'
    Enable to check data corruption before serving any client/peer traffic.
  --experimental-corrupt-check-time '0s'
    Duration of time between cluster corruption check passes.
  --experimental-enable-v2v3 ''
    Serve v2 requests through the v3 backend under a given prefix. Deprecated and to be decommissioned in v3.6.
  --experimental-enable-lease-checkpoint 'false'
    ExperimentalEnableLeaseCheckpoint enables primary lessor to persist lease remainingTTL to prevent indefinite auto-renewal of long lived leases.
  --experimental-compaction-batch-limit 1000
    ExperimentalCompactionBatchLimit sets the maximum revisions deleted in each compaction batch.
  --experimental-peer-skip-client-san-verification 'false'
    跳过server 客户端证书中SAN字段的验证.默认false
  --experimental-watch-progress-notify-interval '10m'
    Duration of periodic watch progress notifications.
  --experimental-warning-apply-duration '100ms'
    时间长度.如果应用请求的时间超过这个值.就会产生一个警告.
  --experimental-txn-mode-write-with-shared-buffer 'true'
    启用写事务在其只读检查操作中使用共享缓冲区.
  --experimental-bootstrap-defrag-threshold-megabytes
    Enable the defrag during etcd etcd bootstrap on condition that it will free at least the provided threshold of disk space. Needs to be set to non-zero value to take effect.

Unsafe feature:
  --force-new-cluster 'false'
    强制创建新的单成员群集.它提交配置更改,强制删除集群中的所有现有成员并添加自身.需要将其设置为还原备份.
  --unsafe-no-fsync 'false'
    禁用fsync,不安全,会导致数据丢失.

CAUTIOUS with unsafe flag! It may break the guarantees given by the consensus protocol!
`
)

// Add back "TO BE DEPRECATED" section if needed
