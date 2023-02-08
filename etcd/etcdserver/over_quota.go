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

package etcdserver

import (
	"sync"

	pb "github.com/ls-2018/etcd_cn/offical/etcdserverpb"

	humanize "github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

const (
	DefaultQuotaBytes = int64(2 * 1024 * 1024 * 1024) // 2GB 是指在超过空间配额之前后端大小可能消耗的字节数.
	MaxQuotaBytes     = int64(8 * 1024 * 1024 * 1024) // 8GB 是建议用于后端配额的最大字节数.较大的配额可能会导致性能下降.
)

// Quota 代表一个针对任意请求的任意配额.每个请求要花费一定的费用；如果没有足够的剩余费用那么配额内可用的资源就太少了无法应用该请求.
type Quota interface {
	Available(req interface{}) bool // 判断给定的请求是否符合配额要求.
	Cost(req interface{}) int       // 计算对某一请求的配额的开销.
	Remaining() int64               // 剩余配额
}

type passthroughQuota struct{}

func (*passthroughQuota) Available(interface{}) bool { return true }
func (*passthroughQuota) Cost(interface{}) int       { return 0 }
func (*passthroughQuota) Remaining() int64           { return 1 }

type backendQuota struct {
	s               *EtcdServer
	maxBackendBytes int64
}

const (
	leaseOverhead = 64  // 是对租约物的存储成本的估计.
	kvOverhead    = 256 // 是对存储一个密钥的元数据的成本的估计.
)

var (
	quotaLogOnce     sync.Once
	DefaultQuotaSize = humanize.Bytes(uint64(DefaultQuotaBytes))
	maxQuotaSize     = humanize.Bytes(uint64(MaxQuotaBytes))
)

// NewBackendQuota 创建一个具有给定存储限制的配额层.
func NewBackendQuota(s *EtcdServer, name string) Quota {
	lg := s.Logger()

	if s.Cfg.QuotaBackendBytes < 0 {
		quotaLogOnce.Do(func() {
			lg.Info("禁用后端配额", zap.String("quota-name", name), zap.Int64("quota-size-bytes", s.Cfg.QuotaBackendBytes))
		})
		return &passthroughQuota{}
	}

	if s.Cfg.QuotaBackendBytes == 0 {
		quotaLogOnce.Do(func() {
			if lg != nil {
				lg.Info(
					"启用后端配置默认值",
					zap.String("quota-name", name),
					zap.Int64("quota-size-bytes", DefaultQuotaBytes),
					zap.String("quota-size", DefaultQuotaSize),
				)
			}
		})
		return &backendQuota{s, DefaultQuotaBytes}
	}

	quotaLogOnce.Do(func() {
		if s.Cfg.QuotaBackendBytes > MaxQuotaBytes {
			lg.Warn(
				"配额超过了最大值",
				zap.String("quota-name", name),
				zap.Int64("quota-size-bytes", s.Cfg.QuotaBackendBytes),
				zap.String("quota-size", humanize.Bytes(uint64(s.Cfg.QuotaBackendBytes))),
				zap.Int64("quota-maximum-size-bytes", MaxQuotaBytes),
				zap.String("quota-maximum-size", maxQuotaSize),
			)
		}
		lg.Info(
			"启用配额",
			zap.String("quota-name", name),
			zap.Int64("quota-size-bytes", s.Cfg.QuotaBackendBytes),
			zap.String("quota-size", humanize.Bytes(uint64(s.Cfg.QuotaBackendBytes))),
		)
	})
	return &backendQuota{s, s.Cfg.QuotaBackendBytes}
}

// Available 粗略计算是否可以存储
func (b *backendQuota) Available(v interface{}) bool {
	return b.s.Backend().Size()+int64(b.Cost(v)) < b.maxBackendBytes
}

// Cost 操作的开销
func (b *backendQuota) Cost(v interface{}) int {
	switch r := v.(type) {
	case *pb.PutRequest:
		return costPut(r)
	case *pb.TxnRequest:
		return costTxn(r)
	case *pb.LeaseGrantRequest:
		return leaseOverhead
	default:
		panic("未知的 cost")
	}
}

func costPut(r *pb.PutRequest) int { return kvOverhead + len(r.Key) + len(r.Value) }

func costTxnReq(u *pb.RequestOp) int {
	r := u.GetRequestPut()
	if r == nil {
		return 0
	}
	return costPut(r)
}

func costTxn(r *pb.TxnRequest) int {
	sizeSuccess := 0
	for _, u := range r.Success {
		sizeSuccess += costTxnReq(u)
	}
	sizeFailure := 0
	for _, u := range r.Failure {
		sizeFailure += costTxnReq(u)
	}
	if sizeFailure > sizeSuccess {
		return sizeFailure
	}
	return sizeSuccess
}

func (b *backendQuota) Remaining() int64 {
	return b.maxBackendBytes - b.s.Backend().Size()
}
