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

package rpctypes

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// server-side error
var (
	ErrGRPCEmptyKey      = status.New(codes.InvalidArgument, "etcdserver: key is not provided").Err()
	ErrGRPCKeyNotFound   = status.New(codes.InvalidArgument, "etcdserver: key not found").Err()
	ErrGRPCValueProvided = status.New(codes.InvalidArgument, "etcdserver: value is provided").Err()
	ErrGRPCLeaseProvided = status.New(codes.InvalidArgument, "etcdserver: lease is provided").Err()
	ErrGRPCTooManyOps    = status.New(codes.InvalidArgument, "etcdserver: too many operations in txn request").Err()
	ErrGRPCDuplicateKey  = status.New(codes.InvalidArgument, "etcdserver: duplicate key given in txn request").Err()
	ErrGRPCCompacted     = status.New(codes.OutOfRange, "etcdserver: mvcc: 所需的修订版 已被压缩").Err()
	ErrGRPCFutureRev     = status.New(codes.OutOfRange, "etcdserver: mvcc: 所需的修订版是一个未来版本").Err()
	ErrGRPCNoSpace       = status.New(codes.ResourceExhausted, "etcdserver: mvcc: database space exceeded").Err()

	ErrGRPCLeaseNotFound    = status.New(codes.NotFound, "etcdserver: 请求的租约不存在").Err()
	ErrGRPCLeaseExist       = status.New(codes.FailedPrecondition, "etcdserver: lease already exists").Err()
	ErrGRPCLeaseTTLTooLarge = status.New(codes.OutOfRange, "etcdserver: too large lease TTL").Err()

	ErrGRPCWatchCanceled = status.New(codes.Canceled, "etcdserver: watch 取消了").Err()

	ErrGRPCMemberExist            = status.New(codes.FailedPrecondition, "etcdserver: member ID already exist").Err()
	ErrGRPCPeerURLExist           = status.New(codes.FailedPrecondition, "etcdserver: Peer URLs already exists").Err()
	ErrGRPCMemberNotEnoughStarted = status.New(codes.FailedPrecondition, "etcdserver: re-configuration failed due to not enough started members").Err()
	ErrGRPCMemberBadURLs          = status.New(codes.InvalidArgument, "etcdserver: given member URLs are invalid").Err()
	ErrGRPCMemberNotFound         = status.New(codes.NotFound, "etcdserver: member not found").Err()
	ErrGRPCMemberNotLearner       = status.New(codes.FailedPrecondition, "etcdserver: can only promote a learner member").Err()
	ErrGRPCLearnerNotReady        = status.New(codes.FailedPrecondition, "etcdserver: can only promote a learner member which is in sync with leader").Err()
	ErrGRPCTooManyLearners        = status.New(codes.FailedPrecondition, "etcdserver: too many learner members in cluster").Err()

	ErrGRPCRequestTooLarge        = status.New(codes.InvalidArgument, "etcdserver: 请求体太大").Err()
	ErrGRPCRequestTooManyRequests = status.New(codes.ResourceExhausted, "etcdserver: 请求次数太多").Err()

	ErrGRPCRootUserNotExist     = status.New(codes.FailedPrecondition, "etcdserver: root用户不存在").Err()
	ErrGRPCRootRoleNotExist     = status.New(codes.FailedPrecondition, "etcdserver: root用户没有root角色").Err()
	ErrGRPCUserAlreadyExist     = status.New(codes.FailedPrecondition, "etcdserver: 用户已存在").Err()
	ErrGRPCUserEmpty            = status.New(codes.InvalidArgument, "etcdserver: 用户名为空").Err()
	ErrGRPCUserNotFound         = status.New(codes.FailedPrecondition, "etcdserver: 用户没找到").Err()
	ErrGRPCRoleAlreadyExist     = status.New(codes.FailedPrecondition, "etcdserver: 角色已存在").Err()
	ErrGRPCRoleNotFound         = status.New(codes.FailedPrecondition, "etcdserver: 角色没找到").Err()
	ErrGRPCRoleEmpty            = status.New(codes.InvalidArgument, "etcdserver: role name is empty").Err()
	ErrGRPCAuthFailed           = status.New(codes.InvalidArgument, "etcdserver: authentication failed, invalid user ID or password").Err()
	ErrGRPCPermissionNotGiven   = status.New(codes.InvalidArgument, "etcdserver: permission not given").Err()
	ErrGRPCPermissionDenied     = status.New(codes.PermissionDenied, "etcdserver: permission denied").Err()
	ErrGRPCRoleNotGranted       = status.New(codes.FailedPrecondition, "etcdserver: role is not granted to the user").Err()
	ErrGRPCPermissionNotGranted = status.New(codes.FailedPrecondition, "etcdserver: permission is not granted to the role").Err()
	ErrGRPCAuthNotEnabled       = status.New(codes.FailedPrecondition, "etcdserver: authentication is not enabled").Err()
	ErrGRPCInvalidAuthToken     = status.New(codes.Unauthenticated, "etcdserver: invalid auth token").Err()
	ErrGRPCInvalidAuthMgmt      = status.New(codes.InvalidArgument, "etcdserver: invalid auth management").Err()
	ErrGRPCAuthOldRevision      = status.New(codes.InvalidArgument, "etcdserver: revision of auth store is old").Err()

	ErrGRPCNoLeader                   = status.New(codes.Unavailable, "etcdserver: 没有leader").Err()
	ErrGRPCNotLeader                  = status.New(codes.FailedPrecondition, "etcdserver: 不是leader").Err()
	ErrGRPCLeaderChanged              = status.New(codes.Unavailable, "etcdserver: leader改变了").Err()
	ErrGRPCNotCapable                 = status.New(codes.Unavailable, "etcdserver: 没有容量了").Err()
	ErrGRPCStopped                    = status.New(codes.Unavailable, "etcdserver: 服务已停止").Err()
	ErrGRPCTimeout                    = status.New(codes.Unavailable, "etcdserver: 请求超时").Err()
	ErrGRPCTimeoutDueToLeaderFail     = status.New(codes.Unavailable, "etcdserver: 请求超时,可能是之前的leader导致的").Err()
	ErrGRPCTimeoutDueToConnectionLost = status.New(codes.Unavailable, "etcdserver: 请求超时,可能是链接丢失").Err()
	ErrGRPCUnhealthy                  = status.New(codes.Unavailable, "etcdserver: 不健康的集群").Err()
	ErrGRPCCorrupt                    = status.New(codes.DataLoss, "etcdserver: 集群损坏").Err()
	ErrGPRCNotSupportedForLearner     = status.New(codes.Unavailable, "etcdserver: learner不支持rpc请求").Err()
	ErrGRPCBadLeaderTransferee        = status.New(codes.FailedPrecondition, "etcdserver: leader转移失败").Err()

	ErrGRPCClusterVersionUnavailable     = status.New(codes.Unavailable, "etcdserver: cluster version not found during downgrade").Err()
	ErrGRPCWrongDowngradeVersionFormat   = status.New(codes.InvalidArgument, "etcdserver: wrong downgrade target version format").Err()
	ErrGRPCInvalidDowngradeTargetVersion = status.New(codes.InvalidArgument, "etcdserver: invalid downgrade target version").Err()
	ErrGRPCDowngradeInProcess            = status.New(codes.FailedPrecondition, "etcdserver: cluster has a downgrade job in progress").Err()
	ErrGRPCNoInflightDowngrade           = status.New(codes.FailedPrecondition, "etcdserver: no inflight downgrade job").Err()

	ErrGRPCCanceled         = status.New(codes.Canceled, "etcdserver: 取消请求").Err()
	ErrGRPCDeadlineExceeded = status.New(codes.DeadlineExceeded, "etcdserver: 上下文超时").Err()

	errStringToError = map[string]error{
		ErrorDesc(ErrGRPCEmptyKey):      ErrGRPCEmptyKey,
		ErrorDesc(ErrGRPCKeyNotFound):   ErrGRPCKeyNotFound,
		ErrorDesc(ErrGRPCValueProvided): ErrGRPCValueProvided,
		ErrorDesc(ErrGRPCLeaseProvided): ErrGRPCLeaseProvided,

		ErrorDesc(ErrGRPCTooManyOps):   ErrGRPCTooManyOps,
		ErrorDesc(ErrGRPCDuplicateKey): ErrGRPCDuplicateKey,
		ErrorDesc(ErrGRPCCompacted):    ErrGRPCCompacted,
		ErrorDesc(ErrGRPCFutureRev):    ErrGRPCFutureRev,
		ErrorDesc(ErrGRPCNoSpace):      ErrGRPCNoSpace,

		ErrorDesc(ErrGRPCLeaseNotFound):    ErrGRPCLeaseNotFound,
		ErrorDesc(ErrGRPCLeaseExist):       ErrGRPCLeaseExist,
		ErrorDesc(ErrGRPCLeaseTTLTooLarge): ErrGRPCLeaseTTLTooLarge,

		ErrorDesc(ErrGRPCMemberExist):            ErrGRPCMemberExist,
		ErrorDesc(ErrGRPCPeerURLExist):           ErrGRPCPeerURLExist,
		ErrorDesc(ErrGRPCMemberNotEnoughStarted): ErrGRPCMemberNotEnoughStarted,
		ErrorDesc(ErrGRPCMemberBadURLs):          ErrGRPCMemberBadURLs,
		ErrorDesc(ErrGRPCMemberNotFound):         ErrGRPCMemberNotFound,
		ErrorDesc(ErrGRPCMemberNotLearner):       ErrGRPCMemberNotLearner,
		ErrorDesc(ErrGRPCLearnerNotReady):        ErrGRPCLearnerNotReady,
		ErrorDesc(ErrGRPCTooManyLearners):        ErrGRPCTooManyLearners,

		ErrorDesc(ErrGRPCRequestTooLarge):        ErrGRPCRequestTooLarge,
		ErrorDesc(ErrGRPCRequestTooManyRequests): ErrGRPCRequestTooManyRequests,

		ErrorDesc(ErrGRPCRootUserNotExist):     ErrGRPCRootUserNotExist,
		ErrorDesc(ErrGRPCRootRoleNotExist):     ErrGRPCRootRoleNotExist,
		ErrorDesc(ErrGRPCUserAlreadyExist):     ErrGRPCUserAlreadyExist,
		ErrorDesc(ErrGRPCUserEmpty):            ErrGRPCUserEmpty,
		ErrorDesc(ErrGRPCUserNotFound):         ErrGRPCUserNotFound,
		ErrorDesc(ErrGRPCRoleAlreadyExist):     ErrGRPCRoleAlreadyExist,
		ErrorDesc(ErrGRPCRoleNotFound):         ErrGRPCRoleNotFound,
		ErrorDesc(ErrGRPCRoleEmpty):            ErrGRPCRoleEmpty,
		ErrorDesc(ErrGRPCAuthFailed):           ErrGRPCAuthFailed,
		ErrorDesc(ErrGRPCPermissionDenied):     ErrGRPCPermissionDenied,
		ErrorDesc(ErrGRPCRoleNotGranted):       ErrGRPCRoleNotGranted,
		ErrorDesc(ErrGRPCPermissionNotGranted): ErrGRPCPermissionNotGranted,
		ErrorDesc(ErrGRPCAuthNotEnabled):       ErrGRPCAuthNotEnabled,
		ErrorDesc(ErrGRPCInvalidAuthToken):     ErrGRPCInvalidAuthToken,
		ErrorDesc(ErrGRPCInvalidAuthMgmt):      ErrGRPCInvalidAuthMgmt,
		ErrorDesc(ErrGRPCAuthOldRevision):      ErrGRPCAuthOldRevision,

		ErrorDesc(ErrGRPCNoLeader):                   ErrGRPCNoLeader,
		ErrorDesc(ErrGRPCNotLeader):                  ErrGRPCNotLeader,
		ErrorDesc(ErrGRPCLeaderChanged):              ErrGRPCLeaderChanged,
		ErrorDesc(ErrGRPCNotCapable):                 ErrGRPCNotCapable,
		ErrorDesc(ErrGRPCStopped):                    ErrGRPCStopped,
		ErrorDesc(ErrGRPCTimeout):                    ErrGRPCTimeout,
		ErrorDesc(ErrGRPCTimeoutDueToLeaderFail):     ErrGRPCTimeoutDueToLeaderFail,
		ErrorDesc(ErrGRPCTimeoutDueToConnectionLost): ErrGRPCTimeoutDueToConnectionLost,
		ErrorDesc(ErrGRPCUnhealthy):                  ErrGRPCUnhealthy,
		ErrorDesc(ErrGRPCCorrupt):                    ErrGRPCCorrupt,
		ErrorDesc(ErrGPRCNotSupportedForLearner):     ErrGPRCNotSupportedForLearner,
		ErrorDesc(ErrGRPCBadLeaderTransferee):        ErrGRPCBadLeaderTransferee,

		ErrorDesc(ErrGRPCClusterVersionUnavailable):     ErrGRPCClusterVersionUnavailable,
		ErrorDesc(ErrGRPCWrongDowngradeVersionFormat):   ErrGRPCWrongDowngradeVersionFormat,
		ErrorDesc(ErrGRPCInvalidDowngradeTargetVersion): ErrGRPCInvalidDowngradeTargetVersion,
		ErrorDesc(ErrGRPCDowngradeInProcess):            ErrGRPCDowngradeInProcess,
		ErrorDesc(ErrGRPCNoInflightDowngrade):           ErrGRPCNoInflightDowngrade,
	}
)

// client-side error
var (
	ErrEmptyKey  = Error(ErrGRPCEmptyKey)
	ErrCompacted = Error(ErrGRPCCompacted)
	ErrFutureRev = Error(ErrGRPCFutureRev)

	ErrLeaseNotFound = Error(ErrGRPCLeaseNotFound)

	ErrMemberNotEnoughStarted = Error(ErrGRPCMemberNotEnoughStarted)

	ErrTooManyRequests = Error(ErrGRPCRequestTooManyRequests)

	ErrRootRoleNotExist = Error(ErrGRPCRootRoleNotExist)
	ErrUserEmpty        = Error(ErrGRPCUserEmpty)
	ErrPermissionDenied = Error(ErrGRPCPermissionDenied)
	ErrAuthNotEnabled   = Error(ErrGRPCAuthNotEnabled)
	ErrInvalidAuthToken = Error(ErrGRPCInvalidAuthToken)
	ErrAuthOldRevision  = Error(ErrGRPCAuthOldRevision)

	ErrNoLeader = Error(ErrGRPCNoLeader)
)

// EtcdError defines gRPC server errors.
// (https://github.com/grpc/grpc-go/blob/master/rpc_util.go#L319-L323)
type EtcdError struct {
	code codes.Code
	desc string
}

// Code returns grpc/codes.Code.
// TODO: define clientv3/codes.Code.
func (e EtcdError) Code() codes.Code {
	return e.code
}

func (e EtcdError) Error() string {
	return e.desc
}

func Error(err error) error {
	if err == nil {
		return nil
	}
	verr, ok := errStringToError[ErrorDesc(err)]
	if !ok { // not gRPC error
		return err
	}
	ev, ok := status.FromError(verr)
	var desc string
	if ok {
		desc = ev.Message()
	} else {
		desc = verr.Error()
	}
	return EtcdError{code: ev.Code(), desc: desc}
}

func ErrorDesc(err error) string {
	if s, ok := status.FromError(err); ok {
		return s.Message()
	}
	return err.Error()
}
