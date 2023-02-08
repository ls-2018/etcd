package etcdserverpb

import (
	context "context"
	"encoding/json"
	fmt "fmt"
	math "math"
	math_bits "math/bits"

	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/golang/protobuf/proto"
	authpb "github.com/ls-2018/etcd_cn/offical/api/v3/authpb"
	mvccpb "github.com/ls-2018/etcd_cn/offical/api/v3/mvccpb"
	_ "google.golang.org/genproto/googleapis/api/annotations"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal

var (
	_ = fmt.Errorf
	_ = math.Inf
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type AlarmType int32

const (
	AlarmType_NONE    AlarmType = 0
	AlarmType_NOSPACE AlarmType = 1
	AlarmType_CORRUPT AlarmType = 2
)

var AlarmType_name = map[int32]string{
	0: "NONE",
	1: "NOSPACE",
	2: "CORRUPT",
}

var AlarmType_value = map[string]int32{
	"NONE":    0,
	"NOSPACE": 1,
	"CORRUPT": 2,
}

func (x AlarmType) String() string {
	return proto.EnumName(AlarmType_name, int32(x))
}

func (AlarmType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{0}
}

type RangeRequest_SortOrder int32

const (
	RangeRequest_NONE    RangeRequest_SortOrder = 0 // 不排序
	RangeRequest_ASCEND  RangeRequest_SortOrder = 1 // 升序
	RangeRequest_DESCEND RangeRequest_SortOrder = 2 // 降序
)

var RangeRequest_SortOrder_name = map[int32]string{
	0: "NONE",
	1: "ASCEND",
	2: "DESCEND",
}

var RangeRequest_SortOrder_value = map[string]int32{
	"NONE":    0,
	"ASCEND":  1,
	"DESCEND": 2,
}

func (x RangeRequest_SortOrder) String() string {
	return proto.EnumName(RangeRequest_SortOrder_name, int32(x))
}

func (RangeRequest_SortOrder) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{1, 0}
}

type RangeRequest_SortTarget int32

const (
	RangeRequest_KEY     RangeRequest_SortTarget = 0
	RangeRequest_VERSION RangeRequest_SortTarget = 1
	RangeRequest_CREATE  RangeRequest_SortTarget = 2
	RangeRequest_MOD     RangeRequest_SortTarget = 3
	RangeRequest_VALUE   RangeRequest_SortTarget = 4
)

var RangeRequest_SortTarget_name = map[int32]string{
	0: "KEY",
	1: "VERSION",
	2: "CREATE",
	3: "MOD",
	4: "VALUE",
}

var RangeRequest_SortTarget_value = map[string]int32{
	"KEY":     0,
	"VERSION": 1,
	"CREATE":  2,
	"MOD":     3,
	"VALUE":   4,
}

func (x RangeRequest_SortTarget) String() string {
	return proto.EnumName(RangeRequest_SortTarget_name, int32(x))
}

func (RangeRequest_SortTarget) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{1, 1}
}

type Compare_CompareResult int32

const (
	Compare_EQUAL     Compare_CompareResult = 0
	Compare_GREATER   Compare_CompareResult = 1
	Compare_LESS      Compare_CompareResult = 2
	Compare_NOT_EQUAL Compare_CompareResult = 3
)

var Compare_CompareResult_name = map[int32]string{
	0: "EQUAL",
	1: "GREATER",
	2: "LESS",
	3: "NOT_EQUAL",
}

var Compare_CompareResult_value = map[string]int32{
	"EQUAL":     0,
	"GREATER":   1,
	"LESS":      2,
	"NOT_EQUAL": 3,
}

func (x Compare_CompareResult) String() string {
	return proto.EnumName(Compare_CompareResult_name, int32(x))
}

func (Compare_CompareResult) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{9, 0}
}

type Compare_CompareTarget int32

const (
	Compare_VERSION Compare_CompareTarget = 0
	Compare_CREATE  Compare_CompareTarget = 1
	Compare_MOD     Compare_CompareTarget = 2
	Compare_VALUE   Compare_CompareTarget = 3
	Compare_LEASE   Compare_CompareTarget = 4
)

var Compare_CompareTarget_name = map[int32]string{
	0: "VERSION",
	1: "CREATE",
	2: "MOD",
	3: "VALUE",
	4: "LEASE",
}

var Compare_CompareTarget_value = map[string]int32{
	"VERSION": 0,
	"CREATE":  1,
	"MOD":     2,
	"VALUE":   3,
	"LEASE":   4,
}

func (x Compare_CompareTarget) String() string {
	return proto.EnumName(Compare_CompareTarget_name, int32(x))
}

func (Compare_CompareTarget) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{9, 1}
}

type WatchCreateRequest_FilterType int32

const (
	// filter out put event.
	WatchCreateRequest_NOPUT WatchCreateRequest_FilterType = 0
	// filter out delete event.
	WatchCreateRequest_NODELETE WatchCreateRequest_FilterType = 1
)

var WatchCreateRequest_FilterType_name = map[int32]string{
	0: "NOPUT",
	1: "NODELETE",
}

var WatchCreateRequest_FilterType_value = map[string]int32{
	"NOPUT":    0,
	"NODELETE": 1,
}

func (x WatchCreateRequest_FilterType) String() string {
	return proto.EnumName(WatchCreateRequest_FilterType_name, int32(x))
}

func (WatchCreateRequest_FilterType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{21, 0}
}

type AlarmRequest_AlarmAction int32

const (
	AlarmRequest_GET        AlarmRequest_AlarmAction = 0
	AlarmRequest_ACTIVATE   AlarmRequest_AlarmAction = 1 // 激活
	AlarmRequest_DEACTIVATE AlarmRequest_AlarmAction = 2 // 取消
)

var AlarmRequest_AlarmAction_name = map[int32]string{
	0: "GET",
	1: "ACTIVATE",
	2: "DEACTIVATE",
}

var AlarmRequest_AlarmAction_value = map[string]int32{
	"GET":        0,
	"ACTIVATE":   1,
	"DEACTIVATE": 2,
}

func (x AlarmRequest_AlarmAction) String() string {
	return proto.EnumName(AlarmRequest_AlarmAction_name, int32(x))
}

func (AlarmRequest_AlarmAction) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{54, 0}
}

type DowngradeRequest_DowngradeAction int32

const (
	DowngradeRequest_VALIDATE DowngradeRequest_DowngradeAction = 0
	DowngradeRequest_ENABLE   DowngradeRequest_DowngradeAction = 1
	DowngradeRequest_CANCEL   DowngradeRequest_DowngradeAction = 2
)

var DowngradeRequest_DowngradeAction_name = map[int32]string{
	0: "VALIDATE",
	1: "ENABLE",
	2: "CANCEL",
}

var DowngradeRequest_DowngradeAction_value = map[string]int32{
	"VALIDATE": 0,
	"ENABLE":   1,
	"CANCEL":   2,
}

func (x DowngradeRequest_DowngradeAction) String() string {
	return proto.EnumName(DowngradeRequest_DowngradeAction_name, int32(x))
}

func (DowngradeRequest_DowngradeAction) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{57, 0}
}

type ResponseHeader struct {
	// cluster_id is the ID of the cluster which sent the response.
	ClusterId uint64 `protobuf:"varint,1,opt,name=cluster_id,json=clusterId,proto3" json:"cluster_id,omitempty"`
	MemberId  uint64 `protobuf:"varint,2,opt,name=member_id,json=memberId,proto3" json:"member_id,omitempty"`
	Revision  int64  // 当前 watchResponse 实例创建时对应的 revision 值
	// raft_term is the raft term when the request was applied.
	RaftTerm uint64 `protobuf:"varint,4,opt,name=raft_term,json=raftTerm,proto3" json:"raft_term,omitempty"`
}

func (m *ResponseHeader) Reset()         { *m = ResponseHeader{} }
func (m *ResponseHeader) String() string { return proto.CompactTextString(m) }
func (*ResponseHeader) ProtoMessage()    {}
func (*ResponseHeader) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{0}
}

func (m *ResponseHeader) GetClusterId() uint64 {
	if m != nil {
		return m.ClusterId
	}
	return 0
}

func (m *ResponseHeader) GetMemberId() uint64 {
	if m != nil {
		return m.MemberId
	}
	return 0
}

func (m *ResponseHeader) GetRevision() int64 {
	if m != nil {
		return m.Revision
	}
	return 0
}

func (m *ResponseHeader) GetRaftTerm() uint64 {
	if m != nil {
		return m.RaftTerm
	}
	return 0
}

type RangeRequest struct {
	// key is the first key for the range. If range_end is not given, the request only looks up key.
	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// range_end is the upper bound on the requested range [key, range_end).
	// If range_end is '\0', the range is all keys >= key.
	// If range_end is key plus one (e.g., "aa"+1 == "ab", "a\xff"+1 == "b"),
	// then the range request gets all keys prefixed with key.
	// If both key and range_end are '\0', then the range request returns all keys.
	RangeEnd string `protobuf:"bytes,2,opt,name=range_end,json=rangeEnd,proto3" json:"range_end,omitempty"`
	// limit is a limit on the number of keys returned for the request. When limit is set to 0,
	// it is treated as no limit.
	Limit int64 `protobuf:"varint,3,opt,name=limit,proto3" json:"limit,omitempty"`
	// revision is the point-in-time of the key-value store to use for the range.
	// If revision is less or equal to zero, the range is over the newest key-value store.
	// If the revision has been compacted, ErrCompacted is returned as a response.
	Revision int64 `protobuf:"varint,4,opt,name=revision,proto3" json:"revision,omitempty"`
	// 返回的数据 排序方式
	SortOrder RangeRequest_SortOrder `protobuf:"varint,5,opt,name=sort_order,json=sortOrder,proto3,enum=etcdserverpb.RangeRequest_SortOrder" json:"sort_order,omitempty"`
	// sort_target is the key-value field to use for sorting.
	SortTarget RangeRequest_SortTarget `protobuf:"varint,6,opt,name=sort_target,json=sortTarget,proto3,enum=etcdserverpb.RangeRequest_SortTarget" json:"sort_target,omitempty"`
	// 表示设置range请求通过可串行化（ serializable)的方式从接受请求的节点读取本地数据.默认情况下, range 请求是可线性化的,它反映了当前集群的一致性.为了获得更好的性能和可用
	// 性,可以考虑使用可串行化的读,以有一定的概率读到过期数据为代价,不需要经过一致性协议与集群中其他节点的协同,而是直接从本地节点读数据.
	Serializable bool `protobuf:"varint,7,opt,name=serializable,proto3" json:"serializable,omitempty"`
	KeysOnly     bool `protobuf:"varint,8,opt,name=keys_only,json=keysOnly,proto3" json:"keys_only,omitempty"`    // 表示是否只返回key 而不返回value
	CountOnly    bool `protobuf:"varint,9,opt,name=count_only,json=countOnly,proto3" json:"count_only,omitempty"` // ,表示是否只返回range 请求返回的key 的数量.
	// min_mod_revision is the lower bound for returned key mod revisions; all keys with
	// lesser mod revisions will be filtered away.
	MinModRevision int64 `protobuf:"varint,10,opt,name=min_mod_revision,json=minModRevision,proto3" json:"min_mod_revision,omitempty"`
	// max_mod_revision is the upper bound for returned key mod revisions; all keys with
	// greater mod revisions will be filtered away.
	MaxModRevision int64 `protobuf:"varint,11,opt,name=max_mod_revision,json=maxModRevision,proto3" json:"max_mod_revision,omitempty"`
	// min_create_revision is the lower bound for returned key create revisions; all keys with
	// lesser create revisions will be filtered away.
	MinCreateRevision int64 `protobuf:"varint,12,opt,name=min_create_revision,json=minCreateRevision,proto3" json:"min_create_revision,omitempty"`
	// max_create_revision is the upper bound for returned key create revisions; all keys with
	// greater create revisions will be filtered away.
	MaxCreateRevision int64 `protobuf:"varint,13,opt,name=max_create_revision,json=maxCreateRevision,proto3" json:"max_create_revision,omitempty"`
}

func (m *RangeRequest) Reset()         { *m = RangeRequest{} }
func (m *RangeRequest) String() string { return proto.CompactTextString(m) }
func (*RangeRequest) ProtoMessage()    {}
func (*RangeRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{1}
}

func (m *RangeRequest) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *RangeRequest) GetRangeEnd() string {
	if m != nil {
		return m.RangeEnd
	}
	return ""
}

func (m *RangeRequest) GetLimit() int64 {
	if m != nil {
		return m.Limit
	}
	return 0
}

func (m *RangeRequest) GetRevision() int64 {
	if m != nil {
		return m.Revision
	}
	return 0
}

func (m *RangeRequest) GetSortOrder() RangeRequest_SortOrder {
	if m != nil {
		return m.SortOrder
	}
	return RangeRequest_NONE
}

func (m *RangeRequest) GetSortTarget() RangeRequest_SortTarget {
	if m != nil {
		return m.SortTarget
	}
	return RangeRequest_KEY
}

func (m *RangeRequest) GetSerializable() bool {
	if m != nil {
		return m.Serializable
	}
	return false
}

func (m *RangeRequest) GetKeysOnly() bool {
	if m != nil {
		return m.KeysOnly
	}
	return false
}

func (m *RangeRequest) GetCountOnly() bool {
	if m != nil {
		return m.CountOnly
	}
	return false
}

func (m *RangeRequest) GetMinModRevision() int64 {
	if m != nil {
		return m.MinModRevision
	}
	return 0
}

func (m *RangeRequest) GetMaxModRevision() int64 {
	if m != nil {
		return m.MaxModRevision
	}
	return 0
}

func (m *RangeRequest) GetMinCreateRevision() int64 {
	if m != nil {
		return m.MinCreateRevision
	}
	return 0
}

func (m *RangeRequest) GetMaxCreateRevision() int64 {
	if m != nil {
		return m.MaxCreateRevision
	}
	return 0
}

type RangeResponse struct {
	Header *ResponseHeader    `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	Kvs    []*mvccpb.KeyValue `protobuf:"bytes,2,rep,name=kvs,proto3" json:"kvs,omitempty"`      // 表示符合range 请求的key-value 对列表.如果Count_Only 设置为true ,则kvs 就为空.
	More   bool               `protobuf:"varint,3,opt,name=more,proto3" json:"more,omitempty"`   // 是否还有更多的数据,没有返回
	Count  int64              `protobuf:"varint,4,opt,name=count,proto3" json:"count,omitempty"` // 符合条件的总数
}

func (m *RangeResponse) Reset()         { *m = RangeResponse{} }
func (m *RangeResponse) String() string { return proto.CompactTextString(m) }
func (*RangeResponse) ProtoMessage()    {}
func (*RangeResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{2}
}

func (m *RangeResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *RangeResponse) GetKvs() []*mvccpb.KeyValue {
	if m != nil {
		return m.Kvs
	}
	return nil
}

func (m *RangeResponse) GetMore() bool {
	if m != nil {
		return m.More
	}
	return false
}

func (m *RangeResponse) GetCount() int64 {
	if m != nil {
		return m.Count
	}
	return 0
}

type PutRequest struct {
	// key is the key, in bytes, to put into the key-value store.
	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// value is the value, in bytes, to associate with the key in the key-value store.
	Value string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	// lease is the lease ID to associate with the key in the key-value store. A lease
	// value of 0 indicates no lease.
	Lease  int64 `protobuf:"varint,3,opt,name=lease,proto3" json:"lease,omitempty"`
	PrevKv bool  `protobuf:"varint,4,opt,name=prev_kv,json=prevKv,proto3" json:"prev_kv,omitempty"` // 是否设置了前缀
	// If ignore_value is set, etcd updates the key using its current value.
	// Returns an error if the key does not exist.
	IgnoreValue bool `protobuf:"varint,5,opt,name=ignore_value,json=ignoreValue,proto3" json:"ignore_value,omitempty"`
	// If ignore_lease is set, etcd updates the key using its current lease.
	// Returns an error if the key does not exist.
	IgnoreLease bool `protobuf:"varint,6,opt,name=ignore_lease,json=ignoreLease,proto3" json:"ignore_lease,omitempty"`
}

func (m *PutRequest) Reset()         { *m = PutRequest{} }
func (m *PutRequest) String() string { return proto.CompactTextString(m) }
func (*PutRequest) ProtoMessage()    {}
func (*PutRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{3}
}

func (m *PutRequest) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *PutRequest) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

func (m *PutRequest) GetLease() int64 {
	if m != nil {
		return m.Lease
	}
	return 0
}

func (m *PutRequest) GetPrevKv() bool {
	if m != nil {
		return m.PrevKv
	}
	return false
}

func (m *PutRequest) GetIgnoreValue() bool {
	if m != nil {
		return m.IgnoreValue
	}
	return false
}

func (m *PutRequest) GetIgnoreLease() bool {
	if m != nil {
		return m.IgnoreLease
	}
	return false
}

type PutResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// if prev_kv is set in the request, the previous key-value pair will be returned.
	PrevKv               *mvccpb.KeyValue `protobuf:"bytes,2,opt,name=prev_kv,json=prevKv,proto3" json:"prev_kv,omitempty"`
	XXX_NoUnkeyedLiteral struct{}         `json:"-"`
	XXX_unrecognized     []byte           `json:"-"`
	XXX_sizecache        int32            `json:"-"`
}

func (m *PutResponse) Reset()         { *m = PutResponse{} }
func (m *PutResponse) String() string { return proto.CompactTextString(m) }
func (*PutResponse) ProtoMessage()    {}
func (*PutResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{4}
}

func (m *PutResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *PutResponse) GetPrevKv() *mvccpb.KeyValue {
	if m != nil {
		return m.PrevKv
	}
	return nil
}

type DeleteRangeRequest struct {
	// [key,RangeEnd]
	Key      string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	RangeEnd string `protobuf:"bytes,2,opt,name=range_end,json=rangeEnd,proto3" json:"range_end,omitempty"`
	// 如果设置了prev_kv,etcd在删除前会获取之前的键值对.先前的键值对将在删除响应中返回.
	PrevKv bool `protobuf:"varint,3,opt,name=prev_kv,json=prevKv,proto3" json:"prev_kv,omitempty"`
}

func (m *DeleteRangeRequest) Reset()         { *m = DeleteRangeRequest{} }
func (m *DeleteRangeRequest) String() string { return proto.CompactTextString(m) }
func (*DeleteRangeRequest) ProtoMessage()    {}
func (*DeleteRangeRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{5}
}

func (m *DeleteRangeRequest) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *DeleteRangeRequest) GetRangeEnd() string {
	if m != nil {
		return m.RangeEnd
	}
	return ""
}

func (m *DeleteRangeRequest) GetPrevKv() bool {
	if m != nil {
		return m.PrevKv
	}
	return false
}

type DeleteRangeResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// deleted is the number of keys deleted by the delete range request.
	Deleted int64 `protobuf:"varint,2,opt,name=deleted,proto3" json:"deleted,omitempty"`
	// if prev_kv is set in the request, the previous key-value pairs will be returned.
	PrevKvs              []*mvccpb.KeyValue `protobuf:"bytes,3,rep,name=prev_kvs,json=prevKvs,proto3" json:"prev_kvs,omitempty"`
	XXX_NoUnkeyedLiteral struct{}           `json:"-"`
	XXX_unrecognized     []byte             `json:"-"`
	XXX_sizecache        int32              `json:"-"`
}

func (m *DeleteRangeResponse) Reset()         { *m = DeleteRangeResponse{} }
func (m *DeleteRangeResponse) String() string { return proto.CompactTextString(m) }
func (*DeleteRangeResponse) ProtoMessage()    {}
func (*DeleteRangeResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{6}
}

func (m *DeleteRangeResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *DeleteRangeResponse) GetDeleted() int64 {
	if m != nil {
		return m.Deleted
	}
	return 0
}

func (m *DeleteRangeResponse) GetPrevKvs() []*mvccpb.KeyValue {
	if m != nil {
		return m.PrevKvs
	}
	return nil
}

type RequestOp struct {
	// request is a union of request types accepted by a transaction.
	//
	// Types that are valid to be assigned to Request:
	RequestOp_RequestRange       *RequestOp_RequestRange
	RequestOp_RequestPut         *RequestOp_RequestPut
	RequestOp_RequestDeleteRange *RequestOp_RequestDeleteRange
	RequestOp_RequestTxn         *RequestOp_RequestTxn
	XXX_NoUnkeyedLiteral         struct{} `json:"-"`
	XXX_unrecognized             []byte   `json:"-"`
	XXX_sizecache                int32    `json:"-"`
}

func (m *RequestOp) Reset()         { *m = RequestOp{} }
func (m *RequestOp) String() string { return proto.CompactTextString(m) }
func (*RequestOp) ProtoMessage()    {}
func (*RequestOp) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{7}
}

type isRequestOp_Request interface {
	isRequestOp_Request()
	Size() int
}

type RequestOp_RequestRange struct {
	RequestRange *RangeRequest `protobuf:"bytes,1,opt,name=request_range,json=requestRange,proto3,oneof" json:"request_range,omitempty"`
}
type RequestOp_RequestPut struct {
	RequestPut *PutRequest `protobuf:"bytes,2,opt,name=request_put,json=requestPut,proto3,oneof" json:"request_put,omitempty"`
}
type RequestOp_RequestDeleteRange struct {
	RequestDeleteRange *DeleteRangeRequest `protobuf:"bytes,3,opt,name=request_delete_range,json=requestDeleteRange,proto3,oneof" json:"request_delete_range,omitempty"`
}
type RequestOp_RequestTxn struct {
	RequestTxn *TxnRequest `protobuf:"bytes,4,opt,name=request_txn,json=requestTxn,proto3,oneof" json:"request_txn,omitempty"`
}

func (*RequestOp_RequestRange) isRequestOp_Request()       {}
func (*RequestOp_RequestPut) isRequestOp_Request()         {}
func (*RequestOp_RequestDeleteRange) isRequestOp_Request() {}
func (*RequestOp_RequestTxn) isRequestOp_Request()         {}

func (m *RequestOp) GetRequestRange() *RangeRequest {
	if m.RequestOp_RequestRange != nil {
		return m.RequestOp_RequestRange.RequestRange
	}
	return nil
}

func (m *RequestOp) GetRequestPut() *PutRequest {
	if m.RequestOp_RequestPut != nil {
		return m.RequestOp_RequestPut.RequestPut
	}
	return nil
}

func (m *RequestOp) GetRequestDeleteRange() *DeleteRangeRequest {
	if m.RequestOp_RequestDeleteRange != nil {
		return m.RequestOp_RequestDeleteRange.RequestDeleteRange
	}
	return nil
}

func (m *RequestOp) GetRequestTxn() *TxnRequest {
	if m.RequestOp_RequestTxn != nil {
		return m.RequestOp_RequestTxn.RequestTxn
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*RequestOp) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*RequestOp_RequestRange)(nil),
		(*RequestOp_RequestPut)(nil),
		(*RequestOp_RequestDeleteRange)(nil),
		(*RequestOp_RequestTxn)(nil),
	}
}

type ResponseOp struct {
	// response is a union of response types returned by a transaction.
	//
	// Types that are valid to be assigned to Response:
	//	*ResponseOp_ResponseRange
	//	*ResponseOp_ResponsePut
	//	*ResponseOp_ResponseDeleteRange
	//	*ResponseOp_ResponseTxn
	ResponseOp_ResponseRange       *ResponseOp_ResponseRange
	ResponseOp_ResponsePut         *ResponseOp_ResponsePut
	ResponseOp_ResponseDeleteRange *ResponseOp_ResponseDeleteRange
	ResponseOp_ResponseTxn         *ResponseOp_ResponseTxn
	// Response             isResponseOp_Response `protobuf_oneof:"response"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ResponseOp) Reset()         { *m = ResponseOp{} }
func (m *ResponseOp) String() string { return proto.CompactTextString(m) }
func (*ResponseOp) ProtoMessage()    {}
func (*ResponseOp) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{8}
}

type isResponseOp_Response interface {
	isResponseOp_Response()
	Size() int
}

type ResponseOp_ResponseRange struct {
	ResponseRange *RangeResponse `protobuf:"bytes,1,opt,name=response_range,json=responseRange,proto3,oneof" json:"response_range,omitempty"`
}
type ResponseOp_ResponsePut struct {
	ResponsePut *PutResponse `protobuf:"bytes,2,opt,name=response_put,json=responsePut,proto3,oneof" json:"response_put,omitempty"`
}
type ResponseOp_ResponseDeleteRange struct {
	ResponseDeleteRange *DeleteRangeResponse `protobuf:"bytes,3,opt,name=response_delete_range,json=responseDeleteRange,proto3,oneof" json:"response_delete_range,omitempty"`
}
type ResponseOp_ResponseTxn struct {
	ResponseTxn *TxnResponse `protobuf:"bytes,4,opt,name=response_txn,json=responseTxn,proto3,oneof" json:"response_txn,omitempty"`
}

func (*ResponseOp_ResponseRange) isResponseOp_Response()       {}
func (*ResponseOp_ResponsePut) isResponseOp_Response()         {}
func (*ResponseOp_ResponseDeleteRange) isResponseOp_Response() {}
func (*ResponseOp_ResponseTxn) isResponseOp_Response()         {}

func (m *ResponseOp) GetResponseRange() *RangeResponse {
	if m.ResponseOp_ResponseRange != nil {
		return m.ResponseOp_ResponseRange.ResponseRange
	}
	return nil
}

func (m *ResponseOp) GetResponsePut() *PutResponse {
	if m.ResponseOp_ResponsePut != nil {
		return m.ResponseOp_ResponsePut.ResponsePut
	}
	return nil
}

func (m *ResponseOp) GetResponseDeleteRange() *DeleteRangeResponse {
	if m.ResponseOp_ResponseDeleteRange != nil {
		return m.ResponseOp_ResponseDeleteRange.ResponseDeleteRange
	}
	return nil
}

func (m *ResponseOp) GetResponseTxn() *TxnResponse {
	if m.ResponseOp_ResponseTxn != nil {
		return m.ResponseOp_ResponseTxn.ResponseTxn
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*ResponseOp) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*ResponseOp_ResponseRange)(nil),
		(*ResponseOp_ResponsePut)(nil),
		(*ResponseOp_ResponseDeleteRange)(nil),
		(*ResponseOp_ResponseTxn)(nil),
	}
}

type Compare struct {
	Result Compare_CompareResult `protobuf:"varint,1,opt,name=result,proto3,enum=etcdserverpb.Compare_CompareResult" json:"result,omitempty"`
	Target Compare_CompareTarget `protobuf:"varint,2,opt,name=target,proto3,enum=etcdserverpb.Compare_CompareTarget" json:"target,omitempty"`
	Key    string                `protobuf:"bytes,3,opt,name=key,proto3" json:"key,omitempty"`
	// Types that are valid to be assigned to TargetUnion:
	//	*Compare_Version
	//	*Compare_CreateRevision
	//	*Compare_ModRevision
	//	*Compare_Value
	//	*Compare_Lease
	// TargetUnion isCompare_TargetUnion `protobuf_oneof:"target_union"`

	Compare_Value          *Compare_Value
	Compare_Version        *Compare_Version
	Compare_CreateRevision *Compare_CreateRevision
	Compare_ModRevision    *Compare_ModRevision
	Compare_Lease          *Compare_Lease

	// range_end compares the given target to all keys in the range [key, range_end).
	// See RangeRequest for more details on key ranges.
	RangeEnd string `protobuf:"bytes,64,opt,name=range_end,json=rangeEnd,proto3" json:"range_end,omitempty"`
}

func (m *Compare) Reset()         { *m = Compare{} }
func (m *Compare) String() string { return proto.CompactTextString(m) }
func (*Compare) ProtoMessage()    {}
func (*Compare) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{9}
}

type isCompare_TargetUnion interface {
	isCompare_TargetUnion()
	Size() int
}

type Compare_Version struct {
	Version int64 `protobuf:"varint,4,opt,name=version,proto3,oneof" json:"version,omitempty"`
}
type Compare_CreateRevision struct {
	CreateRevision int64 `protobuf:"varint,5,opt,name=create_revision,json=createRevision,proto3,oneof" json:"create_revision,omitempty"`
}
type Compare_ModRevision struct {
	ModRevision int64 `protobuf:"varint,6,opt,name=mod_revision,json=modRevision,proto3,oneof" json:"mod_revision,omitempty"`
}
type Compare_Value struct {
	Value string `protobuf:"bytes,7,opt,name=value,proto3,oneof" json:"value,omitempty"`
}
type Compare_Lease struct {
	Lease int64 `protobuf:"varint,8,opt,name=lease,proto3,oneof" json:"lease,omitempty"`
}

func (*Compare_Version) isCompare_TargetUnion()        {}
func (*Compare_CreateRevision) isCompare_TargetUnion() {}
func (*Compare_ModRevision) isCompare_TargetUnion()    {}
func (*Compare_Value) isCompare_TargetUnion()          {}
func (*Compare_Lease) isCompare_TargetUnion()          {}

func (m *Compare) GetResult() Compare_CompareResult {
	if m != nil {
		return m.Result
	}
	return Compare_EQUAL
}

func (m *Compare) GetTarget() Compare_CompareTarget {
	if m != nil {
		return m.Target
	}
	return Compare_VERSION
}

func (m *Compare) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *Compare) GetVersion() int64 {
	if m.Compare_Version != nil {
		return m.Compare_Version.Version
	}
	return 0
}

func (m *Compare) GetCreateRevision() int64 {
	if m.Compare_CreateRevision != nil {
		return m.Compare_CreateRevision.CreateRevision
	}
	return 0
}

func (m *Compare) GetModRevision() int64 {
	if m.Compare_ModRevision != nil {
		return m.Compare_ModRevision.ModRevision
	}
	return 0
}

func (m *Compare) GetValue() string {
	if m.Compare_Value != nil {
		return m.Compare_Value.Value
	}
	return ""
}

func (m *Compare) GetLease() int64 {
	if m.Compare_Lease != nil {
		return m.Compare_Lease.Lease
	}
	return 0
}

func (m *Compare) GetRangeEnd() string {
	if m != nil {
		return m.RangeEnd
	}
	return ""
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*Compare) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*Compare_Version)(nil),
		(*Compare_CreateRevision)(nil),
		(*Compare_ModRevision)(nil),
		(*Compare_Value)(nil),
		(*Compare_Lease)(nil),
	}
}

// From google paxosdb paper:
// Our implementation hinges around a powerful primitive which we call MultiOp. All other database
// operations except for iteration are implemented as a single call to MultiOp. A MultiOp is applied atomically
// and consists of three components:
// 1. A list of tests called guard. Each test in guard checks a single entry in the database. It may check
// for the absence or presence of a value, or compare with a given value. Two different tests in the guard
// may apply to the same or different entries in the database. All tests in the guard are applied and
// MultiOp returns the results. If all tests are true, MultiOp executes t op (see item 2 below), otherwise
// it executes f op (see item 3 below).
// 2. A list of database operations called t op. Each operation in the list is either an insert, delete, or
// lookup operation, and applies to a single database entry. Two different operations in the list may apply
// to the same or different entries in the database. These operations are executed
// if guard evaluates to
// true.
// 3. A list of database operations called f op. Like t op, but executed if guard evaluates to false.
type TxnRequest struct {
	// compare is a list of predicates representing a conjunction of terms.
	// If the comparisons succeed, then the success requests will be processed in order,
	// and the response will contain their respective responses in order.
	// If the comparisons fail, then the failure requests will be processed in order,
	// and the response will contain their respective responses in order.
	Compare []*Compare `protobuf:"bytes,1,rep,name=compare,proto3" json:"compare,omitempty"`
	// success is a list of requests which will be applied when compare evaluates to true.
	Success []*RequestOp `protobuf:"bytes,2,rep,name=success,proto3" json:"success,omitempty"`
	// failure is a list of requests which will be applied when compare evaluates to false.
	Failure              []*RequestOp `protobuf:"bytes,3,rep,name=failure,proto3" json:"failure,omitempty"`
	XXX_NoUnkeyedLiteral struct{}     `json:"-"`
	XXX_unrecognized     []byte       `json:"-"`
	XXX_sizecache        int32        `json:"-"`
}

func (m *TxnRequest) Reset()         { *m = TxnRequest{} }
func (m *TxnRequest) String() string { return proto.CompactTextString(m) }
func (*TxnRequest) ProtoMessage()    {}
func (*TxnRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{10}
}

func (m *TxnRequest) GetCompare() []*Compare {
	if m != nil {
		return m.Compare
	}
	return nil
}

func (m *TxnRequest) GetSuccess() []*RequestOp {
	if m != nil {
		return m.Success
	}
	return nil
}

func (m *TxnRequest) GetFailure() []*RequestOp {
	if m != nil {
		return m.Failure
	}
	return nil
}

type TxnResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// succeeded is set to true if the compare evaluated to true or false otherwise.
	Succeeded bool `protobuf:"varint,2,opt,name=succeeded,proto3" json:"succeeded,omitempty"`
	// responses is a list of responses corresponding to the results from applying
	// success if succeeded is true or failure if succeeded is false.
	Responses            []*ResponseOp `protobuf:"bytes,3,rep,name=responses,proto3" json:"responses,omitempty"`
	XXX_NoUnkeyedLiteral struct{}      `json:"-"`
	XXX_unrecognized     []byte        `json:"-"`
	XXX_sizecache        int32         `json:"-"`
}

func (m *TxnResponse) Reset()         { *m = TxnResponse{} }
func (m *TxnResponse) String() string { return proto.CompactTextString(m) }
func (*TxnResponse) ProtoMessage()    {}
func (*TxnResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{11}
}

func (m *TxnResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *TxnResponse) GetSucceeded() bool {
	if m != nil {
		return m.Succeeded
	}
	return false
}

func (m *TxnResponse) GetResponses() []*ResponseOp {
	if m != nil {
		return m.Responses
	}
	return nil
}

type CompactionRequest struct {
	//  Revision是用于压缩操作的键-值存储修订.
	Revision int64 `protobuf:"varint,1,opt,name=revision,proto3" json:"revision,omitempty"`
	//
	Physical bool `protobuf:"varint,2,opt,name=physical,proto3" json:"physical,omitempty"`
}

func (m *CompactionRequest) Reset()         { *m = CompactionRequest{} }
func (m *CompactionRequest) String() string { return proto.CompactTextString(m) }
func (*CompactionRequest) ProtoMessage()    {}
func (*CompactionRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{12}
}

func (m *CompactionRequest) GetRevision() int64 {
	if m != nil {
		return m.Revision
	}
	return 0
}

func (m *CompactionRequest) GetPhysical() bool {
	if m != nil {
		return m.Physical
	}
	return false
}

type CompactionResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *CompactionResponse) Reset()         { *m = CompactionResponse{} }
func (m *CompactionResponse) String() string { return proto.CompactTextString(m) }
func (*CompactionResponse) ProtoMessage()    {}
func (*CompactionResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{13}
}

func (m *CompactionResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

type HashRequest struct{}

func (m *HashRequest) Reset()         { *m = HashRequest{} }
func (m *HashRequest) String() string { return proto.CompactTextString(m) }
func (*HashRequest) ProtoMessage()    {}
func (*HashRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{14}
}

type HashKVRequest struct {
	// revision是哈希操作的键值存储修订版.
	Revision int64 `protobuf:"varint,1,opt,name=revision,proto3" json:"revision,omitempty"`
}

func (m *HashKVRequest) Reset()         { *m = HashKVRequest{} }
func (m *HashKVRequest) String() string { return proto.CompactTextString(m) }
func (*HashKVRequest) ProtoMessage()    {}
func (*HashKVRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{15}
}

func (m *HashKVRequest) GetRevision() int64 {
	if m != nil {
		return m.Revision
	}
	return 0
}

type HashKVResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// hash is the hash value computed from the responding member's MVCC keys up to a given revision.
	Hash uint32 `protobuf:"varint,2,opt,name=hash,proto3" json:"hash,omitempty"`
	// compact_revision is the compacted revision of key-value store when hash begins.
	CompactRevision int64 `protobuf:"varint,3,opt,name=compact_revision,json=compactRevision,proto3" json:"compact_revision,omitempty"`
}

func (m *HashKVResponse) Reset()         { *m = HashKVResponse{} }
func (m *HashKVResponse) String() string { return proto.CompactTextString(m) }
func (*HashKVResponse) ProtoMessage()    {}
func (*HashKVResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{16}
}

func (m *HashKVResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *HashKVResponse) GetHash() uint32 {
	if m != nil {
		return m.Hash
	}
	return 0
}

func (m *HashKVResponse) GetCompactRevision() int64 {
	if m != nil {
		return m.CompactRevision
	}
	return 0
}

type HashResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// hash is the hash value computed from the responding member's KV's backend.
	Hash uint32 `protobuf:"varint,2,opt,name=hash,proto3" json:"hash,omitempty"`
}

func (m *HashResponse) Reset()         { *m = HashResponse{} }
func (m *HashResponse) String() string { return proto.CompactTextString(m) }
func (*HashResponse) ProtoMessage()    {}
func (*HashResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{17}
}

func (m *HashResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *HashResponse) GetHash() uint32 {
	if m != nil {
		return m.Hash
	}
	return 0
}

type SnapshotRequest struct{}

func (m *SnapshotRequest) Reset()         { *m = SnapshotRequest{} }
func (m *SnapshotRequest) String() string { return proto.CompactTextString(m) }
func (*SnapshotRequest) ProtoMessage()    {}
func (*SnapshotRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{18}
}

type SnapshotResponse struct {
	// header has the current key-value store information. The first header in the snapshot
	// stream indicates the point in time of the snapshot.
	Header         *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	RemainingBytes uint64          `protobuf:"varint,2,opt,name=remaining_bytes,json=remainingBytes,proto3" json:"remaining_bytes,omitempty"` // 剩余数据量
	// blob contains the next chunk of the snapshot in the snapshot stream.
	Blob []byte `protobuf:"bytes,3,opt,name=blob,proto3" json:"blob,omitempty"`
}

func (m *SnapshotResponse) Reset()         { *m = SnapshotResponse{} }
func (m *SnapshotResponse) String() string { return proto.CompactTextString(m) }
func (*SnapshotResponse) ProtoMessage()    {}
func (*SnapshotResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{19}
}

func (m *SnapshotResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *SnapshotResponse) GetRemainingBytes() uint64 {
	if m != nil {
		return m.RemainingBytes
	}
	return 0
}

func (m *SnapshotResponse) GetBlob() []byte {
	if m != nil {
		return m.Blob
	}
	return nil
}

type WatchRequest struct {
	WatchRequest_CreateRequest   *WatchRequest_CreateRequest
	WatchRequest_CancelRequest   *WatchRequest_CancelRequest
	WatchRequest_ProgressRequest *WatchRequest_ProgressRequest
	XXX_NoUnkeyedLiteral         struct{} `json:"-"`
	XXX_unrecognized             []byte   `json:"-"`
	XXX_sizecache                int32    `json:"-"`
}

func (m *WatchRequest) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *WatchRequest) Unmarshal(dAtA []byte) error {
	err := json.Unmarshal(dAtA, m)
	return err
}

func (m *WatchRequest) Reset()         { *m = WatchRequest{} }
func (m *WatchRequest) String() string { return proto.CompactTextString(m) }
func (*WatchRequest) ProtoMessage()    {}
func (*WatchRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{20}
}

type WatchRequest_CreateRequest struct {
	CreateRequest *WatchCreateRequest `protobuf:"bytes,1,opt,name=create_request,json=createRequest,proto3,oneof" json:"create_request,omitempty"`
}
type WatchRequest_CancelRequest struct {
	CancelRequest *WatchCancelRequest `protobuf:"bytes,2,opt,name=cancel_request,json=cancelRequest,proto3,oneof" json:"cancel_request,omitempty"`
}
type WatchRequest_ProgressRequest struct {
	ProgressRequest *WatchProgressRequest `protobuf:"bytes,3,opt,name=progress_request,json=progressRequest,proto3,oneof" json:"progress_request,omitempty"`
}

func (*WatchRequest_CreateRequest) isWatchRequest_RequestUnion()   {}
func (*WatchRequest_CancelRequest) isWatchRequest_RequestUnion()   {}
func (*WatchRequest_ProgressRequest) isWatchRequest_RequestUnion() {}

func (m *WatchRequest) GetCreateRequest() *WatchCreateRequest {
	if m.WatchRequest_CreateRequest != nil {
		return m.WatchRequest_CreateRequest.CreateRequest
	}
	return nil
}

func (m *WatchRequest) GetCancelRequest() *WatchCancelRequest {
	if m.WatchRequest_CancelRequest != nil {
		return m.WatchRequest_CancelRequest.CancelRequest
	}
	return nil
}

func (m *WatchRequest) GetProgressRequest() *WatchProgressRequest {
	if m.WatchRequest_ProgressRequest != nil {
		return m.WatchRequest_ProgressRequest.ProgressRequest
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*WatchRequest) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*WatchRequest_CreateRequest)(nil),
		(*WatchRequest_CancelRequest)(nil),
		(*WatchRequest_ProgressRequest)(nil),
	}
}

type WatchCreateRequest struct {
	// key is the key to register for watching.
	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// range_end is the end of the range [key, range_end) to watch. If range_end is not given,
	// only the key argument is watched. If range_end is equal to '\0', all keys greater than
	// or equal to the key argument are watched.
	// If the range_end is one bit larger than the given key,
	// then all keys with the prefix (the given key) will be watched.
	RangeEnd      string `protobuf:"bytes,2,opt,name=range_end,json=rangeEnd,proto3" json:"range_end,omitempty"`
	StartRevision int64  `protobuf:"varint,3,opt,name=start_revision,json=startRevision,proto3" json:"start_revision,omitempty"`
	// progress_notify被设置为etcd服务器将定期发送WatchResponse如果没有最近的事件,它是有用的希望恢复断开的观察者从最近的已知修订开始.
	// etcd服务器可以根据当前的负载决定发送通知的频率.
	ProgressNotify bool `protobuf:"varint,4,opt,name=progress_notify,json=progressNotify,proto3" json:"progress_notify,omitempty"`
	// filters filter the events at server side before it sends back to the watcher.
	Filters []WatchCreateRequest_FilterType `protobuf:"varint,5,rep,packed,name=filters,proto3,enum=etcdserverpb.WatchCreateRequest_FilterType" json:"filters,omitempty"`
	// If prev_kv is set, created watcher gets the previous KV before the event happens.
	// If the previous KV is already compacted, nothing will be returned.
	PrevKv bool `protobuf:"varint,6,opt,name=prev_kv,json=prevKv,proto3" json:"prev_kv,omitempty"`
	// 如果提供了watch_id且非零,则将它分配给这个监视程序.因为在etcd中创建监视器不是同步操作,所以当在同一流中创建多个监视器时,
	// 可以使用它来确保顺序是正确的.在流上创建ID已在使用的监视程序将导致返回错误.
	WatchId int64 `protobuf:"varint,7,opt,name=watch_id,json=watchId,proto3" json:"watch_id,omitempty"`
	// 拆分大的变更 成多个watch响应.
	Fragment bool `protobuf:"varint,8,opt,name=fragment,proto3" json:"fragment,omitempty"`
}

func (m *WatchCreateRequest) Reset()         { *m = WatchCreateRequest{} }
func (m *WatchCreateRequest) String() string { return proto.CompactTextString(m) }
func (*WatchCreateRequest) ProtoMessage()    {}
func (*WatchCreateRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{21}
}

func (m *WatchCreateRequest) GetKey() []byte {
	if m != nil {
		return []byte(m.Key)
	}
	return nil
}

func (m *WatchCreateRequest) GetRangeEnd() []byte {
	if m != nil {
		return []byte(m.RangeEnd)
	}
	return nil
}

func (m *WatchCreateRequest) GetStartRevision() int64 {
	if m != nil {
		return m.StartRevision
	}
	return 0
}

func (m *WatchCreateRequest) GetProgressNotify() bool {
	if m != nil {
		return m.ProgressNotify
	}
	return false
}

func (m *WatchCreateRequest) GetFilters() []WatchCreateRequest_FilterType {
	if m != nil {
		return m.Filters
	}
	return nil
}

func (m *WatchCreateRequest) GetPrevKv() bool {
	if m != nil {
		return m.PrevKv
	}
	return false
}

func (m *WatchCreateRequest) GetWatchId() int64 {
	if m != nil {
		return m.WatchId
	}
	return 0
}

func (m *WatchCreateRequest) GetFragment() bool {
	if m != nil {
		return m.Fragment
	}
	return false
}

type WatchCancelRequest struct {
	// watch_id is the watcher id to cancel so that no more events are transmitted.
	WatchId int64 `protobuf:"varint,1,opt,name=watch_id,json=watchId,proto3" json:"watch_id,omitempty"`
}

func (m *WatchCancelRequest) Reset()         { *m = WatchCancelRequest{} }
func (m *WatchCancelRequest) String() string { return proto.CompactTextString(m) }
func (*WatchCancelRequest) ProtoMessage()    {}
func (*WatchCancelRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{22}
}

func (m *WatchCancelRequest) GetWatchId() int64 {
	if m != nil {
		return m.WatchId
	}
	return 0
}

// WatchProgressRequest 获取watch的状态
type WatchProgressRequest struct{}

func (m *WatchProgressRequest) Reset()         { *m = WatchProgressRequest{} }
func (m *WatchProgressRequest) String() string { return proto.CompactTextString(m) }
func (*WatchProgressRequest) ProtoMessage()    {}
func (*WatchProgressRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{23}
}

type WatchResponse struct {
	CompactRevision int64           // 压缩操作对应的 revison
	Header          *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	WatchId         int64           `protobuf:"varint,2,opt,name=watch_id,json=watchId,proto3" json:"watch_id,omitempty"`
	Created         bool            `protobuf:"varint,3,opt,name=created,proto3" json:"created,omitempty"`
	Canceled        bool            `protobuf:"varint,4,opt,name=canceled,proto3" json:"canceled,omitempty"`
	CancelReason    string          `protobuf:"bytes,6,opt,name=cancel_reason,json=cancelReason,proto3" json:"cancel_reason,omitempty"`
	// framgment is true if large watch response was split over multiple responses.
	Fragment             bool            `protobuf:"varint,7,opt,name=fragment,proto3" json:"fragment,omitempty"`
	Events               []*mvccpb.Event `protobuf:"bytes,11,rep,name=events,proto3" json:"events,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *WatchResponse) Reset()         { *m = WatchResponse{} }
func (m *WatchResponse) String() string { return proto.CompactTextString(m) }
func (*WatchResponse) ProtoMessage()    {}
func (*WatchResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{24}
}

func (m *WatchResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *WatchResponse) GetWatchId() int64 {
	if m != nil {
		return m.WatchId
	}
	return 0
}

func (m *WatchResponse) GetCreated() bool {
	if m != nil {
		return m.Created
	}
	return false
}

func (m *WatchResponse) GetCanceled() bool {
	if m != nil {
		return m.Canceled
	}
	return false
}

func (m *WatchResponse) GetCompactRevision() int64 {
	if m != nil {
		return m.CompactRevision
	}
	return 0
}

func (m *WatchResponse) GetCancelReason() string {
	if m != nil {
		return m.CancelReason
	}
	return ""
}

func (m *WatchResponse) GetFragment() bool {
	if m != nil {
		return m.Fragment
	}
	return false
}

func (m *WatchResponse) GetEvents() []*mvccpb.Event {
	if m != nil {
		return m.Events
	}
	return nil
}

type Member struct {
	// ID is the member ID for this member.
	ID uint64 `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
	// name is the human-readable name of the member. If the member is not started, the name will be an empty string.
	Name string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	// peerURLs is the list of URLs the member exposes to the cluster for communication.
	PeerURLs []string `protobuf:"bytes,3,rep,name=peerURLs,proto3" json:"peerURLs,omitempty"`
	// clientURLs is the list of URLs the member exposes to clients for communication. If the member is not started, clientURLs will be empty.
	ClientURLs []string `protobuf:"bytes,4,rep,name=clientURLs,proto3" json:"clientURLs,omitempty"`
	// isLearner indicates if the member is raft learner.
	IsLearner bool `protobuf:"varint,5,opt,name=isLearner,proto3" json:"isLearner,omitempty"`
}

func (m *Member) Reset()         { *m = Member{} }
func (m *Member) String() string { return proto.CompactTextString(m) }
func (*Member) ProtoMessage()    {}
func (*Member) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{39}
}

func (m *Member) GetID() uint64 {
	if m != nil {
		return m.ID
	}
	return 0
}

func (m *Member) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Member) GetPeerURLs() []string {
	if m != nil {
		return m.PeerURLs
	}
	return nil
}

func (m *Member) GetClientURLs() []string {
	if m != nil {
		return m.ClientURLs
	}
	return nil
}

func (m *Member) GetIsLearner() bool {
	if m != nil {
		return m.IsLearner
	}
	return false
}

type MemberAddRequest struct {
	// peerURLs是新增成员用来与集群通信的URL列表.
	PeerURLs  []string `protobuf:"bytes,1,rep,name=peerURLs,proto3" json:"peerURLs,omitempty"`
	IsLearner bool     `protobuf:"varint,2,opt,name=isLearner,proto3" json:"isLearner,omitempty"`
}

func (m *MemberAddRequest) Reset()         { *m = MemberAddRequest{} }
func (m *MemberAddRequest) String() string { return proto.CompactTextString(m) }
func (*MemberAddRequest) ProtoMessage()    {}
func (*MemberAddRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{40}
}

func (m *MemberAddRequest) GetPeerURLs() []string {
	if m != nil {
		return m.PeerURLs
	}
	return nil
}

func (m *MemberAddRequest) GetIsLearner() bool {
	if m != nil {
		return m.IsLearner
	}
	return false
}

type MemberAddResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// member is the member information for the added member.
	Member *Member `protobuf:"bytes,2,opt,name=member,proto3" json:"member,omitempty"`
	// members is a list of all members after adding the new member.
	Members              []*Member `protobuf:"bytes,3,rep,name=members,proto3" json:"members,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *MemberAddResponse) Reset()         { *m = MemberAddResponse{} }
func (m *MemberAddResponse) String() string { return proto.CompactTextString(m) }
func (*MemberAddResponse) ProtoMessage()    {}
func (*MemberAddResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{41}
}

func (m *MemberAddResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *MemberAddResponse) GetMember() *Member {
	if m != nil {
		return m.Member
	}
	return nil
}

func (m *MemberAddResponse) GetMembers() []*Member {
	if m != nil {
		return m.Members
	}
	return nil
}

type MemberRemoveRequest struct {
	// ID is the member ID of the member to remove.
	ID uint64 `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
}

func (m *MemberRemoveRequest) Reset()         { *m = MemberRemoveRequest{} }
func (m *MemberRemoveRequest) String() string { return proto.CompactTextString(m) }
func (*MemberRemoveRequest) ProtoMessage()    {}
func (*MemberRemoveRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{42}
}

func (m *MemberRemoveRequest) GetID() uint64 {
	if m != nil {
		return m.ID
	}
	return 0
}

type MemberRemoveResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// members is a list of all members after removing the member.
	Members              []*Member `protobuf:"bytes,2,rep,name=members,proto3" json:"members,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *MemberRemoveResponse) Reset()         { *m = MemberRemoveResponse{} }
func (m *MemberRemoveResponse) String() string { return proto.CompactTextString(m) }
func (*MemberRemoveResponse) ProtoMessage()    {}
func (*MemberRemoveResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{43}
}

func (m *MemberRemoveResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *MemberRemoveResponse) GetMembers() []*Member {
	if m != nil {
		return m.Members
	}
	return nil
}

type MemberUpdateRequest struct {
	// ID is the member ID of the member to update.
	ID uint64 `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
	// peerURLs is the new list of URLs the member will use to communicate with the cluster.
	PeerURLs []string `protobuf:"bytes,2,rep,name=peerURLs,proto3" json:"peerURLs,omitempty"`
}

func (m *MemberUpdateRequest) Reset()         { *m = MemberUpdateRequest{} }
func (m *MemberUpdateRequest) String() string { return proto.CompactTextString(m) }
func (*MemberUpdateRequest) ProtoMessage()    {}
func (*MemberUpdateRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{44}
}

func (m *MemberUpdateRequest) GetID() uint64 {
	if m != nil {
		return m.ID
	}
	return 0
}

func (m *MemberUpdateRequest) GetPeerURLs() []string {
	if m != nil {
		return m.PeerURLs
	}
	return nil
}

type MemberUpdateResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// members is a list of all members after updating the member.
	Members              []*Member `protobuf:"bytes,2,rep,name=members,proto3" json:"members,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *MemberUpdateResponse) Reset()         { *m = MemberUpdateResponse{} }
func (m *MemberUpdateResponse) String() string { return proto.CompactTextString(m) }
func (*MemberUpdateResponse) ProtoMessage()    {}
func (*MemberUpdateResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{45}
}

func (m *MemberUpdateResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *MemberUpdateResponse) GetMembers() []*Member {
	if m != nil {
		return m.Members
	}
	return nil
}

type MemberListRequest struct {
	Linearizable bool `protobuf:"varint,1,opt,name=linearizable,proto3" json:"linearizable,omitempty"`
}

func (m *MemberListRequest) Reset()         { *m = MemberListRequest{} }
func (m *MemberListRequest) String() string { return proto.CompactTextString(m) }
func (*MemberListRequest) ProtoMessage()    {}
func (*MemberListRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{46}
}

func (m *MemberListRequest) GetLinearizable() bool {
	if m != nil {
		return m.Linearizable
	}
	return false
}

type MemberListResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// members is a list of all members associated with the cluster.
	Members              []*Member `protobuf:"bytes,2,rep,name=members,proto3" json:"members,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *MemberListResponse) Reset()         { *m = MemberListResponse{} }
func (m *MemberListResponse) String() string { return proto.CompactTextString(m) }
func (*MemberListResponse) ProtoMessage()    {}
func (*MemberListResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{47}
}

func (m *MemberListResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *MemberListResponse) GetMembers() []*Member {
	if m != nil {
		return m.Members
	}
	return nil
}

type MemberPromoteRequest struct {
	// ID is the member ID of the member to promote.
	ID uint64 `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
}

func (m *MemberPromoteRequest) Reset()         { *m = MemberPromoteRequest{} }
func (m *MemberPromoteRequest) String() string { return proto.CompactTextString(m) }
func (*MemberPromoteRequest) ProtoMessage()    {}
func (*MemberPromoteRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{48}
}

func (m *MemberPromoteRequest) GetID() uint64 {
	if m != nil {
		return m.ID
	}
	return 0
}

type MemberPromoteResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// members is a list of all members after promoting the member.
	Members              []*Member `protobuf:"bytes,2,rep,name=members,proto3" json:"members,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *MemberPromoteResponse) Reset()         { *m = MemberPromoteResponse{} }
func (m *MemberPromoteResponse) String() string { return proto.CompactTextString(m) }
func (*MemberPromoteResponse) ProtoMessage()    {}
func (*MemberPromoteResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{49}
}

func (m *MemberPromoteResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *MemberPromoteResponse) GetMembers() []*Member {
	if m != nil {
		return m.Members
	}
	return nil
}

type DefragmentRequest struct{}

func (m *DefragmentRequest) Reset()         { *m = DefragmentRequest{} }
func (m *DefragmentRequest) String() string { return proto.CompactTextString(m) }
func (*DefragmentRequest) ProtoMessage()    {}
func (*DefragmentRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{50}
}

type DefragmentResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *DefragmentResponse) Reset()         { *m = DefragmentResponse{} }
func (m *DefragmentResponse) String() string { return proto.CompactTextString(m) }
func (*DefragmentResponse) ProtoMessage()    {}
func (*DefragmentResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{51}
}

func (m *DefragmentResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

type MoveLeaderRequest struct {
	// targetID is the node ID for the new leader.
	TargetID uint64 `protobuf:"varint,1,opt,name=targetID,proto3" json:"targetID,omitempty"`
}

func (m *MoveLeaderRequest) Reset()         { *m = MoveLeaderRequest{} }
func (m *MoveLeaderRequest) String() string { return proto.CompactTextString(m) }
func (*MoveLeaderRequest) ProtoMessage()    {}
func (*MoveLeaderRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{52}
}

func (m *MoveLeaderRequest) GetTargetID() uint64 {
	if m != nil {
		return m.TargetID
	}
	return 0
}

type MoveLeaderResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *MoveLeaderResponse) Reset()         { *m = MoveLeaderResponse{} }
func (m *MoveLeaderResponse) String() string { return proto.CompactTextString(m) }
func (*MoveLeaderResponse) ProtoMessage()    {}
func (*MoveLeaderResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{53}
}

func (m *MoveLeaderResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

type AlarmRequest struct {
	// action is the kind of alarm request to issue. The action
	// may GET alarm statuses, ACTIVATE an alarm, or DEACTIVATE a
	// raised alarm.
	Action AlarmRequest_AlarmAction `protobuf:"varint,1,opt,name=action,proto3,enum=etcdserverpb.AlarmRequest_AlarmAction" json:"action,omitempty"`
	// memberID is the ID of the member associated with the alarm. If memberID is 0, the
	// alarm request covers all members.
	MemberID uint64 `protobuf:"varint,2,opt,name=memberID,proto3" json:"memberID,omitempty"`
	// alarm is the type of alarm to consider for this request.
	Alarm                AlarmType `protobuf:"varint,3,opt,name=alarm,proto3,enum=etcdserverpb.AlarmType" json:"alarm,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *AlarmRequest) Reset()         { *m = AlarmRequest{} }
func (m *AlarmRequest) String() string { return proto.CompactTextString(m) }
func (*AlarmRequest) ProtoMessage()    {}
func (*AlarmRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{54}
}

func (m *AlarmRequest) GetAction() AlarmRequest_AlarmAction {
	if m != nil {
		return m.Action
	}
	return AlarmRequest_GET
}

func (m *AlarmRequest) GetMemberID() uint64 {
	if m != nil {
		return m.MemberID
	}
	return 0
}

func (m *AlarmRequest) GetAlarm() AlarmType {
	if m != nil {
		return m.Alarm
	}
	return AlarmType_NONE
}

type AlarmMember struct {
	MemberID uint64    `protobuf:"varint,1,opt,name=memberID,proto3" json:"memberID,omitempty"`
	Alarm    AlarmType `protobuf:"varint,2,opt,name=alarm,proto3,enum=etcdserverpb.AlarmType" json:"alarm,omitempty"`
}

func (m *AlarmMember) Reset()         { *m = AlarmMember{} }
func (m *AlarmMember) String() string { return proto.CompactTextString(m) }
func (*AlarmMember) ProtoMessage()    {}
func (*AlarmMember) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{55}
}

func (m *AlarmMember) GetMemberID() uint64 {
	if m != nil {
		return m.MemberID
	}
	return 0
}

func (m *AlarmMember) GetAlarm() AlarmType {
	if m != nil {
		return m.Alarm
	}
	return AlarmType_NONE
}

type AlarmResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// alarms is a list of alarms associated with the alarm request.
	Alarms               []*AlarmMember `protobuf:"bytes,2,rep,name=alarms,proto3" json:"alarms,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *AlarmResponse) Reset()         { *m = AlarmResponse{} }
func (m *AlarmResponse) String() string { return proto.CompactTextString(m) }
func (*AlarmResponse) ProtoMessage()    {}
func (*AlarmResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{56}
}

func (m *AlarmResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *AlarmResponse) GetAlarms() []*AlarmMember {
	if m != nil {
		return m.Alarms
	}
	return nil
}

type DowngradeRequest struct {
	// action is the kind of downgrade request to issue. The action may
	// VALIDATE the target version, DOWNGRADE the cluster version,
	// or CANCEL the current downgrading job.
	Action DowngradeRequest_DowngradeAction `protobuf:"varint,1,opt,name=action,proto3,enum=etcdserverpb.DowngradeRequest_DowngradeAction" json:"action,omitempty"`
	// version is the target version to downgrade.
	Version string `protobuf:"bytes,2,opt,name=version,proto3" json:"version,omitempty"`
}

func (m *DowngradeRequest) Reset()         { *m = DowngradeRequest{} }
func (m *DowngradeRequest) String() string { return proto.CompactTextString(m) }
func (*DowngradeRequest) ProtoMessage()    {}
func (*DowngradeRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{57}
}

func (m *DowngradeRequest) GetAction() DowngradeRequest_DowngradeAction {
	if m != nil {
		return m.Action
	}
	return DowngradeRequest_VALIDATE
}

func (m *DowngradeRequest) GetVersion() string {
	if m != nil {
		return m.Version
	}
	return ""
}

type DowngradeResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// version is the current cluster version.
	Version string `protobuf:"bytes,2,opt,name=version,proto3" json:"version,omitempty"`
}

func (m *DowngradeResponse) Reset()         { *m = DowngradeResponse{} }
func (m *DowngradeResponse) String() string { return proto.CompactTextString(m) }
func (*DowngradeResponse) ProtoMessage()    {}
func (*DowngradeResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{58}
}

func (m *DowngradeResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *DowngradeResponse) GetVersion() string {
	if m != nil {
		return m.Version
	}
	return ""
}

type StatusRequest struct{}

func (m *StatusRequest) Reset()         { *m = StatusRequest{} }
func (m *StatusRequest) String() string { return proto.CompactTextString(m) }
func (*StatusRequest) ProtoMessage()    {}
func (*StatusRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{59}
}

type StatusResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// version is the cluster protocol version used by the responding member.
	Version string `protobuf:"bytes,2,opt,name=version,proto3" json:"version,omitempty"`
	// dbSize is the size of the backend database physically allocated, in bytes, of the responding member.
	DbSize int64 `protobuf:"varint,3,opt,name=dbSize,proto3" json:"dbSize,omitempty"`
	// leader is the member ID which the responding member believes is the current leader.
	Leader uint64 `protobuf:"varint,4,opt,name=leader,proto3" json:"leader,omitempty"`
	// raftIndex is the current raft committed index of the responding member.
	RaftIndex uint64 `protobuf:"varint,5,opt,name=raftIndex,proto3" json:"raftIndex,omitempty"`
	// raftTerm is the current raft term of the responding member.
	RaftTerm uint64 `protobuf:"varint,6,opt,name=raftTerm,proto3" json:"raftTerm,omitempty"`
	// raftAppliedIndex is the current raft applied index of the responding member.
	RaftAppliedIndex uint64 `protobuf:"varint,7,opt,name=raftAppliedIndex,proto3" json:"raftAppliedIndex,omitempty"`
	// errors contains alarm/health information and status.
	Errors []string `protobuf:"bytes,8,rep,name=errors,proto3" json:"errors,omitempty"`
	// dbSizeInUse is the size of the backend database logically in use, in bytes, of the responding member.
	DbSizeInUse int64 `protobuf:"varint,9,opt,name=dbSizeInUse,proto3" json:"dbSizeInUse,omitempty"`
	// isLearner indicates if the member is raft learner.
	IsLearner bool `protobuf:"varint,10,opt,name=isLearner,proto3" json:"isLearner,omitempty"`
}

func (m *StatusResponse) Reset()         { *m = StatusResponse{} }
func (m *StatusResponse) String() string { return proto.CompactTextString(m) }
func (*StatusResponse) ProtoMessage()    {}
func (*StatusResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{60}
}

func (m *StatusResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *StatusResponse) GetVersion() string {
	if m != nil {
		return m.Version
	}
	return ""
}

func (m *StatusResponse) GetDbSize() int64 {
	if m != nil {
		return m.DbSize
	}
	return 0
}

func (m *StatusResponse) GetLeader() uint64 {
	if m != nil {
		return m.Leader
	}
	return 0
}

func (m *StatusResponse) GetRaftIndex() uint64 {
	if m != nil {
		return m.RaftIndex
	}
	return 0
}

func (m *StatusResponse) GetRaftTerm() uint64 {
	if m != nil {
		return m.RaftTerm
	}
	return 0
}

func (m *StatusResponse) GetRaftAppliedIndex() uint64 {
	if m != nil {
		return m.RaftAppliedIndex
	}
	return 0
}

func (m *StatusResponse) GetErrors() []string {
	if m != nil {
		return m.Errors
	}
	return nil
}

func (m *StatusResponse) GetDbSizeInUse() int64 {
	if m != nil {
		return m.DbSizeInUse
	}
	return 0
}

func (m *StatusResponse) GetIsLearner() bool {
	if m != nil {
		return m.IsLearner
	}
	return false
}

type AuthEnableRequest struct{}

func (m *AuthEnableRequest) Reset()         { *m = AuthEnableRequest{} }
func (m *AuthEnableRequest) String() string { return proto.CompactTextString(m) }
func (*AuthEnableRequest) ProtoMessage()    {}
func (*AuthEnableRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{61}
}

type AuthDisableRequest struct{}

func (m *AuthDisableRequest) Reset()         { *m = AuthDisableRequest{} }
func (m *AuthDisableRequest) String() string { return proto.CompactTextString(m) }
func (*AuthDisableRequest) ProtoMessage()    {}
func (*AuthDisableRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{62}
}

type AuthStatusRequest struct{}

func (m *AuthStatusRequest) Reset()         { *m = AuthStatusRequest{} }
func (m *AuthStatusRequest) String() string { return proto.CompactTextString(m) }
func (*AuthStatusRequest) ProtoMessage()    {}
func (*AuthStatusRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{63}
}

type AuthenticateRequest struct {
	Name     string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Password string `protobuf:"bytes,2,opt,name=password,proto3" json:"password,omitempty"`
}

func (m *AuthenticateRequest) Reset()         { *m = AuthenticateRequest{} }
func (m *AuthenticateRequest) String() string { return proto.CompactTextString(m) }
func (*AuthenticateRequest) ProtoMessage()    {}
func (*AuthenticateRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{64}
}

func (m *AuthenticateRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *AuthenticateRequest) GetPassword() string {
	if m != nil {
		return m.Password
	}
	return ""
}

type AuthUserAddRequest struct {
	Name                 string                 `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Password             string                 `protobuf:"bytes,2,opt,name=password,proto3" json:"password,omitempty"`
	Options              *authpb.UserAddOptions `protobuf:"bytes,3,opt,name=options,proto3" json:"options,omitempty"`
	HashedPassword       string                 `protobuf:"bytes,4,opt,name=hashedPassword,proto3" json:"hashedPassword,omitempty"`
	XXX_NoUnkeyedLiteral struct{}               `json:"-"`
	XXX_unrecognized     []byte                 `json:"-"`
	XXX_sizecache        int32                  `json:"-"`
}

func (m *AuthUserAddRequest) Reset()         { *m = AuthUserAddRequest{} }
func (m *AuthUserAddRequest) String() string { return proto.CompactTextString(m) }
func (*AuthUserAddRequest) ProtoMessage()    {}
func (*AuthUserAddRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{65}
}

func (m *AuthUserAddRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *AuthUserAddRequest) GetPassword() string {
	if m != nil {
		return m.Password
	}
	return ""
}

func (m *AuthUserAddRequest) GetOptions() *authpb.UserAddOptions {
	if m != nil {
		return m.Options
	}
	return nil
}

func (m *AuthUserAddRequest) GetHashedPassword() string {
	if m != nil {
		return m.HashedPassword
	}
	return ""
}

type AuthUserGetRequest struct {
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

func (m *AuthUserGetRequest) Reset()         { *m = AuthUserGetRequest{} }
func (m *AuthUserGetRequest) String() string { return proto.CompactTextString(m) }
func (*AuthUserGetRequest) ProtoMessage()    {}
func (*AuthUserGetRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{66}
}

func (m *AuthUserGetRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

type AuthUserDeleteRequest struct {
	// name is the name of the user to delete.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

func (m *AuthUserDeleteRequest) Reset()         { *m = AuthUserDeleteRequest{} }
func (m *AuthUserDeleteRequest) String() string { return proto.CompactTextString(m) }
func (*AuthUserDeleteRequest) ProtoMessage()    {}
func (*AuthUserDeleteRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{67}
}

func (m *AuthUserDeleteRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

type AuthUserChangePasswordRequest struct {
	// name is the name of the user whose password is being changed.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// password is the new password for the user. Note that this field will be removed in the API layer.
	Password string `protobuf:"bytes,2,opt,name=password,proto3" json:"password,omitempty"`
	// hashedPassword is the new password for the user. Note that this field will be initialized in the API layer.
	HashedPassword string `protobuf:"bytes,3,opt,name=hashedPassword,proto3" json:"hashedPassword,omitempty"`
}

func (m *AuthUserChangePasswordRequest) Reset()         { *m = AuthUserChangePasswordRequest{} }
func (m *AuthUserChangePasswordRequest) String() string { return proto.CompactTextString(m) }
func (*AuthUserChangePasswordRequest) ProtoMessage()    {}
func (*AuthUserChangePasswordRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{68}
}

func (m *AuthUserChangePasswordRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *AuthUserChangePasswordRequest) GetPassword() string {
	if m != nil {
		return m.Password
	}
	return ""
}

func (m *AuthUserChangePasswordRequest) GetHashedPassword() string {
	if m != nil {
		return m.HashedPassword
	}
	return ""
}

type AuthUserGrantRoleRequest struct {
	// user is the name of the user which should be granted a given role.
	User string `protobuf:"bytes,1,opt,name=user,proto3" json:"user,omitempty"`
	// role is the name of the role to grant to the user.
	Role string `protobuf:"bytes,2,opt,name=role,proto3" json:"role,omitempty"`
}

func (m *AuthUserGrantRoleRequest) Reset()         { *m = AuthUserGrantRoleRequest{} }
func (m *AuthUserGrantRoleRequest) String() string { return proto.CompactTextString(m) }
func (*AuthUserGrantRoleRequest) ProtoMessage()    {}
func (*AuthUserGrantRoleRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{69}
}

func (m *AuthUserGrantRoleRequest) GetUser() string {
	if m != nil {
		return m.User
	}
	return ""
}

func (m *AuthUserGrantRoleRequest) GetRole() string {
	if m != nil {
		return m.Role
	}
	return ""
}

type AuthUserRevokeRoleRequest struct {
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Role string `protobuf:"bytes,2,opt,name=role,proto3" json:"role,omitempty"`
}

func (m *AuthUserRevokeRoleRequest) Reset()         { *m = AuthUserRevokeRoleRequest{} }
func (m *AuthUserRevokeRoleRequest) String() string { return proto.CompactTextString(m) }
func (*AuthUserRevokeRoleRequest) ProtoMessage()    {}
func (*AuthUserRevokeRoleRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{70}
}

func (m *AuthUserRevokeRoleRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *AuthUserRevokeRoleRequest) GetRole() string {
	if m != nil {
		return m.Role
	}
	return ""
}

type AuthRoleAddRequest struct {
	// name is the name of the role to add to the authentication system.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

func (m *AuthRoleAddRequest) Reset()         { *m = AuthRoleAddRequest{} }
func (m *AuthRoleAddRequest) String() string { return proto.CompactTextString(m) }
func (*AuthRoleAddRequest) ProtoMessage()    {}
func (*AuthRoleAddRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{71}
}

func (m *AuthRoleAddRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

type AuthRoleGetRequest struct {
	Role string `protobuf:"bytes,1,opt,name=role,proto3" json:"role,omitempty"`
}

func (m *AuthRoleGetRequest) Reset()         { *m = AuthRoleGetRequest{} }
func (m *AuthRoleGetRequest) String() string { return proto.CompactTextString(m) }
func (*AuthRoleGetRequest) ProtoMessage()    {}
func (*AuthRoleGetRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{72}
}

func (m *AuthRoleGetRequest) GetRole() string {
	if m != nil {
		return m.Role
	}
	return ""
}

type AuthUserListRequest struct{}

func (m *AuthUserListRequest) Reset()         { *m = AuthUserListRequest{} }
func (m *AuthUserListRequest) String() string { return proto.CompactTextString(m) }
func (*AuthUserListRequest) ProtoMessage()    {}
func (*AuthUserListRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{73}
}

type AuthRoleListRequest struct{}

func (m *AuthRoleListRequest) Reset()         { *m = AuthRoleListRequest{} }
func (m *AuthRoleListRequest) String() string { return proto.CompactTextString(m) }
func (*AuthRoleListRequest) ProtoMessage()    {}
func (*AuthRoleListRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{74}
}

type AuthRoleDeleteRequest struct {
	Role string `protobuf:"bytes,1,opt,name=role,proto3" json:"role,omitempty"`
}

func (m *AuthRoleDeleteRequest) Reset()         { *m = AuthRoleDeleteRequest{} }
func (m *AuthRoleDeleteRequest) String() string { return proto.CompactTextString(m) }
func (*AuthRoleDeleteRequest) ProtoMessage()    {}
func (*AuthRoleDeleteRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{75}
}

func (m *AuthRoleDeleteRequest) GetRole() string {
	if m != nil {
		return m.Role
	}
	return ""
}

type AuthRoleGrantPermissionRequest struct {
	// name is the name of the role which will be granted the permission.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Perm是赋予角色的权限.
	Perm                 *authpb.Permission `protobuf:"bytes,2,opt,name=perm,proto3" json:"perm,omitempty"`
	XXX_NoUnkeyedLiteral struct{}           `json:"-"`
	XXX_unrecognized     []byte             `json:"-"`
	XXX_sizecache        int32              `json:"-"`
}

func (m *AuthRoleGrantPermissionRequest) Reset()         { *m = AuthRoleGrantPermissionRequest{} }
func (m *AuthRoleGrantPermissionRequest) String() string { return proto.CompactTextString(m) }
func (*AuthRoleGrantPermissionRequest) ProtoMessage()    {}
func (*AuthRoleGrantPermissionRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{76}
}

func (m *AuthRoleGrantPermissionRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *AuthRoleGrantPermissionRequest) GetPerm() *authpb.Permission {
	if m != nil {
		return m.Perm
	}
	return nil
}

type AuthRoleRevokePermissionRequest struct {
	Role     string `protobuf:"bytes,1,opt,name=role,proto3" json:"role,omitempty"`
	Key      string `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	RangeEnd string `protobuf:"bytes,3,opt,name=range_end,json=rangeEnd,proto3" json:"range_end,omitempty"`
}

func (m *AuthRoleRevokePermissionRequest) Reset()         { *m = AuthRoleRevokePermissionRequest{} }
func (m *AuthRoleRevokePermissionRequest) String() string { return proto.CompactTextString(m) }
func (*AuthRoleRevokePermissionRequest) ProtoMessage()    {}
func (*AuthRoleRevokePermissionRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{77}
}

func (m *AuthRoleRevokePermissionRequest) GetRole() string {
	if m != nil {
		return m.Role
	}
	return ""
}

func (m *AuthRoleRevokePermissionRequest) GetKey() []byte {
	if m != nil {
		return []byte(m.Key)
	}
	return nil
}

func (m *AuthRoleRevokePermissionRequest) GetRangeEnd() []byte {
	if m != nil {
		return []byte(m.RangeEnd)
	}
	return nil
}

type AuthEnableResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *AuthEnableResponse) Reset()         { *m = AuthEnableResponse{} }
func (m *AuthEnableResponse) String() string { return proto.CompactTextString(m) }
func (*AuthEnableResponse) ProtoMessage()    {}
func (*AuthEnableResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{78}
}

func (m *AuthEnableResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

type AuthDisableResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *AuthDisableResponse) Reset()         { *m = AuthDisableResponse{} }
func (m *AuthDisableResponse) String() string { return proto.CompactTextString(m) }
func (*AuthDisableResponse) ProtoMessage()    {}
func (*AuthDisableResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{79}
}

func (m *AuthDisableResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

type AuthStatusResponse struct {
	Header       *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	Enabled      bool            `protobuf:"varint,2,opt,name=enabled,proto3" json:"enabled,omitempty"`
	AuthRevision uint64          `protobuf:"varint,3,opt,name=authRevision,proto3" json:"authRevision,omitempty"`
}

func (m *AuthStatusResponse) Reset()         { *m = AuthStatusResponse{} }
func (m *AuthStatusResponse) String() string { return proto.CompactTextString(m) }
func (*AuthStatusResponse) ProtoMessage()    {}
func (*AuthStatusResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{80}
}

func (m *AuthStatusResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *AuthStatusResponse) GetEnabled() bool {
	if m != nil {
		return m.Enabled
	}
	return false
}

func (m *AuthStatusResponse) GetAuthRevision() uint64 {
	if m != nil {
		return m.AuthRevision
	}
	return 0
}

type AuthenticateResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// token is an authorized token that can be used in succeeding RPCs
	Token string `protobuf:"bytes,2,opt,name=token,proto3" json:"token,omitempty"`
}

func (m *AuthenticateResponse) Reset()         { *m = AuthenticateResponse{} }
func (m *AuthenticateResponse) String() string { return proto.CompactTextString(m) }
func (*AuthenticateResponse) ProtoMessage()    {}
func (*AuthenticateResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{81}
}

func (m *AuthenticateResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *AuthenticateResponse) GetToken() string {
	if m != nil {
		return m.Token
	}
	return ""
}

type AuthUserAddResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *AuthUserAddResponse) Reset()         { *m = AuthUserAddResponse{} }
func (m *AuthUserAddResponse) String() string { return proto.CompactTextString(m) }
func (*AuthUserAddResponse) ProtoMessage()    {}
func (*AuthUserAddResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{82}
}

func (m *AuthUserAddResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

type AuthUserGetResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	Roles                []string        `protobuf:"bytes,2,rep,name=roles,proto3" json:"roles,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *AuthUserGetResponse) Reset()         { *m = AuthUserGetResponse{} }
func (m *AuthUserGetResponse) String() string { return proto.CompactTextString(m) }
func (*AuthUserGetResponse) ProtoMessage()    {}
func (*AuthUserGetResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{83}
}

func (m *AuthUserGetResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *AuthUserGetResponse) GetRoles() []string {
	if m != nil {
		return m.Roles
	}
	return nil
}

type AuthUserDeleteResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *AuthUserDeleteResponse) Reset()         { *m = AuthUserDeleteResponse{} }
func (m *AuthUserDeleteResponse) String() string { return proto.CompactTextString(m) }
func (*AuthUserDeleteResponse) ProtoMessage()    {}
func (*AuthUserDeleteResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{84}
}

func (m *AuthUserDeleteResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

type AuthUserChangePasswordResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *AuthUserChangePasswordResponse) Reset()         { *m = AuthUserChangePasswordResponse{} }
func (m *AuthUserChangePasswordResponse) String() string { return proto.CompactTextString(m) }
func (*AuthUserChangePasswordResponse) ProtoMessage()    {}
func (*AuthUserChangePasswordResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{85}
}

func (m *AuthUserChangePasswordResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

type AuthUserGrantRoleResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *AuthUserGrantRoleResponse) Reset()         { *m = AuthUserGrantRoleResponse{} }
func (m *AuthUserGrantRoleResponse) String() string { return proto.CompactTextString(m) }
func (*AuthUserGrantRoleResponse) ProtoMessage()    {}
func (*AuthUserGrantRoleResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{86}
}

func (m *AuthUserGrantRoleResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

type AuthUserRevokeRoleResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *AuthUserRevokeRoleResponse) Reset()         { *m = AuthUserRevokeRoleResponse{} }
func (m *AuthUserRevokeRoleResponse) String() string { return proto.CompactTextString(m) }
func (*AuthUserRevokeRoleResponse) ProtoMessage()    {}
func (*AuthUserRevokeRoleResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{87}
}

func (m *AuthUserRevokeRoleResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

type AuthRoleAddResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *AuthRoleAddResponse) Reset()         { *m = AuthRoleAddResponse{} }
func (m *AuthRoleAddResponse) String() string { return proto.CompactTextString(m) }
func (*AuthRoleAddResponse) ProtoMessage()    {}
func (*AuthRoleAddResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{88}
}

func (m *AuthRoleAddResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

type AuthRoleGetResponse struct {
	Header               *ResponseHeader      `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	Perm                 []*authpb.Permission `protobuf:"bytes,2,rep,name=perm,proto3" json:"perm,omitempty"`
	XXX_NoUnkeyedLiteral struct{}             `json:"-"`
	XXX_unrecognized     []byte               `json:"-"`
	XXX_sizecache        int32                `json:"-"`
}

func (m *AuthRoleGetResponse) Reset()         { *m = AuthRoleGetResponse{} }
func (m *AuthRoleGetResponse) String() string { return proto.CompactTextString(m) }
func (*AuthRoleGetResponse) ProtoMessage()    {}
func (*AuthRoleGetResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{89}
}

func (m *AuthRoleGetResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *AuthRoleGetResponse) GetPerm() []*authpb.Permission {
	if m != nil {
		return m.Perm
	}
	return nil
}

type AuthRoleListResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	Roles                []string        `protobuf:"bytes,2,rep,name=roles,proto3" json:"roles,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *AuthRoleListResponse) Reset()         { *m = AuthRoleListResponse{} }
func (m *AuthRoleListResponse) String() string { return proto.CompactTextString(m) }
func (*AuthRoleListResponse) ProtoMessage()    {}
func (*AuthRoleListResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{90}
}

func (m *AuthRoleListResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *AuthRoleListResponse) GetRoles() []string {
	if m != nil {
		return m.Roles
	}
	return nil
}

type AuthUserListResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	Users                []string        `protobuf:"bytes,2,rep,name=users,proto3" json:"users,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *AuthUserListResponse) Reset()         { *m = AuthUserListResponse{} }
func (m *AuthUserListResponse) String() string { return proto.CompactTextString(m) }
func (*AuthUserListResponse) ProtoMessage()    {}
func (*AuthUserListResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{91}
}

func (m *AuthUserListResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *AuthUserListResponse) GetUsers() []string {
	if m != nil {
		return m.Users
	}
	return nil
}

type AuthRoleDeleteResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *AuthRoleDeleteResponse) Reset()         { *m = AuthRoleDeleteResponse{} }
func (m *AuthRoleDeleteResponse) String() string { return proto.CompactTextString(m) }
func (*AuthRoleDeleteResponse) ProtoMessage()    {}
func (*AuthRoleDeleteResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{92}
}

func (m *AuthRoleDeleteResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

type AuthRoleGrantPermissionResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *AuthRoleGrantPermissionResponse) Reset()         { *m = AuthRoleGrantPermissionResponse{} }
func (m *AuthRoleGrantPermissionResponse) String() string { return proto.CompactTextString(m) }
func (*AuthRoleGrantPermissionResponse) ProtoMessage()    {}
func (*AuthRoleGrantPermissionResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{93}
}

func (m *AuthRoleGrantPermissionResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

type AuthRoleRevokePermissionResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *AuthRoleRevokePermissionResponse) Reset()         { *m = AuthRoleRevokePermissionResponse{} }
func (m *AuthRoleRevokePermissionResponse) String() string { return proto.CompactTextString(m) }
func (*AuthRoleRevokePermissionResponse) ProtoMessage()    {}
func (*AuthRoleRevokePermissionResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{94}
}

func (m *AuthRoleRevokePermissionResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func init() {
	proto.RegisterEnum("etcdserverpb.AlarmType", AlarmType_name, AlarmType_value)
	proto.RegisterEnum("etcdserverpb.RangeRequest_SortOrder", RangeRequest_SortOrder_name, RangeRequest_SortOrder_value)
	proto.RegisterEnum("etcdserverpb.RangeRequest_SortTarget", RangeRequest_SortTarget_name, RangeRequest_SortTarget_value)
	proto.RegisterEnum("etcdserverpb.Compare_CompareResult", Compare_CompareResult_name, Compare_CompareResult_value)
	proto.RegisterEnum("etcdserverpb.Compare_CompareTarget", Compare_CompareTarget_name, Compare_CompareTarget_value)
	proto.RegisterEnum("etcdserverpb.WatchCreateRequest_FilterType", WatchCreateRequest_FilterType_name, WatchCreateRequest_FilterType_value)
	proto.RegisterEnum("etcdserverpb.AlarmRequest_AlarmAction", AlarmRequest_AlarmAction_name, AlarmRequest_AlarmAction_value)
	proto.RegisterEnum("etcdserverpb.DowngradeRequest_DowngradeAction", DowngradeRequest_DowngradeAction_name, DowngradeRequest_DowngradeAction_value)
	proto.RegisterType((*ResponseHeader)(nil), "etcdserverpb.ResponseHeader")
	proto.RegisterType((*RangeRequest)(nil), "etcdserverpb.RangeRequest")
	proto.RegisterType((*RangeResponse)(nil), "etcdserverpb.RangeResponse")
	proto.RegisterType((*PutRequest)(nil), "etcdserverpb.PutRequest")
	proto.RegisterType((*PutResponse)(nil), "etcdserverpb.PutResponse")
	proto.RegisterType((*DeleteRangeRequest)(nil), "etcdserverpb.DeleteRangeRequest")
	proto.RegisterType((*DeleteRangeResponse)(nil), "etcdserverpb.DeleteRangeResponse")
	proto.RegisterType((*RequestOp)(nil), "etcdserverpb.RequestOp")
	proto.RegisterType((*ResponseOp)(nil), "etcdserverpb.ResponseOp")
	proto.RegisterType((*Compare)(nil), "etcdserverpb.Compare")
	proto.RegisterType((*TxnRequest)(nil), "etcdserverpb.TxnRequest")
	proto.RegisterType((*TxnResponse)(nil), "etcdserverpb.TxnResponse")
	proto.RegisterType((*CompactionRequest)(nil), "etcdserverpb.CompactionRequest")
	proto.RegisterType((*CompactionResponse)(nil), "etcdserverpb.CompactionResponse")
	proto.RegisterType((*HashRequest)(nil), "etcdserverpb.HashRequest")
	proto.RegisterType((*HashKVRequest)(nil), "etcdserverpb.HashKVRequest")
	proto.RegisterType((*HashKVResponse)(nil), "etcdserverpb.HashKVResponse")
	proto.RegisterType((*HashResponse)(nil), "etcdserverpb.HashResponse")
	proto.RegisterType((*SnapshotRequest)(nil), "etcdserverpb.SnapshotRequest")
	proto.RegisterType((*SnapshotResponse)(nil), "etcdserverpb.SnapshotResponse")
	proto.RegisterType((*WatchRequest)(nil), "etcdserverpb.WatchRequest")
	proto.RegisterType((*WatchCreateRequest)(nil), "etcdserverpb.WatchCreateRequest")
	proto.RegisterType((*WatchCancelRequest)(nil), "etcdserverpb.WatchCancelRequest")
	proto.RegisterType((*WatchProgressRequest)(nil), "etcdserverpb.WatchProgressRequest")
	proto.RegisterType((*WatchResponse)(nil), "etcdserverpb.WatchResponse")
	proto.RegisterType((*LeaseGrantRequest)(nil), "etcdserverpb.LeaseGrantRequest")
	proto.RegisterType((*LeaseGrantResponse)(nil), "etcdserverpb.LeaseGrantResponse")
	proto.RegisterType((*LeaseRevokeRequest)(nil), "etcdserverpb.LeaseRevokeRequest")
	proto.RegisterType((*LeaseRevokeResponse)(nil), "etcdserverpb.LeaseRevokeResponse")
	proto.RegisterType((*LeaseCheckpoint)(nil), "etcdserverpb.LeaseCheckpoint")
	proto.RegisterType((*LeaseCheckpointRequest)(nil), "etcdserverpb.LeaseCheckpointRequest")
	proto.RegisterType((*LeaseCheckpointResponse)(nil), "etcdserverpb.LeaseCheckpointResponse")
	proto.RegisterType((*LeaseKeepAliveRequest)(nil), "etcdserverpb.LeaseKeepAliveRequest")
	proto.RegisterType((*LeaseKeepAliveResponse)(nil), "etcdserverpb.LeaseKeepAliveResponse")
	proto.RegisterType((*LeaseTimeToLiveRequest)(nil), "etcdserverpb.LeaseTimeToLiveRequest")
	proto.RegisterType((*LeaseTimeToLiveResponse)(nil), "etcdserverpb.LeaseTimeToLiveResponse")
	proto.RegisterType((*LeaseLeasesRequest)(nil), "etcdserverpb.LeaseLeasesRequest")
	proto.RegisterType((*LeaseStatus)(nil), "etcdserverpb.LeaseStatus")
	proto.RegisterType((*LeaseLeasesResponse)(nil), "etcdserverpb.LeaseLeasesResponse")
	proto.RegisterType((*Member)(nil), "etcdserverpb.Member")
	proto.RegisterType((*MemberAddRequest)(nil), "etcdserverpb.MemberAddRequest")
	proto.RegisterType((*MemberAddResponse)(nil), "etcdserverpb.MemberAddResponse")
	proto.RegisterType((*MemberRemoveRequest)(nil), "etcdserverpb.MemberRemoveRequest")
	proto.RegisterType((*MemberRemoveResponse)(nil), "etcdserverpb.MemberRemoveResponse")
	proto.RegisterType((*MemberUpdateRequest)(nil), "etcdserverpb.MemberUpdateRequest")
	proto.RegisterType((*MemberUpdateResponse)(nil), "etcdserverpb.MemberUpdateResponse")
	proto.RegisterType((*MemberListRequest)(nil), "etcdserverpb.MemberListRequest")
	proto.RegisterType((*MemberListResponse)(nil), "etcdserverpb.MemberListResponse")
	proto.RegisterType((*MemberPromoteRequest)(nil), "etcdserverpb.MemberPromoteRequest")
	proto.RegisterType((*MemberPromoteResponse)(nil), "etcdserverpb.MemberPromoteResponse")
	proto.RegisterType((*DefragmentRequest)(nil), "etcdserverpb.DefragmentRequest")
	proto.RegisterType((*DefragmentResponse)(nil), "etcdserverpb.DefragmentResponse")
	proto.RegisterType((*MoveLeaderRequest)(nil), "etcdserverpb.MoveLeaderRequest")
	proto.RegisterType((*MoveLeaderResponse)(nil), "etcdserverpb.MoveLeaderResponse")
	proto.RegisterType((*AlarmRequest)(nil), "etcdserverpb.AlarmRequest")
	proto.RegisterType((*AlarmMember)(nil), "etcdserverpb.AlarmMember")
	proto.RegisterType((*AlarmResponse)(nil), "etcdserverpb.AlarmResponse")
	proto.RegisterType((*DowngradeRequest)(nil), "etcdserverpb.DowngradeRequest")
	proto.RegisterType((*DowngradeResponse)(nil), "etcdserverpb.DowngradeResponse")
	proto.RegisterType((*StatusRequest)(nil), "etcdserverpb.StatusRequest")
	proto.RegisterType((*StatusResponse)(nil), "etcdserverpb.StatusResponse")
	proto.RegisterType((*AuthEnableRequest)(nil), "etcdserverpb.AuthEnableRequest")
	proto.RegisterType((*AuthDisableRequest)(nil), "etcdserverpb.AuthDisableRequest")
	proto.RegisterType((*AuthStatusRequest)(nil), "etcdserverpb.AuthStatusRequest")
	proto.RegisterType((*AuthenticateRequest)(nil), "etcdserverpb.AuthenticateRequest")
	proto.RegisterType((*AuthUserAddRequest)(nil), "etcdserverpb.AuthUserAddRequest")
	proto.RegisterType((*AuthUserGetRequest)(nil), "etcdserverpb.AuthUserGetRequest")
	proto.RegisterType((*AuthUserDeleteRequest)(nil), "etcdserverpb.AuthUserDeleteRequest")
	proto.RegisterType((*AuthUserChangePasswordRequest)(nil), "etcdserverpb.AuthUserChangePasswordRequest")
	proto.RegisterType((*AuthUserGrantRoleRequest)(nil), "etcdserverpb.AuthUserGrantRoleRequest")
	proto.RegisterType((*AuthUserRevokeRoleRequest)(nil), "etcdserverpb.AuthUserRevokeRoleRequest")
	proto.RegisterType((*AuthRoleAddRequest)(nil), "etcdserverpb.AuthRoleAddRequest")
	proto.RegisterType((*AuthRoleGetRequest)(nil), "etcdserverpb.AuthRoleGetRequest")
	proto.RegisterType((*AuthUserListRequest)(nil), "etcdserverpb.AuthUserListRequest")
	proto.RegisterType((*AuthRoleListRequest)(nil), "etcdserverpb.AuthRoleListRequest")
	proto.RegisterType((*AuthRoleDeleteRequest)(nil), "etcdserverpb.AuthRoleDeleteRequest")
	proto.RegisterType((*AuthRoleGrantPermissionRequest)(nil), "etcdserverpb.AuthRoleGrantPermissionRequest")
	proto.RegisterType((*AuthRoleRevokePermissionRequest)(nil), "etcdserverpb.AuthRoleRevokePermissionRequest")
	proto.RegisterType((*AuthEnableResponse)(nil), "etcdserverpb.AuthEnableResponse")
	proto.RegisterType((*AuthDisableResponse)(nil), "etcdserverpb.AuthDisableResponse")
	proto.RegisterType((*AuthStatusResponse)(nil), "etcdserverpb.AuthStatusResponse")
	proto.RegisterType((*AuthenticateResponse)(nil), "etcdserverpb.AuthenticateResponse")
	proto.RegisterType((*AuthUserAddResponse)(nil), "etcdserverpb.AuthUserAddResponse")
	proto.RegisterType((*AuthUserGetResponse)(nil), "etcdserverpb.AuthUserGetResponse")
	proto.RegisterType((*AuthUserDeleteResponse)(nil), "etcdserverpb.AuthUserDeleteResponse")
	proto.RegisterType((*AuthUserChangePasswordResponse)(nil), "etcdserverpb.AuthUserChangePasswordResponse")
	proto.RegisterType((*AuthUserGrantRoleResponse)(nil), "etcdserverpb.AuthUserGrantRoleResponse")
	proto.RegisterType((*AuthUserRevokeRoleResponse)(nil), "etcdserverpb.AuthUserRevokeRoleResponse")
	proto.RegisterType((*AuthRoleAddResponse)(nil), "etcdserverpb.AuthRoleAddResponse")
	proto.RegisterType((*AuthRoleGetResponse)(nil), "etcdserverpb.AuthRoleGetResponse")
	proto.RegisterType((*AuthRoleListResponse)(nil), "etcdserverpb.AuthRoleListResponse")
	proto.RegisterType((*AuthUserListResponse)(nil), "etcdserverpb.AuthUserListResponse")
	proto.RegisterType((*AuthRoleDeleteResponse)(nil), "etcdserverpb.AuthRoleDeleteResponse")
	proto.RegisterType((*AuthRoleGrantPermissionResponse)(nil), "etcdserverpb.AuthRoleGrantPermissionResponse")
	proto.RegisterType((*AuthRoleRevokePermissionResponse)(nil), "etcdserverpb.AuthRoleRevokePermissionResponse")
}

func init() { proto.RegisterFile("rpc.proto", fileDescriptor_77a6da22d6a3feb1) }

var fileDescriptor_77a6da22d6a3feb1 = []byte{
	// 4107 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xbc, 0x5b, 0x5b, 0x73, 0x1b, 0xc9,
	0x75, 0xe6, 0x00, 0xc4, 0xed, 0xe0, 0x42, 0xb0, 0x79, 0x11, 0x84, 0x95, 0x28, 0x6e, 0x6b, 0xa5,
	0xe5, 0x4a, 0xbb, 0xc4, 0x9a, 0xb6, 0xb3, 0x55, 0x4a, 0xe2, 0x18, 0x22, 0xb1, 0x12, 0x97, 0x14,
	0xc9, 0x1d, 0x42, 0xda, 0x4b, 0xb9, 0xc2, 0x1a, 0x02, 0x2d, 0x72, 0x42, 0x60, 0x06, 0x9e, 0x19,
	0x40, 0xe4, 0xe6, 0xe2, 0x94, 0xcb, 0x71, 0x25, 0xaf, 0x76, 0x55, 0x2a, 0x79, 0x48, 0x5e, 0x52,
	0x29, 0x97, 0x1f, 0xfc, 0x9c, 0xbf, 0x90, 0xa7, 0x5c, 0x2a, 0x7f, 0x20, 0xb5, 0xf1, 0x4b, 0xf2,
	0x23, 0x52, 0xae, 0xbe, 0xcd, 0xf4, 0xdc, 0x40, 0xd9, 0xd8, 0xdd, 0x17, 0x11, 0x7d, 0xfa, 0xf4,
	0xf9, 0x4e, 0x9f, 0xee, 0x3e, 0xe7, 0xf4, 0xe9, 0x11, 0x94, 0x9c, 0x51, 0x6f, 0x73, 0xe4, 0xd8,
	0x9e, 0x8d, 0x2a, 0xc4, 0xeb, 0xf5, 0x5d, 0xe2, 0x4c, 0x88, 0x33, 0x3a, 0x6d, 0x2e, 0x9f, 0xd9,
	0x67, 0x36, 0xeb, 0x68, 0xd1, 0x5f, 0x9c, 0xa7, 0xd9, 0xa0, 0x3c, 0x2d, 0x63, 0x64, 0xb6, 0x86,
	0x93, 0x5e, 0x6f, 0x74, 0xda, 0xba, 0x98, 0x88, 0x9e, 0xa6, 0xdf, 0x63, 0x8c, 0xbd, 0xf3, 0xd1,
	0x29, 0xfb, 0x23, 0xfa, 0x6e, 0x9d, 0xd9, 0xf6, 0xd9, 0x80, 0xf0, 0x5e, 0xcb, 0xb2, 0x3d, 0xc3,
	0x33, 0x6d, 0xcb, 0xe5, 0xbd, 0xf8, 0xaf, 0x34, 0xa8, 0xe9, 0xc4, 0x1d, 0xd9, 0x96, 0x4b, 0x9e,
	0x12, 0xa3, 0x4f, 0x1c, 0x74, 0x1b, 0xa0, 0x37, 0x18, 0xbb, 0x1e, 0x71, 0x4e, 0xcc, 0x7e, 0x43,
	0x5b, 0xd7, 0x36, 0xe6, 0xf5, 0x92, 0xa0, 0xec, 0xf6, 0xd1, 0x1b, 0x50, 0x1a, 0x92, 0xe1, 0x29,
	0xef, 0xcd, 0xb0, 0xde, 0x22, 0x27, 0xec, 0xf6, 0x51, 0x13, 0x8a, 0x0e, 0x99, 0x98, 0xae, 0x69,
	0x5b, 0x8d, 0xec, 0xba, 0xb6, 0x91, 0xd5, 0xfd, 0x36, 0x1d, 0xe8, 0x18, 0x2f, 0xbd, 0x13, 0x8f,
	0x38, 0xc3, 0xc6, 0x3c, 0x1f, 0x48, 0x09, 0x5d, 0xe2, 0x0c, 0xf1, 0x4f, 0x72, 0x50, 0xd1, 0x0d,
	0xeb, 0x8c, 0xe8, 0xe4, 0x87, 0x63, 0xe2, 0x7a, 0xa8, 0x0e, 0xd9, 0x0b, 0x72, 0xc5, 0xe0, 0x2b,
	0x3a, 0xfd, 0xc9, 0xc7, 0x5b, 0x67, 0xe4, 0x84, 0x58, 0x1c, 0xb8, 0x42, 0xc7, 0x5b, 0x67, 0xa4,
	0x63, 0xf5, 0xd1, 0x32, 0xe4, 0x06, 0xe6, 0xd0, 0xf4, 0x04, 0x2a, 0x6f, 0x84, 0xd4, 0x99, 0x8f,
	0xa8, 0xb3, 0x0d, 0xe0, 0xda, 0x8e, 0x77, 0x62, 0x3b, 0x7d, 0xe2, 0x34, 0x72, 0xeb, 0xda, 0x46,
	0x6d, 0xeb, 0xad, 0x4d, 0x75, 0x19, 0x36, 0x55, 0x85, 0x36, 0x8f, 0x6d, 0xc7, 0x3b, 0xa4, 0xbc,
	0x7a, 0xc9, 0x95, 0x3f, 0xd1, 0x87, 0x50, 0x66, 0x42, 0x3c, 0xc3, 0x39, 0x23, 0x5e, 0x23, 0xcf,
	0xa4, 0xdc, 0xbb, 0x46, 0x4a, 0x97, 0x31, 0xeb, 0x0c, 0x9e, 0xff, 0x46, 0x18, 0x2a, 0x2e, 0x71,
	0x4c, 0x63, 0x60, 0x7e, 0x61, 0x9c, 0x0e, 0x48, 0xa3, 0xb0, 0xae, 0x6d, 0x14, 0xf5, 0x10, 0x8d,
	0xce, 0xff, 0x82, 0x5c, 0xb9, 0x27, 0xb6, 0x35, 0xb8, 0x6a, 0x14, 0x19, 0x43, 0x91, 0x12, 0x0e,
	0xad, 0xc1, 0x15, 0x5b, 0x34, 0x7b, 0x6c, 0x79, 0xbc, 0xb7, 0xc4, 0x7a, 0x4b, 0x8c, 0xc2, 0xba,
	0x37, 0xa0, 0x3e, 0x34, 0xad, 0x93, 0xa1, 0xdd, 0x3f, 0xf1, 0x0d, 0x02, 0xcc, 0x20, 0xb5, 0xa1,
	0x69, 0x3d, 0xb3, 0xfb, 0xba, 0x34, 0x0b, 0xe5, 0x34, 0x2e, 0xc3, 0x9c, 0x65, 0xc1, 0x69, 0x5c,
	0xaa, 0x9c, 0x9b, 0xb0, 0x44, 0x65, 0xf6, 0x1c, 0x62, 0x78, 0x24, 0x60, 0xae, 0x30, 0xe6, 0xc5,
	0xa1, 0x69, 0x6d, 0xb3, 0x9e, 0x10, 0xbf, 0x71, 0x19, 0xe3, 0xaf, 0x0a, 0x7e, 0xe3, 0x32, 0xcc,
	0x8f, 0x37, 0xa1, 0xe4, 0xdb, 0x1c, 0x15, 0x61, 0xfe, 0xe0, 0xf0, 0xa0, 0x53, 0x9f, 0x43, 0x00,
	0xf9, 0xf6, 0xf1, 0x76, 0xe7, 0x60, 0xa7, 0xae, 0xa1, 0x32, 0x14, 0x76, 0x3a, 0xbc, 0x91, 0xc1,
	0x8f, 0x01, 0x02, 0xeb, 0xa2, 0x02, 0x64, 0xf7, 0x3a, 0x9f, 0xd5, 0xe7, 0x28, 0xcf, 0x8b, 0x8e,
	0x7e, 0xbc, 0x7b, 0x78, 0x50, 0xd7, 0xe8, 0xe0, 0x6d, 0xbd, 0xd3, 0xee, 0x76, 0xea, 0x19, 0xca,
	0xf1, 0xec, 0x70, 0xa7, 0x9e, 0x45, 0x25, 0xc8, 0xbd, 0x68, 0xef, 0x3f, 0xef, 0xd4, 0xe7, 0xf1,
	0xcf, 0x35, 0xa8, 0x8a, 0xf5, 0xe2, 0x67, 0x02, 0x7d, 0x07, 0xf2, 0xe7, 0xec, 0x5c, 0xb0, 0xad,
	0x58, 0xde, 0xba, 0x15, 0x59, 0xdc, 0xd0, 0xd9, 0xd1, 0x05, 0x2f, 0xc2, 0x90, 0xbd, 0x98, 0xb8,
	0x8d, 0xcc, 0x7a, 0x76, 0xa3, 0xbc, 0x55, 0xdf, 0xe4, 0xe7, 0x75, 0x73, 0x8f, 0x5c, 0xbd, 0x30,
	0x06, 0x63, 0xa2, 0xd3, 0x4e, 0x84, 0x60, 0x7e, 0x68, 0x3b, 0x84, 0xed, 0xd8, 0xa2, 0xce, 0x7e,
	0xd3, 0x6d, 0xcc, 0x16, 0x4d, 0xec, 0x56, 0xde, 0xc0, 0xbf, 0xd4, 0x00, 0x8e, 0xc6, 0x5e, 0xfa,
	0xd1, 0x58, 0x86, 0xdc, 0x84, 0x0a, 0x16, 0xc7, 0x82, 0x37, 0xd8, 0x99, 0x20, 0x86, 0x4b, 0xfc,
	0x33, 0x41, 0x1b, 0xe8, 0x06, 0x14, 0x46, 0x0e, 0x99, 0x9c, 0x5c, 0x4c, 0x18, 0x48, 0x51, 0xcf,
	0xd3, 0xe6, 0xde, 0x04, 0xbd, 0x09, 0x15, 0xf3, 0xcc, 0xb2, 0x1d, 0x72, 0xc2, 0x65, 0xe5, 0x58,
	0x6f, 0x99, 0xd3, 0x98, 0xde, 0x0a, 0x0b, 0x17, 0x9c, 0x57, 0x59, 0xf6, 0x29, 0x09, 0x5b, 0x50,
	0x66, 0xaa, 0xce, 0x64, 0xbe, 0x77, 0x02, 0x1d, 0x33, 0x6c, 0x58, 0xdc, 0x84, 0x42, 0x6b, 0xfc,
	0x03, 0x40, 0x3b, 0x64, 0x40, 0x3c, 0x32, 0x8b, 0xf7, 0x50, 0x6c, 0x92, 0x55, 0x6d, 0x82, 0x7f,
	0xa6, 0xc1, 0x52, 0x48, 0xfc, 0x4c, 0xd3, 0x6a, 0x40, 0xa1, 0xcf, 0x84, 0x71, 0x0d, 0xb2, 0xba,
	0x6c, 0xa2, 0x87, 0x50, 0x14, 0x0a, 0xb8, 0x8d, 0x6c, 0xca, 0xa6, 0x29, 0x70, 0x9d, 0x5c, 0xfc,
	0xcb, 0x0c, 0x94, 0xc4, 0x44, 0x0f, 0x47, 0xa8, 0x0d, 0x55, 0x87, 0x37, 0x4e, 0xd8, 0x7c, 0x84,
	0x46, 0xcd, 0x74, 0x27, 0xf4, 0x74, 0x4e, 0xaf, 0x88, 0x21, 0x8c, 0x8c, 0x7e, 0x1f, 0xca, 0x52,
	0xc4, 0x68, 0xec, 0x09, 0x93, 0x37, 0xc2, 0x02, 0x82, 0xfd, 0xf7, 0x74, 0x4e, 0x07, 0xc1, 0x7e,
	0x34, 0xf6, 0x50, 0x17, 0x96, 0xe5, 0x60, 0x3e, 0x1b, 0xa1, 0x46, 0x96, 0x49, 0x59, 0x0f, 0x4b,
	0x89, 0x2f, 0xd5, 0xd3, 0x39, 0x1d, 0x89, 0xf1, 0x4a, 0xa7, 0xaa, 0x92, 0x77, 0xc9, 0x9d, 0x77,
	0x4c, 0xa5, 0xee, 0xa5, 0x15, 0x57, 0xa9, 0x7b, 0x69, 0x3d, 0x2e, 0x41, 0x41, 0xb4, 0xf0, 0xbf,
	0x64, 0x00, 0xe4, 0x6a, 0x1c, 0x8e, 0xd0, 0x0e, 0xd4, 0x1c, 0xd1, 0x0a, 0x59, 0xeb, 0x8d, 0x44,
	0x6b, 0x89, 0x45, 0x9c, 0xd3, 0xab, 0x72, 0x10, 0x57, 0xee, 0x7b, 0x50, 0xf1, 0xa5, 0x04, 0x06,
	0xbb, 0x99, 0x60, 0x30, 0x5f, 0x42, 0x59, 0x0e, 0xa0, 0x26, 0xfb, 0x04, 0x56, 0xfc, 0xf1, 0x09,
	0x36, 0x7b, 0x73, 0x8a, 0xcd, 0x7c, 0x81, 0x4b, 0x52, 0x82, 0x6a, 0x35, 0x55, 0xb1, 0xc0, 0x6c,
	0x37, 0x13, 0xcc, 0x16, 0x57, 0x8c, 0x1a, 0x0e, 0x68, 0xbc, 0xe4, 0x4d, 0xfc, 0xbf, 0x59, 0x28,
	0x6c, 0xdb, 0xc3, 0x91, 0xe1, 0xd0, 0xd5, 0xc8, 0x3b, 0xc4, 0x1d, 0x0f, 0x3c, 0x66, 0xae, 0xda,
	0xd6, 0xdd, 0xb0, 0x44, 0xc1, 0x26, 0xff, 0xea, 0x8c, 0x55, 0x17, 0x43, 0xe8, 0x60, 0x11, 0x1e,
	0x33, 0xaf, 0x31, 0x58, 0x04, 0x47, 0x31, 0x44, 0x1e, 0xe4, 0x6c, 0x70, 0x90, 0x9b, 0x50, 0x98,
	0x10, 0x27, 0x08, 0xe9, 0x4f, 0xe7, 0x74, 0x49, 0x40, 0xef, 0xc0, 0x42, 0x34, 0xbc, 0xe4, 0x04,
	0x4f, 0xad, 0x17, 0x8e, 0x46, 0x77, 0xa1, 0x12, 0x8a, 0x71, 0x79, 0xc1, 0x57, 0x1e, 0x2a, 0x21,
	0x6e, 0x55, 0xfa, 0x55, 0x1a, 0x8f, 0x2b, 0x4f, 0xe7, 0xa4, 0x67, 0x5d, 0x95, 0x9e, 0xb5, 0x28,
	0x46, 0x09, 0xdf, 0x1a, 0x72, 0x32, 0xdf, 0x0f, 0x3b, 0x19, 0xfc, 0x7d, 0xa8, 0x86, 0x0c, 0x44,
	0xe3, 0x4e, 0xe7, 0xe3, 0xe7, 0xed, 0x7d, 0x1e, 0xa4, 0x9e, 0xb0, 0xb8, 0xa4, 0xd7, 0x35, 0x1a,
	0xeb, 0xf6, 0x3b, 0xc7, 0xc7, 0xf5, 0x0c, 0xaa, 0x42, 0xe9, 0xe0, 0xb0, 0x7b, 0xc2, 0xb9, 0xb2,
	0xf8, 0x89, 0x2f, 0x41, 0x04, 0x39, 0x25, 0xb6, 0xcd, 0x29, 0xb1, 0x4d, 0x93, 0xb1, 0x2d, 0x13,
	0xc4, 0x36, 0x16, 0xe6, 0xf6, 0x3b, 0xed, 0xe3, 0x4e, 0x7d, 0xfe, 0x71, 0x0d, 0x2a, 0xdc, 0xbe,
	0x27, 0x63, 0x8b, 0x86, 0xda, 0x7f, 0xd2, 0x00, 0x82, 0xd3, 0x84, 0x5a, 0x50, 0xe8, 0x71, 0x9c,
	0x86, 0xc6, 0x9c, 0xd1, 0x4a, 0xe2, 0x92, 0xe9, 0x92, 0x0b, 0x7d, 0x0b, 0x0a, 0xee, 0xb8, 0xd7,
	0x23, 0xae, 0x0c, 0x79, 0x37, 0xa2, 0xfe, 0x50, 0x78, 0x2b, 0x5d, 0xf2, 0xd1, 0x21, 0x2f, 0x0d,
	0x73, 0x30, 0x66, 0x01, 0x70, 0xfa, 0x10, 0xc1, 0x87, 0xff, 0x5e, 0x83, 0xb2, 0xb2, 0x79, 0x7f,
	0x47, 0x27, 0x7c, 0x0b, 0x4a, 0x4c, 0x07, 0xd2, 0x17, 0x6e, 0xb8, 0xa8, 0x07, 0x04, 0xf4, 0x7b,
	0x50, 0x92, 0x27, 0x40, 0x7a, 0xe2, 0x46, 0xb2, 0xd8, 0xc3, 0x91, 0x1e, 0xb0, 0xe2, 0x3d, 0x58,
	0x64, 0x56, 0xe9, 0xd1, 0xe4, 0x5a, 0xda, 0x51, 0x4d, 0x3f, 0xb5, 0x48, 0xfa, 0xd9, 0x84, 0xe2,
	0xe8, 0xfc, 0xca, 0x35, 0x7b, 0xc6, 0x40, 0x68, 0xe1, 0xb7, 0xf1, 0x47, 0x80, 0x54, 0x61, 0xb3,
	0x4c, 0x17, 0x57, 0xa1, 0xfc, 0xd4, 0x70, 0xcf, 0x85, 0x4a, 0xf8, 0x21, 0x54, 0x69, 0x73, 0xef,
	0xc5, 0x6b, 0xe8, 0xc8, 0x2e, 0x07, 0x92, 0x7b, 0x26, 0x9b, 0x23, 0x98, 0x3f, 0x37, 0xdc, 0x73,
	0x36, 0xd1, 0xaa, 0xce, 0x7e, 0xa3, 0x77, 0xa0, 0xde, 0xe3, 0x93, 0x3c, 0x89, 0x5c, 0x19, 0x16,
	0x04, 0xdd, 0xcf, 0x04, 0x3f, 0x85, 0x0a, 0x9f, 0xc3, 0x57, 0xad, 0x04, 0x5e, 0x84, 0x85, 0x63,
	0xcb, 0x18, 0xb9, 0xe7, 0xb6, 0x8c, 0x6e, 0x74, 0xd2, 0xf5, 0x80, 0x36, 0x13, 0xe2, 0xdb, 0xb0,
	0xe0, 0x90, 0xa1, 0x61, 0x5a, 0xa6, 0x75, 0x76, 0x72, 0x7a, 0xe5, 0x11, 0x57, 0x5c, 0x98, 0x6a,
	0x3e, 0xf9, 0x31, 0xa5, 0x52, 0xd5, 0x4e, 0x07, 0xf6, 0xa9, 0x70, 0x73, 0xec, 0x37, 0xfe, 0x69,
	0x06, 0x2a, 0x9f, 0x18, 0x5e, 0x4f, 0x2e, 0x1d, 0xda, 0x85, 0x9a, 0xef, 0xdc, 0x18, 0x45, 0xe8,
	0x12, 0x09, 0xb1, 0x6c, 0x8c, 0x4c, 0xa5, 0x65, 0x74, 0xac, 0xf6, 0x54, 0x02, 0x13, 0x65, 0x58,
	0x3d, 0x32, 0xf0, 0x45, 0x65, 0xd2, 0x45, 0x31, 0x46, 0x55, 0x94, 0x4a, 0x40, 0x87, 0x50, 0x1f,
	0x39, 0xf6, 0x99, 0x43, 0x5c, 0xd7, 0x17, 0xc6, 0xc3, 0x18, 0x4e, 0x10, 0x76, 0x24, 0x58, 0x03,
	0x71, 0x0b, 0xa3, 0x30, 0xe9, 0xf1, 0x42, 0x90, 0xcf, 0x70, 0xe7, 0xf4, 0x9f, 0x19, 0x40, 0xf1,
	0x49, 0xfd, 0xb6, 0x29, 0xde, 0x3d, 0xa8, 0xb9, 0x9e, 0xe1, 0xc4, 0x36, 0x5b, 0x95, 0x51, 0x7d,
	0x8f, 0xff, 0x36, 0xf8, 0x0a, 0x9d, 0x58, 0xb6, 0x67, 0xbe, 0xbc, 0x12, 0x59, 0x72, 0x4d, 0x92,
	0x0f, 0x18, 0x15, 0x75, 0xa0, 0xf0, 0xd2, 0x1c, 0x78, 0xc4, 0x71, 0x1b, 0xb9, 0xf5, 0xec, 0x46,
	0x6d, 0xeb, 0xe1, 0x75, 0xcb, 0xb0, 0xf9, 0x21, 0xe3, 0xef, 0x5e, 0x8d, 0x88, 0x2e, 0xc7, 0xaa,
	0x99, 0x67, 0x3e, 0x94, 0x8d, 0xdf, 0x84, 0xe2, 0x2b, 0x2a, 0x82, 0xde, 0xb2, 0x0b, 0x3c, 0x59,
	0x64, 0x6d, 0x7e, 0xc9, 0x7e, 0xe9, 0x18, 0x67, 0x43, 0x62, 0x79, 0xf2, 0x1e, 0x28, 0xdb, 0xf8,
	0x1e, 0x40, 0x00, 0x43, 0x5d, 0xfe, 0xc1, 0xe1, 0xd1, 0xf3, 0x6e, 0x7d, 0x0e, 0x55, 0xa0, 0x78,
	0x70, 0xb8, 0xd3, 0xd9, 0xef, 0xd0, 0xf8, 0x80, 0x5b, 0xd2, 0xa4, 0xa1, 0xb5, 0x54, 0x31, 0xb5,
	0x10, 0x26, 0x5e, 0x85, 0xe5, 0xa4, 0x05, 0xa4, 0xb9, 0x68, 0x55, 0xec, 0xd2, 0x99, 0x8e, 0x8a,
	0x0a, 0x9d, 0x09, 0x4f, 0xb7, 0x01, 0x05, 0xbe, 0x7b, 0xfb, 0x22, 0x39, 0x97, 0x4d, 0x6a, 0x08,
	0xbe, 0x19, 0x49, 0x5f, 0xac, 0x92, 0xdf, 0x4e, 0x74, 0x2f, 0xb9, 0x44, 0xf7, 0x82, 0xee, 0x42,
	0xd5, 0x3f, 0x0d, 0x86, 0x2b, 0x72, 0x81, 0x92, 0x5e, 0x91, 0x1b, 0x9d, 0xd2, 0x42, 0x46, 0x2f,
	0x84, 0x8d, 0x8e, 0xee, 0x41, 0x9e, 0x4c, 0x88, 0xe5, 0xb9, 0x8d, 0x32, 0x8b, 0x18, 0x55, 0x99,
	0xbb, 0x77, 0x28, 0x55, 0x17, 0x9d, 0xf8, 0xbb, 0xb0, 0xc8, 0xee, 0x48, 0x4f, 0x1c, 0xc3, 0x52,
	0x2f, 0x73, 0xdd, 0xee, 0xbe, 0x30, 0x37, 0xfd, 0x89, 0x6a, 0x90, 0xd9, 0xdd, 0x11, 0x46, 0xc8,
	0xec, 0xee, 0xe0, 0x1f, 0x6b, 0x80, 0xd4, 0x71, 0x33, 0xd9, 0x39, 0x22, 0x5c, 0xc2, 0x67, 0x03,
	0xf8, 0x65, 0xc8, 0x11, 0xc7, 0xb1, 0x1d, 0x66, 0xd1, 0x92, 0xce, 0x1b, 0xf8, 0x2d, 0xa1, 0x83,
	0x4e, 0x26, 0xf6, 0x85, 0x7f, 0x06, 0xb9, 0x34, 0xcd, 0x57, 0x75, 0x0f, 0x96, 0x42, 0x5c, 0x33,
	0x45, 0xae, 0x0f, 0x61, 0x81, 0x09, 0xdb, 0x3e, 0x27, 0xbd, 0x8b, 0x91, 0x6d, 0x5a, 0x31, 0x3c,
	0xba, 0x72, 0x81, 0x83, 0xa5, 0xf3, 0xe0, 0x13, 0xab, 0xf8, 0xc4, 0x6e, 0x77, 0x1f, 0x7f, 0x06,
	0xab, 0x11, 0x39, 0x52, 0xfd, 0x3f, 0x82, 0x72, 0xcf, 0x27, 0xba, 0x22, 0xd7, 0xb9, 0x1d, 0x56,
	0x2e, 0x3a, 0x54, 0x1d, 0x81, 0x0f, 0xe1, 0x46, 0x4c, 0xf4, 0x4c, 0x73, 0x7e, 0x1b, 0x56, 0x98,
	0xc0, 0x3d, 0x42, 0x46, 0xed, 0x81, 0x39, 0x49, 0xb5, 0xf4, 0x48, 0x4c, 0x4a, 0x61, 0xfc, 0x7a,
	0xf7, 0x05, 0xfe, 0x03, 0x81, 0xd8, 0x35, 0x87, 0xa4, 0x6b, 0xef, 0xa7, 0xeb, 0x46, 0xa3, 0xd9,
	0x05, 0xb9, 0x72, 0x45, 0x5a, 0xc3, 0x7e, 0xe3, 0x7f, 0xd6, 0x84, 0xa9, 0xd4, 0xe1, 0x5f, 0xf3,
	0x4e, 0x5e, 0x03, 0x38, 0xa3, 0x47, 0x86, 0xf4, 0x69, 0x07, 0xaf, 0xa8, 0x28, 0x14, 0x5f, 0x4f,
	0xea, 0xbf, 0x2b, 0x42, 0xcf, 0x65, 0xb1, 0xcf, 0xd9, 0x3f, 0xbe, 0x97, 0xbb, 0x0d, 0x65, 0x46,
	0x38, 0xf6, 0x0c, 0x6f, 0xec, 0xc6, 0x16, 0xe3, 0x2f, 0xc4, 0xb6, 0x97, 0x83, 0x66, 0x9a, 0xd7,
	0xb7, 0x20, 0xcf, 0x2e, 0x13, 0x32, 0x95, 0xbe, 0x99, 0xb0, 0x1f, 0xb9, 0x1e, 0xba, 0x60, 0xc4,
	0x3f, 0xd5, 0x20, 0xff, 0x8c, 0x95, 0x60, 0x15, 0xd5, 0xe6, 0xe5, 0x5a, 0x58, 0xc6, 0x90, 0x17,
	0x86, 0x4a, 0x3a, 0xfb, 0xcd, 0x52, 0x4f, 0x42, 0x9c, 0xe7, 0xfa, 0x3e, 0x4f, 0x71, 0x4b, 0xba,
	0xdf, 0xa6, 0x36, 0xeb, 0x0d, 0x4c, 0x62, 0x79, 0xac, 0x77, 0x9e, 0xf5, 0x2a, 0x14, 0x9a, 0x3d,
	0x9b, 0xee, 0x3e, 0x31, 0x1c, 0x4b, 0x14, 0x4d, 0x8b, 0x7a, 0x40, 0xc0, 0xfb, 0x50, 0xe7, 0x7a,
	0xb4, 0xfb, 0x7d, 0x25, 0xc1, 0xf4, 0xd1, 0xb4, 0x08, 0x5a, 0x48, 0x5a, 0x26, 0x2a, 0xed, 0x17,
	0x1a, 0x2c, 0x2a, 0xe2, 0x66, 0xb2, 0xea, 0xbb, 0x90, 0xe7, 0x45, 0x6a, 0x91, 0xe9, 0x2c, 0x87,
	0x47, 0x71, 0x18, 0x5d, 0xf0, 0xa0, 0x4d, 0x28, 0xf0, 0x5f, 0xf2, 0x0e, 0x90, 0xcc, 0x2e, 0x99,
	0xf0, 0x3d, 0x58, 0x12, 0x24, 0x32, 0xb4, 0x93, 0x0e, 0x06, 0x5b, 0x0c, 0xfc, 0x67, 0xb0, 0x1c,
	0x66, 0x9b, 0x69, 0x4a, 0x8a, 0x92, 0x99, 0xd7, 0x51, 0xb2, 0x2d, 0x95, 0x7c, 0x3e, 0xea, 0x2b,
	0x79, 0x54, 0x74, 0xc7, 0xa8, 0xeb, 0x95, 0x09, 0xaf, 0x57, 0x30, 0x01, 0x29, 0xe2, 0x1b, 0x9d,
	0xc0, 0x07, 0x72, 0x3b, 0xec, 0x9b, 0xae, 0xef, 0xc3, 0x31, 0x54, 0x06, 0xa6, 0x45, 0x0c, 0x47,
	0x54, 0xce, 0x35, 0x5e, 0x39, 0x57, 0x69, 0xf8, 0x0b, 0x40, 0xea, 0xc0, 0x6f, 0x54, 0xe9, 0xfb,
	0xd2, 0x64, 0x47, 0x8e, 0x3d, 0xb4, 0x53, 0xcd, 0x8e, 0xff, 0x1c, 0x56, 0x22, 0x7c, 0xdf, 0xa8,
	0x9a, 0x4b, 0xb0, 0xb8, 0x43, 0x64, 0x42, 0x23, 0xdd, 0xde, 0x47, 0x80, 0x54, 0xe2, 0x4c, 0x91,
	0xad, 0x05, 0x8b, 0xcf, 0xec, 0x09, 0x75, 0x91, 0x94, 0x1a, 0xf8, 0x06, 0x5e, 0x87, 0xf0, 0x4d,
	0xe1, 0xb7, 0x29, 0xb8, 0x3a, 0x60, 0x26, 0xf0, 0x7f, 0xd7, 0xa0, 0xd2, 0x1e, 0x18, 0xce, 0x50,
	0x02, 0x7f, 0x0f, 0xf2, 0xfc, 0x76, 0x2d, 0x0a, 0x5a, 0xf7, 0xc3, 0x62, 0x54, 0x5e, 0xde, 0x68,
	0xf3, 0xbb, 0xb8, 0x18, 0x45, 0x15, 0x17, 0x6f, 0x5e, 0x3b, 0x91, 0x37, 0xb0, 0x1d, 0xf4, 0x1e,
	0xe4, 0x0c, 0x3a, 0x84, 0x85, 0xa2, 0x5a, 0xb4, 0xae, 0xc1, 0xa4, 0xb1, 0x3b, 0x00, 0xe7, 0xc2,
	0xdf, 0x81, 0xb2, 0x82, 0x80, 0x0a, 0x90, 0x7d, 0xd2, 0x11, 0x09, 0x7b, 0x7b, 0xbb, 0xbb, 0xfb,
	0x82, 0x17, 0x74, 0x6a, 0x00, 0x3b, 0x1d, 0xbf, 0x9d, 0xc1, 0x9f, 0x8a, 0x51, 0xc2, 0xed, 0xab,
	0xfa, 0x68, 0x69, 0xfa, 0x64, 0x5e, 0x4b, 0x9f, 0x4b, 0xa8, 0x8a, 0xe9, 0xcf, 0x1a, 0xc6, 0x98,
	0xbc, 0x94, 0x30, 0xa6, 0x28, 0xaf, 0x0b, 0x46, 0xfc, 0x2b, 0x0d, 0xea, 0x3b, 0xf6, 0x2b, 0xeb,
	0xcc, 0x31, 0xfa, 0xfe, 0x39, 0xf9, 0x30, 0xb2, 0x52, 0x9b, 0x91, 0xe2, 0x68, 0x84, 0x3f, 0x20,
	0x44, 0x56, 0xac, 0x11, 0x94, 0x0d, 0x79, 0x2c, 0x94, 0x4d, 0xfc, 0x01, 0x2c, 0x44, 0x06, 0x51,
	0xdb, 0xbf, 0x68, 0xef, 0xef, 0xee, 0x50, 0x5b, 0xb3, 0xc2, 0x5a, 0xe7, 0xa0, 0xfd, 0x78, 0xbf,
	0x23, 0x1e, 0x90, 0xda, 0x07, 0xdb, 0x9d, 0xfd, 0x7a, 0x06, 0xf7, 0x60, 0x51, 0x81, 0x9f, 0xf5,
	0x65, 0x20, 0x45, 0xbb, 0x05, 0xa8, 0x8a, 0x68, 0x2f, 0x0e, 0xe5, 0xbf, 0x65, 0xa0, 0x26, 0x29,
	0x5f, 0x0f, 0x26, 0x5a, 0x85, 0x7c, 0xff, 0xf4, 0xd8, 0xfc, 0x42, 0xbe, 0x1c, 0x89, 0x16, 0xa5,
	0x0f, 0x38, 0x0e, 0x7f, 0xbe, 0x15, 0x2d, 0x1a, 0xc6, 0x1d, 0xe3, 0xa5, 0xb7, 0x6b, 0xf5, 0xc9,
	0x25, 0x4b, 0x0a, 0xe6, 0xf5, 0x80, 0xc0, 0x2a, 0x4c, 0xe2, 0x99, 0x97, 0xdd, 0xac, 0x94, 0x67,
	0x5f, 0xf4, 0x00, 0xea, 0xf4, 0x77, 0x7b, 0x34, 0x1a, 0x98, 0xa4, 0xcf, 0x05, 0x14, 0x18, 0x4f,
	0x8c, 0x4e, 0xd1, 0xd9, 0x5d, 0xc4, 0x6d, 0x14, 0x59, 0x58, 0x12, 0x2d, 0xb4, 0x0e, 0x65, 0xae,
	0xdf, 0xae, 0xf5, 0xdc, 0x25, 0xec, 0xed, 0x33, 0xab, 0xab, 0xa4, 0x70, 0x9a, 0x01, 0xd1, 0x34,
	0x63, 0x09, 0x16, 0xdb, 0x63, 0xef, 0xbc, 0x63, 0xd1, 0x58, 0x21, 0xad, 0xbc, 0x0c, 0x88, 0x12,
	0x77, 0x4c, 0x57, 0xa5, 0x0a, 0xd6, 0xf0, 0x82, 0x74, 0x60, 0x89, 0x12, 0x89, 0xe5, 0x99, 0x3d,
	0x25, 0xae, 0xca, 0xcc, 0x4b, 0x8b, 0x64, 0x5e, 0x86, 0xeb, 0xbe, 0xb2, 0x9d, 0xbe, 0xb0, 0xb9,
	0xdf, 0xc6, 0xff, 0xa8, 0x71, 0xc8, 0xe7, 0x6e, 0x28, 0x7d, 0xfa, 0x2d, 0xc5, 0xa0, 0xf7, 0xa1,
	0x60, 0x8f, 0xd8, 0x0b, 0xbf, 0x28, 0xc3, 0xac, 0x6e, 0xf2, 0x6f, 0x02, 0x36, 0x85, 0xe0, 0x43,
	0xde, 0xab, 0x4b, 0x36, 0x74, 0x1f, 0x6a, 0xe7, 0x86, 0x7b, 0x4e, 0xfa, 0x47, 0x52, 0x26, 0xbf,
	0xf9, 0x45, 0xa8, 0x78, 0x23, 0xd0, 0xef, 0x09, 0xf1, 0xa6, 0xe8, 0x87, 0x1f, 0xc2, 0x8a, 0xe4,
	0x14, 0xaf, 0x13, 0x53, 0x98, 0x5f, 0xc1, 0x6d, 0xc9, 0xbc, 0x7d, 0x6e, 0x58, 0x67, 0x44, 0x02,
	0xfe, 0xae, 0x16, 0x88, 0xcf, 0x27, 0x9b, 0x38, 0x9f, 0xc7, 0xd0, 0xf0, 0xe7, 0xc3, 0x6e, 0xd6,
	0xf6, 0x40, 0x55, 0x74, 0xec, 0x8a, 0xf3, 0x54, 0xd2, 0xd9, 0x6f, 0x4a, 0x73, 0xec, 0x81, 0x9f,
	0x4a, 0xd3, 0xdf, 0x78, 0x1b, 0x6e, 0x4a, 0x19, 0xe2, 0xce, 0x1b, 0x16, 0x12, 0x53, 0x3c, 0x49,
	0x88, 0x30, 0x2c, 0x1d, 0x3a, 0x7d, 0xe1, 0x55, 0xce, 0xf0, 0x12, 0x30, 0x99, 0x9a, 0x22, 0x73,
	0x85, 0x6f, 0x4a, 0xaa, 0x98, 0x92, 0x2d, 0x49, 0x32, 0x15, 0xa0, 0x92, 0xc5, 0x82, 0x51, 0x72,
	0x6c, 0xc1, 0x62, 0xa2, 0x7f, 0x00, 0x6b, 0xbe, 0x12, 0xd4, 0x6e, 0x47, 0xc4, 0x19, 0x9a, 0xae,
	0xab, 0xd4, 0xbd, 0x93, 0x26, 0x7e, 0x1f, 0xe6, 0x47, 0x44, 0x04, 0xa1, 0xf2, 0x16, 0x92, 0x9b,
	0x52, 0x19, 0xcc, 0xfa, 0x71, 0x1f, 0xee, 0x48, 0xe9, 0xdc, 0xa2, 0x89, 0xe2, 0xa3, 0x4a, 0xc9,
	0x6a, 0x60, 0x26, 0xa5, 0x1a, 0x98, 0x8d, 0xbc, 0xc5, 0x7c, 0xc4, 0x0d, 0x29, 0xcf, 0xfc, 0x4c,
	0xc9, 0xc5, 0x1e, 0xb7, 0xa9, 0xef, 0x2a, 0x66, 0x12, 0xf6, 0xd7, 0xc2, 0x0b, 0x7c, 0x55, 0x1e,
	0x9e, 0xb0, 0x19, 0xca, 0x87, 0x0e, 0xd9, 0xa4, 0x59, 0x33, 0x5d, 0x00, 0x5d, 0xad, 0x85, 0xce,
	0xeb, 0x21, 0x1a, 0x3e, 0x85, 0xe5, 0xb0, 0x5f, 0x9b, 0x49, 0x97, 0x65, 0xc8, 0x79, 0xf6, 0x05,
	0x91, 0xb1, 0x86, 0x37, 0xa4, 0xed, 0x7c, 0x9f, 0x37, 0x93, 0xed, 0x8c, 0x40, 0x18, 0x3b, 0x1d,
	0xb3, 0xea, 0x4b, 0x37, 0x96, 0xbc, 0x03, 0xf1, 0x06, 0x3e, 0x80, 0xd5, 0xa8, 0x67, 0x9b, 0x49,
	0xe5, 0x17, 0xfc, 0x2c, 0x25, 0x39, 0xbf, 0x99, 0xe4, 0x7e, 0x1c, 0xf8, 0x25, 0xc5, 0xb7, 0xcd,
	0x24, 0x52, 0x87, 0x66, 0x92, 0xab, 0xfb, 0x2a, 0x8e, 0x8e, 0xef, 0xf9, 0x66, 0x12, 0xe6, 0x06,
	0xc2, 0x66, 0x5f, 0xfe, 0xc0, 0x5d, 0x65, 0xa7, 0xba, 0x2b, 0x71, 0x48, 0x02, 0x87, 0xfa, 0x35,
	0x6c, 0x3a, 0x81, 0x11, 0xf8, 0xf2, 0x59, 0x31, 0x68, 0x38, 0xf3, 0x31, 0x58, 0x43, 0x6e, 0x6c,
	0x35, 0x02, 0xcc, 0xb4, 0x18, 0x9f, 0x04, 0x6e, 0x3c, 0x16, 0x24, 0x66, 0x12, 0xfc, 0x29, 0xac,
	0xa7, 0xc7, 0x87, 0x59, 0x24, 0x3f, 0x68, 0x41, 0xc9, 0xbf, 0x0c, 0x29, 0xdf, 0x9b, 0x95, 0xa1,
	0x70, 0x70, 0x78, 0x7c, 0xd4, 0xde, 0xee, 0xf0, 0x0f, 0xce, 0xb6, 0x0f, 0x75, 0xfd, 0xf9, 0x51,
	0xb7, 0x9e, 0xd9, 0xfa, 0x75, 0x16, 0x32, 0x7b, 0x2f, 0xd0, 0x67, 0x90, 0xe3, 0x5f, 0x5f, 0x4c,
	0xf9, 0xe4, 0xa6, 0x39, 0xed, 0x03, 0x13, 0x7c, 0xe3, 0xc7, 0xff, 0xf5, 0xeb, 0x9f, 0x67, 0x16,
	0x71, 0xa5, 0x35, 0xf9, 0x76, 0xeb, 0x62, 0xd2, 0x62, 0x61, 0xea, 0x91, 0xf6, 0x00, 0x7d, 0x0c,
	0xd9, 0xa3, 0xb1, 0x87, 0x52, 0x3f, 0xc5, 0x69, 0xa6, 0x7f, 0x73, 0x82, 0x57, 0x98, 0xd0, 0x05,
	0x0c, 0x42, 0xe8, 0x68, 0xec, 0x51, 0x91, 0x3f, 0x84, 0xb2, 0xfa, 0xc5, 0xc8, 0xb5, 0xdf, 0xe7,
	0x34, 0xaf, 0xff, 0x1a, 0x05, 0xdf, 0x66, 0x50, 0x37, 0x30, 0x12, 0x50, 0xfc, 0x9b, 0x16, 0x75,
	0x16, 0xdd, 0x4b, 0x0b, 0xa5, 0x7e, 0xbd, 0xd3, 0x4c, 0xff, 0x40, 0x25, 0x36, 0x0b, 0xef, 0xd2,
	0xa2, 0x22, 0xff, 0x44, 0x7c, 0x9b, 0xd2, 0xf3, 0xd0, 0x9d, 0x84, 0x6f, 0x13, 0xd4, 0x57, 0xf8,
	0xe6, 0x7a, 0x3a, 0x83, 0x00, 0xb9, 0xc5, 0x40, 0x56, 0xf1, 0xa2, 0x00, 0xe9, 0xf9, 0x2c, 0x8f,
	0xb4, 0x07, 0x5b, 0x3d, 0xc8, 0xb1, 0x17, 0x2e, 0xf4, 0xb9, 0xfc, 0xd1, 0x4c, 0x78, 0xea, 0x4b,
	0x59, 0xe8, 0xd0, 0xdb, 0x18, 0x5e, 0x66, 0x40, 0x35, 0x5c, 0xa2, 0x40, 0xec, 0x7d, 0xeb, 0x91,
	0xf6, 0x60, 0x43, 0x7b, 0x5f, 0xdb, 0xfa, 0x55, 0x0e, 0x72, 0xac, 0xb4, 0x8b, 0x2e, 0x00, 0x82,
	0xd7, 0x9e, 0xe8, 0xec, 0x62, 0xef, 0x47, 0xd1, 0xd9, 0xc5, 0x1f, 0x8a, 0x70, 0x93, 0x81, 0x2e,
	0xe3, 0x05, 0x0a, 0xca, 0x2a, 0xc6, 0x2d, 0x56, 0x04, 0xa7, 0x76, 0xfc, 0x1b, 0x4d, 0x54, 0xb6,
	0xf9, 0x59, 0x42, 0x49, 0xd2, 0x42, 0x4f, 0x3e, 0xd1, 0xed, 0x90, 0xf0, 0xdc, 0x83, 0xbf, 0xcb,
	0x00, 0x5b, 0xb8, 0x1e, 0x00, 0x3a, 0x8c, 0xe3, 0x91, 0xf6, 0xe0, 0xf3, 0x06, 0x5e, 0x12, 0x56,
	0x8e, 0xf4, 0xa0, 0x1f, 0x41, 0x2d, 0xfc, 0xa4, 0x81, 0xee, 0x26, 0x60, 0x45, 0x5f, 0x46, 0x9a,
	0x6f, 0x4d, 0x67, 0x12, 0x3a, 0xad, 0x31, 0x9d, 0x04, 0x38, 0x47, 0xbe, 0x20, 0x64, 0x64, 0x50,
	0x26, 0xb1, 0x06, 0xe8, 0x1f, 0x34, 0xf1, 0xe2, 0x14, 0xbc, 0x51, 0xa0, 0x24, 0xe9, 0xb1, 0x17,
	0x90, 0xe6, 0xbd, 0x6b, 0xb8, 0x84, 0x12, 0x7f, 0xc8, 0x94, 0xf8, 0x00, 0x2f, 0x07, 0x4a, 0x78,
	0xe6, 0x90, 0x78, 0xb6, 0xd0, 0xe2, 0xf3, 0x5b, 0xf8, 0x46, 0xc8, 0x38, 0xa1, 0xde, 0x60, 0xb1,
	0xf8, 0x3b, 0x43, 0xe2, 0x62, 0x85, 0xde, 0x2d, 0x12, 0x17, 0x2b, 0xfc, 0x48, 0x91, 0xb4, 0x58,
	0xfc, 0x55, 0x21, 0x69, 0xb1, 0xfc, 0x9e, 0xad, 0xff, 0x9b, 0x87, 0xc2, 0x36, 0xff, 0x26, 0x1c,
	0xd9, 0x50, 0xf2, 0xcb, 0xf4, 0x68, 0x2d, 0xa9, 0xce, 0x18, 0x5c, 0x6b, 0x9a, 0x77, 0x52, 0xfb,
	0x85, 0x42, 0x6f, 0x32, 0x85, 0xde, 0xc0, 0xab, 0x14, 0x59, 0x7c, 0x76, 0xde, 0xe2, 0xc5, 0xac,
	0x96, 0xd1, 0xef, 0x53, 0x43, 0xfc, 0x29, 0x54, 0xd4, 0x3a, 0x3a, 0x7a, 0x33, 0xb1, 0xb6, 0xa9,
	0x96, 0xe2, 0x9b, 0x78, 0x1a, 0x8b, 0x40, 0x7e, 0x8b, 0x21, 0xaf, 0xe1, 0x9b, 0x09, 0xc8, 0x0e,
	0x63, 0x0d, 0x81, 0xf3, 0x1a, 0x78, 0x32, 0x78, 0xa8, 0xc4, 0x9e, 0x0c, 0x1e, 0x2e, 0xa1, 0x4f,
	0x05, 0x1f, 0x33, 0x56, 0x0a, 0xee, 0x02, 0x04, 0x95, 0x6c, 0x94, 0x68, 0x4b, 0xe5, 0x5e, 0x17,
	0x75, 0x0e, 0xf1, 0x22, 0x38, 0xc6, 0x0c, 0x56, 0xec, 0xbb, 0x08, 0xec, 0xc0, 0x74, 0x3d, 0x7e,
	0x30, 0xab, 0xa1, 0xd2, 0x34, 0x4a, 0x9c, 0x4f, 0xb8, 0xbe, 0xdd, 0xbc, 0x3b, 0x95, 0x47, 0xa0,
	0xdf, 0x63, 0xe8, 0x77, 0x70, 0x33, 0x01, 0x7d, 0xc4, 0x79, 0xe9, 0x66, 0xfb, 0xff, 0x3c, 0x94,
	0x9f, 0x19, 0xa6, 0xe5, 0x11, 0xcb, 0xb0, 0x7a, 0x04, 0x9d, 0x42, 0x8e, 0x45, 0xea, 0xa8, 0x23,
	0x56, 0xcb, 0xb6, 0x51, 0x47, 0x1c, 0xaa, 0x69, 0xe2, 0x75, 0x06, 0xdc, 0xc4, 0x2b, 0x14, 0x78,
	0x18, 0x88, 0x6e, 0xb1, 0x52, 0x24, 0x9d, 0xf4, 0x4b, 0xc8, 0x8b, 0xd7, 0xbe, 0x88, 0xa0, 0x50,
	0xf1, 0xa7, 0x79, 0x2b, 0xb9, 0x33, 0x69, 0x2f, 0xab, 0x30, 0x2e, 0xe3, 0xa3, 0x38, 0x13, 0x80,
	0xa0, 0xc6, 0x1e, 0x5d, 0xd1, 0x58, 0x49, 0xbe, 0xb9, 0x9e, 0xce, 0x90, 0x64, 0x53, 0x15, 0xb3,
	0xef, 0xf3, 0x52, 0xdc, 0x3f, 0x86, 0xf9, 0xa7, 0x86, 0x7b, 0x8e, 0x22, 0xb1, 0x57, 0xf9, 0x56,
	0xac, 0xd9, 0x4c, 0xea, 0x12, 0x28, 0x77, 0x18, 0xca, 0x4d, 0xee, 0xca, 0x54, 0x94, 0x73, 0xc3,
	0xa5, 0x41, 0x0d, 0xf5, 0x21, 0xcf, 0x3f, 0x1d, 0x8b, 0xda, 0x2f, 0xf4, 0xf9, 0x59, 0xd4, 0x7e,
	0xe1, 0xaf, 0xcd, 0xae, 0x47, 0x19, 0x41, 0x51, 0x7e, 0xab, 0x85, 0x22, 0x0f, 0xf7, 0x91, 0xef,
	0xba, 0x9a, 0x6b, 0x69, 0xdd, 0x02, 0xeb, 0x2e, 0xc3, 0xba, 0x8d, 0x1b, 0xb1, 0xb5, 0x12, 0x9c,
	0x8f, 0xb4, 0x07, 0xef, 0x6b, 0xe8, 0x47, 0x00, 0xc1, 0xb3, 0x44, 0xec, 0x04, 0x46, 0x5f, 0x38,
	0x62, 0x27, 0x30, 0xf6, 0xa2, 0x81, 0x37, 0x19, 0xee, 0x06, 0xbe, 0x1b, 0xc5, 0xf5, 0x1c, 0xc3,
	0x72, 0x5f, 0x12, 0xe7, 0x3d, 0x5e, 0x65, 0x75, 0xcf, 0xcd, 0x11, 0x9d, 0xb2, 0x03, 0x25, 0xbf,
	0xea, 0x1c, 0xf5, 0xb6, 0xd1, 0x6a, 0x78, 0xd4, 0xdb, 0xc6, 0xca, 0xd5, 0x61, 0xb7, 0x13, 0xda,
	0x2d, 0x92, 0x95, 0x1e, 0xc0, 0x5f, 0xd4, 0x61, 0x9e, 0x66, 0xdd, 0x34, 0x39, 0x09, 0xea, 0x26,
	0xd1, 0xd9, 0xc7, 0xaa, 0xa8, 0xd1, 0xd9, 0xc7, 0x4b, 0x2e, 0xe1, 0xe4, 0x84, 0x5e, 0xb2, 0x5a,
	0xbc, 0x44, 0x41, 0x67, 0x6a, 0x43, 0x59, 0x29, 0xac, 0xa0, 0x04, 0x61, 0xe1, 0xf2, 0x6c, 0x34,
	0xdc, 0x25, 0x54, 0x65, 0xf0, 0x1b, 0x0c, 0x6f, 0x85, 0x87, 0x3b, 0x86, 0xd7, 0xe7, 0x1c, 0x14,
	0x50, 0xcc, 0x4e, 0x9c, 0xfb, 0x84, 0xd9, 0x85, 0xcf, 0xfe, 0x7a, 0x3a, 0x43, 0xea, 0xec, 0x82,
	0x83, 0xff, 0x0a, 0x2a, 0x6a, 0x79, 0x05, 0x25, 0x28, 0x1f, 0x29, 0x29, 0x47, 0xe3, 0x48, 0x52,
	0x75, 0x26, 0xec, 0xd9, 0x18, 0xa4, 0xa1, 0xb0, 0x51, 0xe0, 0x01, 0x14, 0x44, 0xbd, 0x25, 0xc9,
	0xa4, 0xe1, 0xf2, 0x73, 0x92, 0x49, 0x23, 0xc5, 0x9a, 0x70, 0xf6, 0xcc, 0x10, 0xe9, 0x95, 0x52,
	0xc6, 0x6a, 0x81, 0xf6, 0x84, 0x78, 0x69, 0x68, 0x41, 0x25, 0x33, 0x0d, 0x4d, 0xb9, 0xce, 0xa7,
	0xa1, 0x9d, 0x11, 0x4f, 0xf8, 0x03, 0x79, 0x4d, 0x46, 0x29, 0xc2, 0xd4, 0xf8, 0x88, 0xa7, 0xb1,
	0x24, 0x5d, 0x6e, 0x02, 0x40, 0x19, 0x1c, 0x2f, 0x01, 0x82, 0x6a, 0x50, 0x34, 0x63, 0x4d, 0xac,
	0x82, 0x47, 0x33, 0xd6, 0xe4, 0x82, 0x52, 0xd8, 0xf7, 0x05, 0xb8, 0xfc, 0x6e, 0x45, 0x91, 0x7f,
	0xa6, 0x01, 0x8a, 0x17, 0x8e, 0xd0, 0xc3, 0x64, 0xe9, 0x89, 0xb5, 0xf5, 0xe6, 0xbb, 0xaf, 0xc7,
	0x9c, 0x14, 0xce, 0x02, 0x95, 0x7a, 0x8c, 0x7b, 0xf4, 0x8a, 0x2a, 0xf5, 0x97, 0x1a, 0x54, 0x43,
	0x55, 0x27, 0x74, 0x3f, 0x65, 0x4d, 0x23, 0x25, 0xf7, 0xe6, 0xdb, 0xd7, 0xf2, 0x25, 0xa5, 0xf2,
	0xca, 0x0e, 0x90, 0x77, 0x9a, 0x9f, 0x68, 0x50, 0x0b, 0x57, 0xa9, 0x50, 0x8a, 0xec, 0x58, 0xc9,
	0xbe, 0xb9, 0x71, 0x3d, 0xe3, 0xf4, 0xe5, 0x09, 0xae, 0x33, 0x03, 0x28, 0x88, 0xba, 0x56, 0xd2,
	0xc6, 0x0f, 0x17, 0xfb, 0x93, 0x36, 0x7e, 0xa4, 0x28, 0x96, 0xb0, 0xf1, 0x1d, 0x7b, 0x40, 0x94,
	0x63, 0x26, 0x0a, 0x5f, 0x69, 0x68, 0xd3, 0x8f, 0x59, 0xa4, 0x6a, 0x96, 0x86, 0x16, 0x1c, 0x33,
	0x59, 0xf1, 0x42, 0x29, 0xc2, 0xae, 0x39, 0x66, 0xd1, 0x82, 0x59, 0xc2, 0x31, 0x63, 0x80, 0xca,
	0x31, 0x0b, 0x6a, 0x53, 0x49, 0xc7, 0x2c, 0xf6, 0x76, 0x91, 0x74, 0xcc, 0xe2, 0xe5, 0xad, 0x84,
	0x75, 0x64, 0xb8, 0xa1, 0x63, 0xb6, 0x94, 0x50, 0xc6, 0x42, 0xef, 0xa6, 0x18, 0x31, 0xf1, 0x49,
	0xa4, 0xf9, 0xde, 0x6b, 0x72, 0xa7, 0xee, 0x71, 0x6e, 0x7e, 0xb9, 0xc7, 0xff, 0x56, 0x83, 0xe5,
	0xa4, 0x12, 0x18, 0x4a, 0xc1, 0x49, 0x79, 0x4a, 0x69, 0x6e, 0xbe, 0x2e, 0xfb, 0x74, 0x6b, 0xf9,
	0xbb, 0xfe, 0x71, 0xfd, 0x5f, 0xbf, 0x5c, 0xd3, 0xfe, 0xe3, 0xcb, 0x35, 0xed, 0xbf, 0xbf, 0x5c,
	0xd3, 0xfe, 0xee, 0x7f, 0xd6, 0xe6, 0x4e, 0xf3, 0xec, 0x3f, 0x1a, 0x7f, 0xfb, 0x37, 0x01, 0x00,
	0x00, 0xff, 0xff, 0xee, 0x4f, 0x63, 0x90, 0xed, 0x3c, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// KVClient is the client API for KV service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type KVClient interface {
	Range(ctx context.Context, in *RangeRequest, opts ...grpc.CallOption) (*RangeResponse, error)
	Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error)
	DeleteRange(ctx context.Context, in *DeleteRangeRequest, opts ...grpc.CallOption) (*DeleteRangeResponse, error)
	Txn(ctx context.Context, in *TxnRequest, opts ...grpc.CallOption) (*TxnResponse, error)
	Compact(ctx context.Context, in *CompactionRequest, opts ...grpc.CallOption) (*CompactionResponse, error)
}

type kVClient struct {
	cc *grpc.ClientConn
}

func NewKVClient(cc *grpc.ClientConn) KVClient {
	return &kVClient{cc}
}

func (c *kVClient) Range(ctx context.Context, in *RangeRequest, opts ...grpc.CallOption) (*RangeResponse, error) {
	out := new(RangeResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.KV/Range", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVClient) Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error) {
	out := new(PutResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.KV/Put", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVClient) DeleteRange(ctx context.Context, in *DeleteRangeRequest, opts ...grpc.CallOption) (*DeleteRangeResponse, error) {
	out := new(DeleteRangeResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.KV/DeleteRange", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVClient) Txn(ctx context.Context, in *TxnRequest, opts ...grpc.CallOption) (*TxnResponse, error) {
	out := new(TxnResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.KV/Txn", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVClient) Compact(ctx context.Context, in *CompactionRequest, opts ...grpc.CallOption) (*CompactionResponse, error) {
	out := new(CompactionResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.KV/Compact", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// KVServer k,v服务
type KVServer interface {
	Range(context.Context, *RangeRequest) (*RangeResponse, error)                   // 范围查询
	Put(context.Context, *PutRequest) (*PutResponse, error)                         // 更新、创建
	DeleteRange(context.Context, *DeleteRangeRequest) (*DeleteRangeResponse, error) // 范围删除
	// Txn 在一个事务中处理多个请求.一个txn请求会增加键值存储的版本并为每个完成的请求生成具有相同版本的事件.不允许在一个txn中多次修改同一个键.
	Txn(context.Context, *TxnRequest) (*TxnResponse, error)
	Compact(context.Context, *CompactionRequest) (*CompactionResponse, error) // 压缩 etcd 键值存储中的事件历史
}

// UnimplementedKVServer can be embedded to have forward compatible implementations.
type UnimplementedKVServer struct{}

func (*UnimplementedKVServer) Range(ctx context.Context, req *RangeRequest) (*RangeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Range not implemented")
}

func (*UnimplementedKVServer) Put(ctx context.Context, req *PutRequest) (*PutResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Put not implemented")
}

func (*UnimplementedKVServer) DeleteRange(ctx context.Context, req *DeleteRangeRequest) (*DeleteRangeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteRange not implemented")
}

func (*UnimplementedKVServer) Txn(ctx context.Context, req *TxnRequest) (*TxnResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Txn not implemented")
}

func (*UnimplementedKVServer) Compact(ctx context.Context, req *CompactionRequest) (*CompactionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Compact not implemented")
}

func RegisterKVServer(s *grpc.Server, srv KVServer) {
	s.RegisterService(&_KV_serviceDesc, srv)
}

func _KV_Range_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RangeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).Range(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.KV/Range",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).Range(ctx, req.(*RangeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).Put(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.KV/Put",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).Put(ctx, req.(*PutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_DeleteRange_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteRangeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).DeleteRange(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.KV/DeleteRange",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).DeleteRange(ctx, req.(*DeleteRangeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_Txn_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TxnRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).Txn(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.KV/Txn",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).Txn(ctx, req.(*TxnRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_Compact_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CompactionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).Compact(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.KV/Compact",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).Compact(ctx, req.(*CompactionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _KV_serviceDesc = grpc.ServiceDesc{
	ServiceName: "etcdserverpb.KV",
	HandlerType: (*KVServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Range",
			Handler:    _KV_Range_Handler,
		},
		{
			MethodName: "Put",
			Handler:    _KV_Put_Handler,
		},
		{
			MethodName: "DeleteRange",
			Handler:    _KV_DeleteRange_Handler,
		},
		{
			MethodName: "Txn",
			Handler:    _KV_Txn_Handler,
		},
		{
			MethodName: "Compact",
			Handler:    _KV_Compact_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "rpc.proto",
}

// WatchClient is the client API for Watch service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type WatchClient interface {
	// Watch watches for events happening or that have happened. Both input and output
	// are streams; the input stream is for creating and canceling watchers and the output
	// stream sends events. One watch RPC can watch on multiple key ranges, streaming events
	// for several watches at once. The entire event history can be watched starting from the
	// last compaction revision.
	Watch(ctx context.Context, opts ...grpc.CallOption) (Watch_WatchClient, error)
}

type watchClient struct {
	cc *grpc.ClientConn
}

func NewWatchClient(cc *grpc.ClientConn) WatchClient {
	return &watchClient{cc}
}

func (c *watchClient) Watch(ctx context.Context, opts ...grpc.CallOption) (Watch_WatchClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Watch_serviceDesc.Streams[0], "/etcdserverpb.Watch/Watch", opts...)
	if err != nil {
		return nil, err
	}
	x := &watchWatchClient{stream}
	return x, nil
}

type Watch_WatchClient interface {
	Send(*WatchRequest) error
	Recv() (*WatchResponse, error)
	grpc.ClientStream
}

type watchWatchClient struct {
	grpc.ClientStream
}

func (x *watchWatchClient) Send(m *WatchRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *watchWatchClient) Recv() (*WatchResponse, error) {
	m := new(WatchResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

type WatchServer interface {
	// Watch 观察正在发生或已经发生的事件.输入和输出都是流;输入流用于创建和取消监视和输出
	// 流发送事件.一个watch RPC可以在多个key range上watch ,一次为几个watch stream event .整个事件历史可以从最后的压缩修订开始观看.
	Watch(Watch_WatchServer) error
}

// UnimplementedWatchServer can be embedded to have forward compatible implementations.
type UnimplementedWatchServer struct{}

func (*UnimplementedWatchServer) Watch(srv Watch_WatchServer) error {
	return status.Errorf(codes.Unimplemented, "method Watch not implemented")
}

func RegisterWatchServer(s *grpc.Server, srv WatchServer) {
	s.RegisterService(&_Watch_serviceDesc, srv)
}

func _Watch_Watch_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(WatchServer).Watch(&watchWatchServer{stream})
}

type Watch_WatchServer interface {
	Send(*WatchResponse) error
	Recv() (*WatchRequest, error)
	grpc.ServerStream
}

type watchWatchServer struct {
	grpc.ServerStream
}

func (x *watchWatchServer) Send(m *WatchResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *watchWatchServer) Recv() (*WatchRequest, error) {
	m := new(WatchRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

var _Watch_serviceDesc = grpc.ServiceDesc{
	ServiceName: "etcdserverpb.Watch",
	HandlerType: (*WatchServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Watch",
			Handler:       _Watch_Watch_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "rpc.proto",
}

// ClusterClient is the client API for Cluster service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type ClusterClient interface {
	MemberAdd(ctx context.Context, in *MemberAddRequest, opts ...grpc.CallOption) (*MemberAddResponse, error)
	MemberRemove(ctx context.Context, in *MemberRemoveRequest, opts ...grpc.CallOption) (*MemberRemoveResponse, error)
	MemberUpdate(ctx context.Context, in *MemberUpdateRequest, opts ...grpc.CallOption) (*MemberUpdateResponse, error)
	MemberList(ctx context.Context, in *MemberListRequest, opts ...grpc.CallOption) (*MemberListResponse, error)
	MemberPromote(ctx context.Context, in *MemberPromoteRequest, opts ...grpc.CallOption) (*MemberPromoteResponse, error)
}

type clusterClient struct {
	cc *grpc.ClientConn
}

func NewClusterClient(cc *grpc.ClientConn) ClusterClient {
	return &clusterClient{cc}
}

func (c *clusterClient) MemberAdd(ctx context.Context, in *MemberAddRequest, opts ...grpc.CallOption) (*MemberAddResponse, error) {
	out := new(MemberAddResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Cluster/MemberAdd", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *clusterClient) MemberRemove(ctx context.Context, in *MemberRemoveRequest, opts ...grpc.CallOption) (*MemberRemoveResponse, error) {
	out := new(MemberRemoveResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Cluster/MemberRemove", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *clusterClient) MemberUpdate(ctx context.Context, in *MemberUpdateRequest, opts ...grpc.CallOption) (*MemberUpdateResponse, error) {
	out := new(MemberUpdateResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Cluster/MemberUpdate", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *clusterClient) MemberList(ctx context.Context, in *MemberListRequest, opts ...grpc.CallOption) (*MemberListResponse, error) {
	out := new(MemberListResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Cluster/MemberList", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *clusterClient) MemberPromote(ctx context.Context, in *MemberPromoteRequest, opts ...grpc.CallOption) (*MemberPromoteResponse, error) {
	out := new(MemberPromoteResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Cluster/MemberPromote", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ClusterServer is the server API for Cluster service.
type ClusterServer interface {
	MemberAdd(context.Context, *MemberAddRequest) (*MemberAddResponse, error)
	MemberRemove(context.Context, *MemberRemoveRequest) (*MemberRemoveResponse, error)
	MemberUpdate(context.Context, *MemberUpdateRequest) (*MemberUpdateResponse, error)
	MemberList(context.Context, *MemberListRequest) (*MemberListResponse, error)
	MemberPromote(context.Context, *MemberPromoteRequest) (*MemberPromoteResponse, error)
}

func RegisterClusterServer(s *grpc.Server, srv ClusterServer) {
	s.RegisterService(&_Cluster_serviceDesc, srv)
}

func _Cluster_MemberAdd_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MemberAddRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClusterServer).MemberAdd(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Cluster/MemberAdd",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClusterServer).MemberAdd(ctx, req.(*MemberAddRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Cluster_MemberRemove_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MemberRemoveRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClusterServer).MemberRemove(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Cluster/MemberRemove",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClusterServer).MemberRemove(ctx, req.(*MemberRemoveRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Cluster_MemberUpdate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MemberUpdateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClusterServer).MemberUpdate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Cluster/MemberUpdate",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClusterServer).MemberUpdate(ctx, req.(*MemberUpdateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Cluster_MemberList_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MemberListRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClusterServer).MemberList(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Cluster/MemberList",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClusterServer).MemberList(ctx, req.(*MemberListRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Cluster_MemberPromote_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MemberPromoteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClusterServer).MemberPromote(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Cluster/MemberPromote",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClusterServer).MemberPromote(ctx, req.(*MemberPromoteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Cluster_serviceDesc = grpc.ServiceDesc{
	ServiceName: "etcdserverpb.Cluster",
	HandlerType: (*ClusterServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "MemberAdd",
			Handler:    _Cluster_MemberAdd_Handler,
		},
		{
			MethodName: "MemberRemove",
			Handler:    _Cluster_MemberRemove_Handler,
		},
		{
			MethodName: "MemberUpdate",
			Handler:    _Cluster_MemberUpdate_Handler,
		},
		{
			MethodName: "MemberList",
			Handler:    _Cluster_MemberList_Handler,
		},
		{
			MethodName: "MemberPromote",
			Handler:    _Cluster_MemberPromote_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "rpc.proto",
}

// MaintenanceClient is the client API for Maintenance service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type MaintenanceClient interface {
	Alarm(ctx context.Context, in *AlarmRequest, opts ...grpc.CallOption) (*AlarmResponse, error)
	Status(ctx context.Context, in *StatusRequest, opts ...grpc.CallOption) (*StatusResponse, error)
	Defragment(ctx context.Context, in *DefragmentRequest, opts ...grpc.CallOption) (*DefragmentResponse, error)
	Hash(ctx context.Context, in *HashRequest, opts ...grpc.CallOption) (*HashResponse, error)
	HashKV(ctx context.Context, in *HashKVRequest, opts ...grpc.CallOption) (*HashKVResponse, error)
	Snapshot(ctx context.Context, in *SnapshotRequest, opts ...grpc.CallOption) (Maintenance_SnapshotClient, error)
	MoveLeader(ctx context.Context, in *MoveLeaderRequest, opts ...grpc.CallOption) (*MoveLeaderResponse, error)
	Downgrade(ctx context.Context, in *DowngradeRequest, opts ...grpc.CallOption) (*DowngradeResponse, error)
}

type maintenanceClient struct {
	cc *grpc.ClientConn
}

func NewMaintenanceClient(cc *grpc.ClientConn) MaintenanceClient {
	return &maintenanceClient{cc}
}

func (c *maintenanceClient) Alarm(ctx context.Context, in *AlarmRequest, opts ...grpc.CallOption) (*AlarmResponse, error) {
	out := new(AlarmResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Maintenance/Alarm", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *maintenanceClient) Status(ctx context.Context, in *StatusRequest, opts ...grpc.CallOption) (*StatusResponse, error) {
	out := new(StatusResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Maintenance/Status", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Defragment 碎片整理
func (c *maintenanceClient) Defragment(ctx context.Context, in *DefragmentRequest, opts ...grpc.CallOption) (*DefragmentResponse, error) {
	out := new(DefragmentResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Maintenance/Defragment", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *maintenanceClient) Hash(ctx context.Context, in *HashRequest, opts ...grpc.CallOption) (*HashResponse, error) {
	out := new(HashResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Maintenance/Hash", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *maintenanceClient) HashKV(ctx context.Context, in *HashKVRequest, opts ...grpc.CallOption) (*HashKVResponse, error) {
	out := new(HashKVResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Maintenance/HashKV", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *maintenanceClient) Snapshot(ctx context.Context, in *SnapshotRequest, opts ...grpc.CallOption) (Maintenance_SnapshotClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Maintenance_serviceDesc.Streams[0], "/etcdserverpb.Maintenance/Snapshot", opts...)
	if err != nil {
		return nil, err
	}
	x := &maintenanceSnapshotClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Maintenance_SnapshotClient interface {
	Recv() (*SnapshotResponse, error)
	grpc.ClientStream
}

type maintenanceSnapshotClient struct {
	grpc.ClientStream
}

func (x *maintenanceSnapshotClient) Recv() (*SnapshotResponse, error) {
	m := new(SnapshotResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *maintenanceClient) MoveLeader(ctx context.Context, in *MoveLeaderRequest, opts ...grpc.CallOption) (*MoveLeaderResponse, error) {
	out := new(MoveLeaderResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Maintenance/MoveLeader", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *maintenanceClient) Downgrade(ctx context.Context, in *DowngradeRequest, opts ...grpc.CallOption) (*DowngradeResponse, error) {
	out := new(DowngradeResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Maintenance/Downgrade", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type MaintenanceServer interface {
	Alarm(context.Context, *AlarmRequest) (*AlarmResponse, error)
	Status(context.Context, *StatusRequest) (*StatusResponse, error)
	Defragment(context.Context, *DefragmentRequest) (*DefragmentResponse, error) // 碎片整理
	Hash(context.Context, *HashRequest) (*HashResponse, error)
	HashKV(context.Context, *HashKVRequest) (*HashKVResponse, error) // 计算所有MVCC键的哈希值直到一个给定的修订.只遍历key桶
	Snapshot(*SnapshotRequest, Maintenance_SnapshotServer) error
	MoveLeader(context.Context, *MoveLeaderRequest) (*MoveLeaderResponse, error)
	Downgrade(context.Context, *DowngradeRequest) (*DowngradeResponse, error)
}

func RegisterMaintenanceServer(s *grpc.Server, srv MaintenanceServer) {
	s.RegisterService(&_Maintenance_serviceDesc, srv)
}

func _Maintenance_Alarm_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AlarmRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MaintenanceServer).Alarm(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Maintenance/Alarm",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MaintenanceServer).Alarm(ctx, req.(*AlarmRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Maintenance_Status_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MaintenanceServer).Status(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Maintenance/Status",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MaintenanceServer).Status(ctx, req.(*StatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Maintenance_Defragment_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DefragmentRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MaintenanceServer).Defragment(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Maintenance/Defragment",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MaintenanceServer).Defragment(ctx, req.(*DefragmentRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Maintenance_Hash_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HashRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MaintenanceServer).Hash(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Maintenance/Hash",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MaintenanceServer).Hash(ctx, req.(*HashRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Maintenance_HashKV_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HashKVRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MaintenanceServer).HashKV(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Maintenance/HashKV",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MaintenanceServer).HashKV(ctx, req.(*HashKVRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Maintenance_Snapshot_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(SnapshotRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(MaintenanceServer).Snapshot(m, &maintenanceSnapshotServer{stream})
}

type Maintenance_SnapshotServer interface {
	Send(*SnapshotResponse) error
	grpc.ServerStream
}

type maintenanceSnapshotServer struct {
	grpc.ServerStream
}

func (x *maintenanceSnapshotServer) Send(m *SnapshotResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _Maintenance_MoveLeader_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MoveLeaderRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MaintenanceServer).MoveLeader(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Maintenance/MoveLeader",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MaintenanceServer).MoveLeader(ctx, req.(*MoveLeaderRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Maintenance_Downgrade_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DowngradeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MaintenanceServer).Downgrade(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Maintenance/Downgrade",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MaintenanceServer).Downgrade(ctx, req.(*DowngradeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Maintenance_serviceDesc = grpc.ServiceDesc{
	ServiceName: "etcdserverpb.Maintenance",
	HandlerType: (*MaintenanceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Alarm",
			Handler:    _Maintenance_Alarm_Handler,
		},
		{
			MethodName: "Status",
			Handler:    _Maintenance_Status_Handler,
		},
		{
			MethodName: "Defragment",
			Handler:    _Maintenance_Defragment_Handler,
		},
		{
			MethodName: "Hash",
			Handler:    _Maintenance_Hash_Handler,
		},
		{
			MethodName: "HashKV",
			Handler:    _Maintenance_HashKV_Handler,
		},
		{
			MethodName: "MoveLeader",
			Handler:    _Maintenance_MoveLeader_Handler,
		},
		{
			MethodName: "Downgrade",
			Handler:    _Maintenance_Downgrade_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Snapshot",
			Handler:       _Maintenance_Snapshot_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "rpc.proto",
}

// AuthClient is the client API for Auth service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type AuthClient interface {
	AuthEnable(ctx context.Context, in *AuthEnableRequest, opts ...grpc.CallOption) (*AuthEnableResponse, error)
	AuthDisable(ctx context.Context, in *AuthDisableRequest, opts ...grpc.CallOption) (*AuthDisableResponse, error)
	AuthStatus(ctx context.Context, in *AuthStatusRequest, opts ...grpc.CallOption) (*AuthStatusResponse, error)
	Authenticate(ctx context.Context, in *AuthenticateRequest, opts ...grpc.CallOption) (*AuthenticateResponse, error)
	UserAdd(ctx context.Context, in *AuthUserAddRequest, opts ...grpc.CallOption) (*AuthUserAddResponse, error)
	UserGet(ctx context.Context, in *AuthUserGetRequest, opts ...grpc.CallOption) (*AuthUserGetResponse, error)
	UserList(ctx context.Context, in *AuthUserListRequest, opts ...grpc.CallOption) (*AuthUserListResponse, error)
	UserDelete(ctx context.Context, in *AuthUserDeleteRequest, opts ...grpc.CallOption) (*AuthUserDeleteResponse, error)
	UserChangePassword(ctx context.Context, in *AuthUserChangePasswordRequest, opts ...grpc.CallOption) (*AuthUserChangePasswordResponse, error)
	UserGrantRole(ctx context.Context, in *AuthUserGrantRoleRequest, opts ...grpc.CallOption) (*AuthUserGrantRoleResponse, error)
	UserRevokeRole(ctx context.Context, in *AuthUserRevokeRoleRequest, opts ...grpc.CallOption) (*AuthUserRevokeRoleResponse, error)
	RoleAdd(ctx context.Context, in *AuthRoleAddRequest, opts ...grpc.CallOption) (*AuthRoleAddResponse, error)
	RoleGet(ctx context.Context, in *AuthRoleGetRequest, opts ...grpc.CallOption) (*AuthRoleGetResponse, error)
	RoleList(ctx context.Context, in *AuthRoleListRequest, opts ...grpc.CallOption) (*AuthRoleListResponse, error)
	RoleDelete(ctx context.Context, in *AuthRoleDeleteRequest, opts ...grpc.CallOption) (*AuthRoleDeleteResponse, error)
	RoleGrantPermission(ctx context.Context, in *AuthRoleGrantPermissionRequest, opts ...grpc.CallOption) (*AuthRoleGrantPermissionResponse, error)
	RoleRevokePermission(ctx context.Context, in *AuthRoleRevokePermissionRequest, opts ...grpc.CallOption) (*AuthRoleRevokePermissionResponse, error)
}

type authClient struct {
	cc *grpc.ClientConn
}

func NewAuthClient(cc *grpc.ClientConn) AuthClient {
	return &authClient{cc}
}

func (c *authClient) AuthEnable(ctx context.Context, in *AuthEnableRequest, opts ...grpc.CallOption) (*AuthEnableResponse, error) {
	out := new(AuthEnableResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/AuthEnable", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *authClient) AuthDisable(ctx context.Context, in *AuthDisableRequest, opts ...grpc.CallOption) (*AuthDisableResponse, error) {
	out := new(AuthDisableResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/AuthDisable", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *authClient) AuthStatus(ctx context.Context, in *AuthStatusRequest, opts ...grpc.CallOption) (*AuthStatusResponse, error) {
	out := new(AuthStatusResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/AuthStatus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *authClient) Authenticate(ctx context.Context, in *AuthenticateRequest, opts ...grpc.CallOption) (*AuthenticateResponse, error) {
	out := new(AuthenticateResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/Authenticate", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *authClient) UserAdd(ctx context.Context, in *AuthUserAddRequest, opts ...grpc.CallOption) (*AuthUserAddResponse, error) {
	out := new(AuthUserAddResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/UserAdd", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *authClient) UserGet(ctx context.Context, in *AuthUserGetRequest, opts ...grpc.CallOption) (*AuthUserGetResponse, error) {
	out := new(AuthUserGetResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/UserGet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *authClient) UserList(ctx context.Context, in *AuthUserListRequest, opts ...grpc.CallOption) (*AuthUserListResponse, error) {
	out := new(AuthUserListResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/UserList", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *authClient) UserDelete(ctx context.Context, in *AuthUserDeleteRequest, opts ...grpc.CallOption) (*AuthUserDeleteResponse, error) {
	out := new(AuthUserDeleteResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/UserDelete", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *authClient) UserChangePassword(ctx context.Context, in *AuthUserChangePasswordRequest, opts ...grpc.CallOption) (*AuthUserChangePasswordResponse, error) {
	out := new(AuthUserChangePasswordResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/UserChangePassword", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *authClient) UserGrantRole(ctx context.Context, in *AuthUserGrantRoleRequest, opts ...grpc.CallOption) (*AuthUserGrantRoleResponse, error) {
	out := new(AuthUserGrantRoleResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/UserGrantRole", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *authClient) UserRevokeRole(ctx context.Context, in *AuthUserRevokeRoleRequest, opts ...grpc.CallOption) (*AuthUserRevokeRoleResponse, error) {
	out := new(AuthUserRevokeRoleResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/UserRevokeRole", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *authClient) RoleAdd(ctx context.Context, in *AuthRoleAddRequest, opts ...grpc.CallOption) (*AuthRoleAddResponse, error) {
	out := new(AuthRoleAddResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/RoleAdd", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *authClient) RoleGet(ctx context.Context, in *AuthRoleGetRequest, opts ...grpc.CallOption) (*AuthRoleGetResponse, error) {
	out := new(AuthRoleGetResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/RoleGet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *authClient) RoleList(ctx context.Context, in *AuthRoleListRequest, opts ...grpc.CallOption) (*AuthRoleListResponse, error) {
	out := new(AuthRoleListResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/RoleList", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *authClient) RoleDelete(ctx context.Context, in *AuthRoleDeleteRequest, opts ...grpc.CallOption) (*AuthRoleDeleteResponse, error) {
	out := new(AuthRoleDeleteResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/RoleDelete", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *authClient) RoleGrantPermission(ctx context.Context, in *AuthRoleGrantPermissionRequest, opts ...grpc.CallOption) (*AuthRoleGrantPermissionResponse, error) {
	out := new(AuthRoleGrantPermissionResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/RoleGrantPermission", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *authClient) RoleRevokePermission(ctx context.Context, in *AuthRoleRevokePermissionRequest, opts ...grpc.CallOption) (*AuthRoleRevokePermissionResponse, error) {
	out := new(AuthRoleRevokePermissionResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Auth/RoleRevokePermission", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type AuthServer interface {
	AuthEnable(context.Context, *AuthEnableRequest) (*AuthEnableResponse, error)
	AuthDisable(context.Context, *AuthDisableRequest) (*AuthDisableResponse, error)
	AuthStatus(context.Context, *AuthStatusRequest) (*AuthStatusResponse, error)
	Authenticate(context.Context, *AuthenticateRequest) (*AuthenticateResponse, error)
	UserAdd(context.Context, *AuthUserAddRequest) (*AuthUserAddResponse, error)
	UserGet(context.Context, *AuthUserGetRequest) (*AuthUserGetResponse, error)
	UserList(context.Context, *AuthUserListRequest) (*AuthUserListResponse, error)
	UserDelete(context.Context, *AuthUserDeleteRequest) (*AuthUserDeleteResponse, error)
	UserChangePassword(context.Context, *AuthUserChangePasswordRequest) (*AuthUserChangePasswordResponse, error)
	UserGrantRole(context.Context, *AuthUserGrantRoleRequest) (*AuthUserGrantRoleResponse, error)
	UserRevokeRole(context.Context, *AuthUserRevokeRoleRequest) (*AuthUserRevokeRoleResponse, error)
	RoleAdd(context.Context, *AuthRoleAddRequest) (*AuthRoleAddResponse, error)
	RoleGet(context.Context, *AuthRoleGetRequest) (*AuthRoleGetResponse, error)
	RoleList(context.Context, *AuthRoleListRequest) (*AuthRoleListResponse, error)
	RoleDelete(context.Context, *AuthRoleDeleteRequest) (*AuthRoleDeleteResponse, error)
	RoleGrantPermission(context.Context, *AuthRoleGrantPermissionRequest) (*AuthRoleGrantPermissionResponse, error)
	RoleRevokePermission(context.Context, *AuthRoleRevokePermissionRequest) (*AuthRoleRevokePermissionResponse, error)
}

func RegisterAuthServer(s *grpc.Server, srv AuthServer) {
	s.RegisterService(&_Auth_serviceDesc, srv)
}

func _Auth_AuthEnable_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthEnableRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).AuthEnable(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/AuthEnable",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).AuthEnable(ctx, req.(*AuthEnableRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Auth_AuthDisable_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthDisableRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).AuthDisable(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/AuthDisable",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).AuthDisable(ctx, req.(*AuthDisableRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Auth_AuthStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).AuthStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/AuthStatus",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).AuthStatus(ctx, req.(*AuthStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Auth_Authenticate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthenticateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).Authenticate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/Authenticate",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).Authenticate(ctx, req.(*AuthenticateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Auth_UserAdd_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthUserAddRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).UserAdd(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/UserAdd",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).UserAdd(ctx, req.(*AuthUserAddRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Auth_UserGet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthUserGetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).UserGet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/UserGet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).UserGet(ctx, req.(*AuthUserGetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Auth_UserList_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthUserListRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).UserList(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/UserList",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).UserList(ctx, req.(*AuthUserListRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Auth_UserDelete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthUserDeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).UserDelete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/UserDelete",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).UserDelete(ctx, req.(*AuthUserDeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Auth_UserChangePassword_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthUserChangePasswordRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).UserChangePassword(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/UserChangePassword",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).UserChangePassword(ctx, req.(*AuthUserChangePasswordRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Auth_UserGrantRole_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthUserGrantRoleRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).UserGrantRole(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/UserGrantRole",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).UserGrantRole(ctx, req.(*AuthUserGrantRoleRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Auth_UserRevokeRole_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthUserRevokeRoleRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).UserRevokeRole(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/UserRevokeRole",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).UserRevokeRole(ctx, req.(*AuthUserRevokeRoleRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Auth_RoleAdd_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthRoleAddRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).RoleAdd(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/RoleAdd",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).RoleAdd(ctx, req.(*AuthRoleAddRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Auth_RoleGet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthRoleGetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).RoleGet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/RoleGet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).RoleGet(ctx, req.(*AuthRoleGetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Auth_RoleList_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthRoleListRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).RoleList(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/RoleList",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).RoleList(ctx, req.(*AuthRoleListRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Auth_RoleDelete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthRoleDeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).RoleDelete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/RoleDelete",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).RoleDelete(ctx, req.(*AuthRoleDeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Auth_RoleGrantPermission_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthRoleGrantPermissionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).RoleGrantPermission(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/RoleGrantPermission",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).RoleGrantPermission(ctx, req.(*AuthRoleGrantPermissionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Auth_RoleRevokePermission_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthRoleRevokePermissionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthServer).RoleRevokePermission(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Auth/RoleRevokePermission",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthServer).RoleRevokePermission(ctx, req.(*AuthRoleRevokePermissionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Auth_serviceDesc = grpc.ServiceDesc{
	ServiceName: "etcdserverpb.Auth",
	HandlerType: (*AuthServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "AuthEnable",
			Handler:    _Auth_AuthEnable_Handler,
		},
		{
			MethodName: "AuthDisable",
			Handler:    _Auth_AuthDisable_Handler,
		},
		{
			MethodName: "AuthStatus",
			Handler:    _Auth_AuthStatus_Handler,
		},
		{
			MethodName: "Authenticate",
			Handler:    _Auth_Authenticate_Handler,
		},
		{
			MethodName: "UserAdd",
			Handler:    _Auth_UserAdd_Handler,
		},
		{
			MethodName: "UserGet",
			Handler:    _Auth_UserGet_Handler,
		},
		{
			MethodName: "UserList",
			Handler:    _Auth_UserList_Handler,
		},
		{
			MethodName: "UserDelete",
			Handler:    _Auth_UserDelete_Handler,
		},
		{
			MethodName: "UserChangePassword",
			Handler:    _Auth_UserChangePassword_Handler,
		},
		{
			MethodName: "UserGrantRole",
			Handler:    _Auth_UserGrantRole_Handler,
		},
		{
			MethodName: "UserRevokeRole",
			Handler:    _Auth_UserRevokeRole_Handler,
		},
		{
			MethodName: "RoleAdd",
			Handler:    _Auth_RoleAdd_Handler,
		},
		{
			MethodName: "RoleGet",
			Handler:    _Auth_RoleGet_Handler,
		},
		{
			MethodName: "RoleList",
			Handler:    _Auth_RoleList_Handler,
		},
		{
			MethodName: "RoleDelete",
			Handler:    _Auth_RoleDelete_Handler,
		},
		{
			MethodName: "RoleGrantPermission",
			Handler:    _Auth_RoleGrantPermission_Handler,
		},
		{
			MethodName: "RoleRevokePermission",
			Handler:    _Auth_RoleRevokePermission_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "rpc.proto",
}

func encodeVarintRpc(dAtA []byte, offset int, v uint64) int {
	offset -= sovRpc(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}

func (m *ResponseHeader) Marshal() (dAtA []byte, err error)      { return json.Marshal(m) }
func (m *RangeRequest) Marshal() (dAtA []byte, err error)        { return json.Marshal(m) }
func (m *RangeResponse) Marshal() (dAtA []byte, err error)       { return json.Marshal(m) }
func (m *PutRequest) Marshal() (dAtA []byte, err error)          { return json.Marshal(m) }
func (m *PutResponse) Marshal() (dAtA []byte, err error)         { return json.Marshal(m) }
func (m *DeleteRangeRequest) Marshal() (dAtA []byte, err error)  { return json.Marshal(m) }
func (m *DeleteRangeResponse) Marshal() (dAtA []byte, err error) { return json.Marshal(m) }
func (m *RequestOp) Marshal() (dAtA []byte, err error)           { return json.Marshal(m) }
func (m *ResponseOp) Marshal() (dAtA []byte, err error)          { return json.Marshal(m) }
func (m *Compare) Marshal() (dAtA []byte, err error)             { return json.Marshal(m) }
func (m *TxnRequest) Marshal() (dAtA []byte, err error)          { return json.Marshal(m) }
func (m *TxnResponse) Marshal() (dAtA []byte, err error)         { return json.Marshal(m) }
func (m *CompactionRequest) Marshal() (dAtA []byte, err error)   { return json.Marshal(m) }
func (m *CompactionResponse) Marshal() (dAtA []byte, err error)  { return json.Marshal(m) }
func (m *HashRequest) Marshal() (dAtA []byte, err error)         { return json.Marshal(m) }
func (m *HashKVRequest) Marshal() (dAtA []byte, err error)       { return json.Marshal(m) }
func (m *HashKVResponse) Marshal() (dAtA []byte, err error)      { return json.Marshal(m) }
func (m *HashResponse) Marshal() (dAtA []byte, err error)        { return json.Marshal(m) }
func (m *SnapshotRequest) Marshal() (dAtA []byte, err error)     { return json.Marshal(m) }
func (m *SnapshotResponse) Marshal() (dAtA []byte, err error)    { return json.Marshal(m) }

func (m *WatchCreateRequest) Marshal() (dAtA []byte, err error)               { return json.Marshal(m) }
func (m *WatchCancelRequest) Marshal() (dAtA []byte, err error)               { return json.Marshal(m) }
func (m *WatchProgressRequest) Marshal() (dAtA []byte, err error)             { return json.Marshal(m) }
func (m *WatchResponse) Marshal() (dAtA []byte, err error)                    { return json.Marshal(m) }
func (m *LeaseGrantRequest) Marshal() (dAtA []byte, err error)                { return json.Marshal(m) }
func (m *LeaseGrantResponse) Marshal() (dAtA []byte, err error)               { return json.Marshal(m) }
func (m *LeaseRevokeRequest) Marshal() (dAtA []byte, err error)               { return json.Marshal(m) }
func (m *LeaseRevokeResponse) Marshal() (dAtA []byte, err error)              { return json.Marshal(m) }
func (m *LeaseCheckpoint) Marshal() (dAtA []byte, err error)                  { return json.Marshal(m) }
func (m *LeaseCheckpointRequest) Marshal() (dAtA []byte, err error)           { return json.Marshal(m) }
func (m *LeaseCheckpointResponse) Marshal() (dAtA []byte, err error)          { return json.Marshal(m) }
func (m *LeaseKeepAliveRequest) Marshal() (dAtA []byte, err error)            { return json.Marshal(m) }
func (m *LeaseKeepAliveResponse) Marshal() (dAtA []byte, err error)           { return json.Marshal(m) }
func (m *LeaseTimeToLiveRequest) Marshal() (dAtA []byte, err error)           { return json.Marshal(m) }
func (m *LeaseTimeToLiveResponse) Marshal() (dAtA []byte, err error)          { return json.Marshal(m) }
func (m *LeaseLeasesRequest) Marshal() (dAtA []byte, err error)               { return json.Marshal(m) }
func (m *LeaseStatus) Marshal() (dAtA []byte, err error)                      { return json.Marshal(m) }
func (m *LeaseLeasesResponse) Marshal() (dAtA []byte, err error)              { return json.Marshal(m) }
func (m *Member) Marshal() (dAtA []byte, err error)                           { return json.Marshal(m) }
func (m *MemberAddRequest) Marshal() (dAtA []byte, err error)                 { return json.Marshal(m) }
func (m *MemberAddResponse) Marshal() (dAtA []byte, err error)                { return json.Marshal(m) }
func (m *MemberRemoveRequest) Marshal() (dAtA []byte, err error)              { return json.Marshal(m) }
func (m *MemberRemoveResponse) Marshal() (dAtA []byte, err error)             { return json.Marshal(m) }
func (m *MemberUpdateRequest) Marshal() (dAtA []byte, err error)              { return json.Marshal(m) }
func (m *MemberUpdateResponse) Marshal() (dAtA []byte, err error)             { return json.Marshal(m) }
func (m *MemberListRequest) Marshal() (dAtA []byte, err error)                { return json.Marshal(m) }
func (m *MemberListResponse) Marshal() (dAtA []byte, err error)               { return json.Marshal(m) }
func (m *MemberPromoteRequest) Marshal() (dAtA []byte, err error)             { return json.Marshal(m) }
func (m *MemberPromoteResponse) Marshal() (dAtA []byte, err error)            { return json.Marshal(m) }
func (m *DefragmentRequest) Marshal() (dAtA []byte, err error)                { return json.Marshal(m) }
func (m *DefragmentResponse) Marshal() (dAtA []byte, err error)               { return json.Marshal(m) }
func (m *MoveLeaderRequest) Marshal() (dAtA []byte, err error)                { return json.Marshal(m) }
func (m *MoveLeaderResponse) Marshal() (dAtA []byte, err error)               { return json.Marshal(m) }
func (m *AlarmRequest) Marshal() (dAtA []byte, err error)                     { return json.Marshal(m) }
func (m *AlarmResponse) Marshal() (dAtA []byte, err error)                    { return json.Marshal(m) }
func (m *DowngradeRequest) Marshal() (dAtA []byte, err error)                 { return json.Marshal(m) }
func (m *DowngradeResponse) Marshal() (dAtA []byte, err error)                { return json.Marshal(m) }
func (m *StatusRequest) Marshal() (dAtA []byte, err error)                    { return json.Marshal(m) }
func (m *StatusResponse) Marshal() (dAtA []byte, err error)                   { return json.Marshal(m) }
func (m *AuthEnableRequest) Marshal() (dAtA []byte, err error)                { return json.Marshal(m) }
func (m *AuthDisableRequest) Marshal() (dAtA []byte, err error)               { return json.Marshal(m) }
func (m *AuthStatusRequest) Marshal() (dAtA []byte, err error)                { return json.Marshal(m) }
func (m *AuthenticateRequest) Marshal() (dAtA []byte, err error)              { return json.Marshal(m) }
func (m *AuthUserAddRequest) Marshal() (dAtA []byte, err error)               { return json.Marshal(m) }
func (m *AuthUserGetRequest) Marshal() (dAtA []byte, err error)               { return json.Marshal(m) }
func (m *AuthUserDeleteRequest) Marshal() (dAtA []byte, err error)            { return json.Marshal(m) }
func (m *AuthUserChangePasswordRequest) Marshal() (dAtA []byte, err error)    { return json.Marshal(m) }
func (m *AuthUserGrantRoleRequest) Marshal() (dAtA []byte, err error)         { return json.Marshal(m) }
func (m *AuthUserRevokeRoleRequest) Marshal() (dAtA []byte, err error)        { return json.Marshal(m) }
func (m *AuthRoleAddRequest) Marshal() (dAtA []byte, err error)               { return json.Marshal(m) }
func (m *AuthRoleGetRequest) Marshal() (dAtA []byte, err error)               { return json.Marshal(m) }
func (m *AuthUserListRequest) Marshal() (dAtA []byte, err error)              { return json.Marshal(m) }
func (m *AuthRoleListRequest) Marshal() (dAtA []byte, err error)              { return json.Marshal(m) }
func (m *AuthRoleDeleteRequest) Marshal() (dAtA []byte, err error)            { return json.Marshal(m) }
func (m *AuthRoleGrantPermissionRequest) Marshal() (dAtA []byte, err error)   { return json.Marshal(m) }
func (m *AuthRoleRevokePermissionRequest) Marshal() (dAtA []byte, err error)  { return json.Marshal(m) }
func (m *AuthEnableResponse) Marshal() (dAtA []byte, err error)               { return json.Marshal(m) }
func (m *AuthDisableResponse) Marshal() (dAtA []byte, err error)              { return json.Marshal(m) }
func (m *AuthStatusResponse) Marshal() (dAtA []byte, err error)               { return json.Marshal(m) }
func (m *AuthenticateResponse) Marshal() (dAtA []byte, err error)             { return json.Marshal(m) }
func (m *AuthUserAddResponse) Marshal() (dAtA []byte, err error)              { return json.Marshal(m) }
func (m *AuthUserGetResponse) Marshal() (dAtA []byte, err error)              { return json.Marshal(m) }
func (m *AuthUserDeleteResponse) Marshal() (dAtA []byte, err error)           { return json.Marshal(m) }
func (m *AuthUserChangePasswordResponse) Marshal() (dAtA []byte, err error)   { return json.Marshal(m) }
func (m *AuthUserGrantRoleResponse) Marshal() (dAtA []byte, err error)        { return json.Marshal(m) }
func (m *AuthUserRevokeRoleResponse) Marshal() (dAtA []byte, err error)       { return json.Marshal(m) }
func (m *AuthRoleAddResponse) Marshal() (dAtA []byte, err error)              { return json.Marshal(m) }
func (m *AuthRoleGetResponse) Marshal() (dAtA []byte, err error)              { return json.Marshal(m) }
func (m *AuthRoleListResponse) Marshal() (dAtA []byte, err error)             { return json.Marshal(m) }
func (m *AuthUserListResponse) Marshal() (dAtA []byte, err error)             { return json.Marshal(m) }
func (m *AuthRoleDeleteResponse) Marshal() (dAtA []byte, err error)           { return json.Marshal(m) }
func (m *AuthRoleGrantPermissionResponse) Marshal() (dAtA []byte, err error)  { return json.Marshal(m) }
func (m *AuthRoleRevokePermissionResponse) Marshal() (dAtA []byte, err error) { return json.Marshal(m) }

func (m *ResponseHeader) Size() (n int)         { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *RangeRequest) Size() (n int)           { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *RangeResponse) Size() (n int)          { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *PutRequest) Size() (n int)             { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *PutResponse) Size() (n int)            { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *DeleteRangeRequest) Size() (n int)     { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *DeleteRangeResponse) Size() (n int)    { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *RequestOp) Size() (n int)              { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *RequestOp_RequestRange) Size() (n int) { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *RequestOp_RequestPut) Size() (n int)   { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *RequestOp_RequestDeleteRange) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}
func (m *RequestOp_RequestTxn) Size() (n int)     { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *ResponseOp) Size() (n int)               { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *ResponseOp_ResponseRange) Size() (n int) { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *ResponseOp_ResponsePut) Size() (n int)   { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *ResponseOp_ResponseDeleteRange) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}
func (m *ResponseOp_ResponseTxn) Size() (n int) { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *Compare) Size() (n int)                { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *Compare_Version) Size() (n int)        { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *Compare_CreateRevision) Size() (n int) { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *Compare_ModRevision) Size() (n int)    { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *Compare_Value) Size() (n int)          { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *Compare_Lease) Size() (n int)          { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *TxnRequest) Size() (n int)             { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *TxnResponse) Size() (n int)            { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *CompactionRequest) Size() (n int)      { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *CompactionResponse) Size() (n int)     { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *HashRequest) Size() (n int)            { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *HashKVRequest) Size() (n int)          { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *HashKVResponse) Size() (n int)         { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *HashResponse) Size() (n int)           { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *SnapshotRequest) Size() (n int)        { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *SnapshotResponse) Size() (n int)       { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *WatchRequest) Size() (n int)           { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *WatchRequest_CreateRequest) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *WatchRequest_CancelRequest) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *WatchRequest_ProgressRequest) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}
func (m *WatchCreateRequest) Size() (n int)      { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *WatchCancelRequest) Size() (n int)      { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *WatchProgressRequest) Size() (n int)    { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *WatchResponse) Size() (n int)           { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *LeaseGrantRequest) Size() (n int)       { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *LeaseGrantResponse) Size() (n int)      { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *LeaseRevokeRequest) Size() (n int)      { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *LeaseRevokeResponse) Size() (n int)     { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *LeaseCheckpoint) Size() (n int)         { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *LeaseCheckpointRequest) Size() (n int)  { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *LeaseCheckpointResponse) Size() (n int) { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *LeaseKeepAliveRequest) Size() (n int)   { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *LeaseKeepAliveResponse) Size() (n int)  { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *LeaseTimeToLiveRequest) Size() (n int)  { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *LeaseTimeToLiveResponse) Size() (n int) { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *LeaseLeasesRequest) Size() (n int)      { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *LeaseStatus) Size() (n int)             { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *LeaseLeasesResponse) Size() (n int)     { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *Member) Size() (n int)                  { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *MemberAddRequest) Size() (n int)        { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *MemberAddResponse) Size() (n int)       { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *MemberRemoveRequest) Size() (n int)     { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *MemberRemoveResponse) Size() (n int)    { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *MemberUpdateRequest) Size() (n int)     { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *MemberUpdateResponse) Size() (n int)    { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *MemberListRequest) Size() (n int)       { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *MemberListResponse) Size() (n int)      { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *MemberPromoteRequest) Size() (n int)    { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *MemberPromoteResponse) Size() (n int)   { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *DefragmentRequest) Size() (n int)       { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *DefragmentResponse) Size() (n int)      { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *MoveLeaderRequest) Size() (n int)       { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *MoveLeaderResponse) Size() (n int)      { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AlarmRequest) Size() (n int)            { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AlarmMember) Size() (n int)             { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AlarmResponse) Size() (n int)           { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *DowngradeRequest) Size() (n int)        { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *DowngradeResponse) Size() (n int)       { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *StatusRequest) Size() (n int)           { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *StatusResponse) Size() (n int)          { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthEnableRequest) Size() (n int)       { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthDisableRequest) Size() (n int)      { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthStatusRequest) Size() (n int)       { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthenticateRequest) Size() (n int)     { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthUserAddRequest) Size() (n int)      { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthUserGetRequest) Size() (n int)      { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthUserDeleteRequest) Size() (n int)   { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthUserChangePasswordRequest) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}
func (m *AuthUserGrantRoleRequest) Size() (n int) { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthUserRevokeRoleRequest) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}
func (m *AuthRoleAddRequest) Size() (n int)    { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthRoleGetRequest) Size() (n int)    { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthUserListRequest) Size() (n int)   { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthRoleListRequest) Size() (n int)   { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthRoleDeleteRequest) Size() (n int) { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthRoleGrantPermissionRequest) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *AuthRoleRevokePermissionRequest) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}
func (m *AuthEnableResponse) Size() (n int)     { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthDisableResponse) Size() (n int)    { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthStatusResponse) Size() (n int)     { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthenticateResponse) Size() (n int)   { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthUserAddResponse) Size() (n int)    { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthUserGetResponse) Size() (n int)    { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthUserDeleteResponse) Size() (n int) { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthUserChangePasswordResponse) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *AuthUserGrantRoleResponse) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *AuthUserRevokeRoleResponse) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}
func (m *AuthRoleAddResponse) Size() (n int)    { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthRoleGetResponse) Size() (n int)    { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthRoleListResponse) Size() (n int)   { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthUserListResponse) Size() (n int)   { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthRoleDeleteResponse) Size() (n int) { marshal, _ := json.Marshal(m); return len(marshal) }
func (m *AuthRoleGrantPermissionResponse) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *AuthRoleRevokePermissionResponse) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func sovRpc(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}

var (
	ErrInvalidLengthRpc        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowRpc          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupRpc = fmt.Errorf("proto: unexpected end of group")
)

func (m *ResponseHeader) Unmarshal(dAtA []byte) error          { return json.Unmarshal(dAtA, m) }
func (m *RangeRequest) Unmarshal(dAtA []byte) error            { return json.Unmarshal(dAtA, m) }
func (m *RangeResponse) Unmarshal(dAtA []byte) error           { return json.Unmarshal(dAtA, m) }
func (m *PutRequest) Unmarshal(dAtA []byte) error              { return json.Unmarshal(dAtA, m) }
func (m *PutResponse) Unmarshal(dAtA []byte) error             { return json.Unmarshal(dAtA, m) }
func (m *DeleteRangeRequest) Unmarshal(dAtA []byte) error      { return json.Unmarshal(dAtA, m) }
func (m *DeleteRangeResponse) Unmarshal(dAtA []byte) error     { return json.Unmarshal(dAtA, m) }
func (m *RequestOp) Unmarshal(dAtA []byte) error               { return json.Unmarshal(dAtA, m) }
func (m *ResponseOp) Unmarshal(dAtA []byte) error              { return json.Unmarshal(dAtA, m) }
func (m *Compare) Unmarshal(dAtA []byte) error                 { return json.Unmarshal(dAtA, m) }
func (m *TxnRequest) Unmarshal(dAtA []byte) error              { return json.Unmarshal(dAtA, m) }
func (m *TxnResponse) Unmarshal(dAtA []byte) error             { return json.Unmarshal(dAtA, m) }
func (m *CompactionRequest) Unmarshal(dAtA []byte) error       { return json.Unmarshal(dAtA, m) }
func (m *CompactionResponse) Unmarshal(dAtA []byte) error      { return json.Unmarshal(dAtA, m) }
func (m *HashRequest) Unmarshal(dAtA []byte) error             { return json.Unmarshal(dAtA, m) }
func (m *HashKVRequest) Unmarshal(dAtA []byte) error           { return json.Unmarshal(dAtA, m) }
func (m *HashKVResponse) Unmarshal(dAtA []byte) error          { return json.Unmarshal(dAtA, m) }
func (m *HashResponse) Unmarshal(dAtA []byte) error            { return json.Unmarshal(dAtA, m) }
func (m *SnapshotRequest) Unmarshal(dAtA []byte) error         { return json.Unmarshal(dAtA, m) }
func (m *SnapshotResponse) Unmarshal(dAtA []byte) error        { return json.Unmarshal(dAtA, m) }
func (m *WatchCreateRequest) Unmarshal(dAtA []byte) error      { return json.Unmarshal(dAtA, m) }
func (m *WatchCancelRequest) Unmarshal(dAtA []byte) error      { return json.Unmarshal(dAtA, m) }
func (m *WatchProgressRequest) Unmarshal(dAtA []byte) error    { return json.Unmarshal(dAtA, m) }
func (m *WatchResponse) Unmarshal(dAtA []byte) error           { return json.Unmarshal(dAtA, m) }
func (m *LeaseGrantRequest) Unmarshal(dAtA []byte) error       { return json.Unmarshal(dAtA, m) }
func (m *LeaseGrantResponse) Unmarshal(dAtA []byte) error      { return json.Unmarshal(dAtA, m) }
func (m *LeaseRevokeRequest) Unmarshal(dAtA []byte) error      { return json.Unmarshal(dAtA, m) }
func (m *LeaseRevokeResponse) Unmarshal(dAtA []byte) error     { return json.Unmarshal(dAtA, m) }
func (m *LeaseCheckpoint) Unmarshal(dAtA []byte) error         { return json.Unmarshal(dAtA, m) }
func (m *LeaseCheckpointRequest) Unmarshal(dAtA []byte) error  { return json.Unmarshal(dAtA, m) }
func (m *LeaseCheckpointResponse) Unmarshal(dAtA []byte) error { return json.Unmarshal(dAtA, m) }
func (m *LeaseKeepAliveRequest) Unmarshal(dAtA []byte) error   { return json.Unmarshal(dAtA, m) }
func (m *LeaseKeepAliveResponse) Unmarshal(dAtA []byte) error  { return json.Unmarshal(dAtA, m) }
func (m *LeaseTimeToLiveRequest) Unmarshal(dAtA []byte) error  { return json.Unmarshal(dAtA, m) }
func (m *LeaseTimeToLiveResponse) Unmarshal(dAtA []byte) error { return json.Unmarshal(dAtA, m) }
func (m *LeaseLeasesRequest) Unmarshal(dAtA []byte) error      { return json.Unmarshal(dAtA, m) }
func (m *LeaseStatus) Unmarshal(dAtA []byte) error             { return json.Unmarshal(dAtA, m) }
func (m *LeaseLeasesResponse) Unmarshal(dAtA []byte) error     { return json.Unmarshal(dAtA, m) }
func (m *Member) Unmarshal(dAtA []byte) error                  { return json.Unmarshal(dAtA, m) }
func (m *MemberAddRequest) Unmarshal(dAtA []byte) error        { return json.Unmarshal(dAtA, m) }
func (m *MemberAddResponse) Unmarshal(dAtA []byte) error       { return json.Unmarshal(dAtA, m) }
func (m *MemberRemoveRequest) Unmarshal(dAtA []byte) error     { return json.Unmarshal(dAtA, m) }
func (m *MemberRemoveResponse) Unmarshal(dAtA []byte) error    { return json.Unmarshal(dAtA, m) }
func (m *MemberUpdateRequest) Unmarshal(dAtA []byte) error     { return json.Unmarshal(dAtA, m) }
func (m *MemberUpdateResponse) Unmarshal(dAtA []byte) error    { return json.Unmarshal(dAtA, m) }
func (m *MemberListRequest) Unmarshal(dAtA []byte) error       { return json.Unmarshal(dAtA, m) }
func (m *MemberListResponse) Unmarshal(dAtA []byte) error      { return json.Unmarshal(dAtA, m) }
func (m *MemberPromoteRequest) Unmarshal(dAtA []byte) error    { return json.Unmarshal(dAtA, m) }
func (m *MemberPromoteResponse) Unmarshal(dAtA []byte) error   { return json.Unmarshal(dAtA, m) }
func (m *DefragmentRequest) Unmarshal(dAtA []byte) error       { return json.Unmarshal(dAtA, m) }
func (m *DefragmentResponse) Unmarshal(dAtA []byte) error      { return json.Unmarshal(dAtA, m) }
func (m *MoveLeaderRequest) Unmarshal(dAtA []byte) error       { return json.Unmarshal(dAtA, m) }
func (m *MoveLeaderResponse) Unmarshal(dAtA []byte) error      { return json.Unmarshal(dAtA, m) }
func (m *AlarmRequest) Unmarshal(dAtA []byte) error            { return json.Unmarshal(dAtA, m) }

func (m *AlarmResponse) Unmarshal(dAtA []byte) error                  { return json.Unmarshal(dAtA, m) }
func (m *DowngradeRequest) Unmarshal(dAtA []byte) error               { return json.Unmarshal(dAtA, m) }
func (m *DowngradeResponse) Unmarshal(dAtA []byte) error              { return json.Unmarshal(dAtA, m) }
func (m *StatusRequest) Unmarshal(dAtA []byte) error                  { return json.Unmarshal(dAtA, m) }
func (m *StatusResponse) Unmarshal(dAtA []byte) error                 { return json.Unmarshal(dAtA, m) }
func (m *AuthEnableRequest) Unmarshal(dAtA []byte) error              { return json.Unmarshal(dAtA, m) }
func (m *AuthDisableRequest) Unmarshal(dAtA []byte) error             { return json.Unmarshal(dAtA, m) }
func (m *AuthStatusRequest) Unmarshal(dAtA []byte) error              { return json.Unmarshal(dAtA, m) }
func (m *AuthenticateRequest) Unmarshal(dAtA []byte) error            { return json.Unmarshal(dAtA, m) }
func (m *AuthUserAddRequest) Unmarshal(dAtA []byte) error             { return json.Unmarshal(dAtA, m) }
func (m *AuthUserGetRequest) Unmarshal(dAtA []byte) error             { return json.Unmarshal(dAtA, m) }
func (m *AuthUserDeleteRequest) Unmarshal(dAtA []byte) error          { return json.Unmarshal(dAtA, m) }
func (m *AuthUserChangePasswordRequest) Unmarshal(dAtA []byte) error  { return json.Unmarshal(dAtA, m) }
func (m *AuthUserGrantRoleRequest) Unmarshal(dAtA []byte) error       { return json.Unmarshal(dAtA, m) }
func (m *AuthUserRevokeRoleRequest) Unmarshal(dAtA []byte) error      { return json.Unmarshal(dAtA, m) }
func (m *AuthRoleAddRequest) Unmarshal(dAtA []byte) error             { return json.Unmarshal(dAtA, m) }
func (m *AuthRoleGetRequest) Unmarshal(dAtA []byte) error             { return json.Unmarshal(dAtA, m) }
func (m *AuthUserListRequest) Unmarshal(dAtA []byte) error            { return json.Unmarshal(dAtA, m) }
func (m *AuthRoleListRequest) Unmarshal(dAtA []byte) error            { return json.Unmarshal(dAtA, m) }
func (m *AuthRoleDeleteRequest) Unmarshal(dAtA []byte) error          { return json.Unmarshal(dAtA, m) }
func (m *AuthRoleGrantPermissionRequest) Unmarshal(dAtA []byte) error { return json.Unmarshal(dAtA, m) }
func (m *AuthRoleRevokePermissionRequest) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}
func (m *AuthEnableResponse) Unmarshal(dAtA []byte) error             { return json.Unmarshal(dAtA, m) }
func (m *AuthDisableResponse) Unmarshal(dAtA []byte) error            { return json.Unmarshal(dAtA, m) }
func (m *AuthStatusResponse) Unmarshal(dAtA []byte) error             { return json.Unmarshal(dAtA, m) }
func (m *AuthenticateResponse) Unmarshal(dAtA []byte) error           { return json.Unmarshal(dAtA, m) }
func (m *AuthUserAddResponse) Unmarshal(dAtA []byte) error            { return json.Unmarshal(dAtA, m) }
func (m *AuthUserGetResponse) Unmarshal(dAtA []byte) error            { return json.Unmarshal(dAtA, m) }
func (m *AuthUserDeleteResponse) Unmarshal(dAtA []byte) error         { return json.Unmarshal(dAtA, m) }
func (m *AuthUserChangePasswordResponse) Unmarshal(dAtA []byte) error { return json.Unmarshal(dAtA, m) }
func (m *AuthUserGrantRoleResponse) Unmarshal(dAtA []byte) error      { return json.Unmarshal(dAtA, m) }
func (m *AuthUserRevokeRoleResponse) Unmarshal(dAtA []byte) error     { return json.Unmarshal(dAtA, m) }
func (m *AuthRoleAddResponse) Unmarshal(dAtA []byte) error            { return json.Unmarshal(dAtA, m) }
func (m *AuthRoleGetResponse) Unmarshal(dAtA []byte) error            { return json.Unmarshal(dAtA, m) }
func (m *AuthRoleListResponse) Unmarshal(dAtA []byte) error           { return json.Unmarshal(dAtA, m) }
func (m *AuthUserListResponse) Unmarshal(dAtA []byte) error           { return json.Unmarshal(dAtA, m) }
func (m *AuthRoleDeleteResponse) Unmarshal(dAtA []byte) error         { return json.Unmarshal(dAtA, m) }
func (m *AuthRoleGrantPermissionResponse) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *AuthRoleRevokePermissionResponse) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

type alarmMember struct {
	MemberID uint64 `protobuf:"varint,1,opt,name=memberID,proto3" json:"memberID,omitempty"`
	Alarm    string `protobuf:"varint,2,opt,name=alarm,proto3,enum=etcdserverpb.AlarmType" json:"alarm,omitempty"`
}

func (m *AlarmMember) Marshal() (dAtA []byte, err error) {
	a := alarmMember{MemberID: m.MemberID}
	switch m.Alarm {
	case 0:
		a.Alarm = "NONE"
	case 1:
		a.Alarm = "NOSPACE"
	case 2:
		a.Alarm = "CORRUPT"
	}

	return json.Marshal(&a)
}

func (m *AlarmMember) Unmarshal(dAtA []byte) error {
	a := alarmMember{}
	err := json.Unmarshal(dAtA, &a)
	if err == nil {
		m.MemberID = a.MemberID
		switch a.Alarm {
		case "NONE":
			m.Alarm = 0
		case "NOSPACE":
			m.Alarm = 1
		case "CORRUPT":
			m.Alarm = 2
		}
	}
	return err
}
