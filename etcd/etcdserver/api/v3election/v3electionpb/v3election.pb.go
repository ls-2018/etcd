package v3electionpb

// source: v3election.proto

import (
	context "context"
	"encoding/json"
	fmt "fmt"
	math "math"
	math_bits "math/bits"

	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/golang/protobuf/proto"
	mvccpb "github.com/ls-2018/etcd_cn/offical/api/v3/mvccpb"
	etcdserverpb "github.com/ls-2018/etcd_cn/offical/etcdserverpb"
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

type CampaignRequest struct {
	// name is the election's identifier for the campaign.
	Name []byte `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// lease is the ID of the lease attached to leadership of the election. If the
	// lease expires or is revoked before resigning leadership, then the
	// leadership is transferred to the next campaigner, if any.
	Lease int64 `protobuf:"varint,2,opt,name=lease,proto3" json:"lease,omitempty"`
	// value is the initial proclaimed value set when the campaigner wins the
	// election.
	Value []byte `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *CampaignRequest) Reset()         { *m = CampaignRequest{} }
func (m *CampaignRequest) String() string { return proto.CompactTextString(m) }
func (*CampaignRequest) ProtoMessage()    {}
func (*CampaignRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_c9b1f26cc432a035, []int{0}
}

func (m *CampaignRequest) GetName() []byte {
	if m != nil {
		return m.Name
	}
	return nil
}

func (m *CampaignRequest) GetLease() int64 {
	if m != nil {
		return m.Lease
	}
	return 0
}

func (m *CampaignRequest) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

type CampaignResponse struct {
	Header *etcdserverpb.ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// leader describes the resources used for holding leadereship of the election.
	Leader               *LeaderKey `protobuf:"bytes,2,opt,name=leader,proto3" json:"leader,omitempty"`
	XXX_NoUnkeyedLiteral struct{}   `json:"-"`
	XXX_unrecognized     []byte     `json:"-"`
	XXX_sizecache        int32      `json:"-"`
}

func (m *CampaignResponse) Reset()         { *m = CampaignResponse{} }
func (m *CampaignResponse) String() string { return proto.CompactTextString(m) }
func (*CampaignResponse) ProtoMessage()    {}
func (*CampaignResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_c9b1f26cc432a035, []int{1}
}

func (m *CampaignResponse) GetHeader() *etcdserverpb.ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *CampaignResponse) GetLeader() *LeaderKey {
	if m != nil {
		return m.Leader
	}
	return nil
}

type LeaderKey struct {
	// name is the election identifier that correponds to the leadership key.
	Name []byte `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// key is an opaque key representing the ownership of the election. If the key
	// is deleted, then leadership is lost.
	Key []byte `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	// rev is the creation revision of the key. It can be used to test for ownership
	// of an election during transactions by testing the key's creation revision
	// matches rev.
	Rev int64 `protobuf:"varint,3,opt,name=rev,proto3" json:"rev,omitempty"`
	// lease is the lease ID of the election leader.
	Lease int64 `protobuf:"varint,4,opt,name=lease,proto3" json:"lease,omitempty"`
}

func (m *LeaderKey) Reset()         { *m = LeaderKey{} }
func (m *LeaderKey) String() string { return proto.CompactTextString(m) }
func (*LeaderKey) ProtoMessage()    {}
func (*LeaderKey) Descriptor() ([]byte, []int) {
	return fileDescriptor_c9b1f26cc432a035, []int{2}
}

func (m *LeaderKey) GetName() []byte {
	if m != nil {
		return m.Name
	}
	return nil
}

func (m *LeaderKey) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *LeaderKey) GetRev() int64 {
	if m != nil {
		return m.Rev
	}
	return 0
}

func (m *LeaderKey) GetLease() int64 {
	if m != nil {
		return m.Lease
	}
	return 0
}

type LeaderRequest struct {
	// name is the election identifier for the leadership information.
	Name []byte `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

func (m *LeaderRequest) Reset()         { *m = LeaderRequest{} }
func (m *LeaderRequest) String() string { return proto.CompactTextString(m) }
func (*LeaderRequest) ProtoMessage()    {}
func (*LeaderRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_c9b1f26cc432a035, []int{3}
}

func (m *LeaderRequest) GetName() []byte {
	if m != nil {
		return m.Name
	}
	return nil
}

type LeaderResponse struct {
	Header *etcdserverpb.ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// kv is the key-value pair representing the latest leader update.
	Kv                   *mvccpb.KeyValue `protobuf:"bytes,2,opt,name=kv,proto3" json:"kv,omitempty"`
	XXX_NoUnkeyedLiteral struct{}         `json:"-"`
	XXX_unrecognized     []byte           `json:"-"`
	XXX_sizecache        int32            `json:"-"`
}

func (m *LeaderResponse) Reset()         { *m = LeaderResponse{} }
func (m *LeaderResponse) String() string { return proto.CompactTextString(m) }
func (*LeaderResponse) ProtoMessage()    {}
func (*LeaderResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_c9b1f26cc432a035, []int{4}
}

func (m *LeaderResponse) GetHeader() *etcdserverpb.ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *LeaderResponse) GetKv() *mvccpb.KeyValue {
	if m != nil {
		return m.Kv
	}
	return nil
}

type ResignRequest struct {
	// leader is the leadership to relinquish by resignation.
	Leader               *LeaderKey `protobuf:"bytes,1,opt,name=leader,proto3" json:"leader,omitempty"`
	XXX_NoUnkeyedLiteral struct{}   `json:"-"`
	XXX_unrecognized     []byte     `json:"-"`
	XXX_sizecache        int32      `json:"-"`
}

func (m *ResignRequest) Reset()         { *m = ResignRequest{} }
func (m *ResignRequest) String() string { return proto.CompactTextString(m) }
func (*ResignRequest) ProtoMessage()    {}
func (*ResignRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_c9b1f26cc432a035, []int{5}
}

func (m *ResignRequest) GetLeader() *LeaderKey {
	if m != nil {
		return m.Leader
	}
	return nil
}

type ResignResponse struct {
	Header               *etcdserverpb.ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                     `json:"-"`
	XXX_unrecognized     []byte                       `json:"-"`
	XXX_sizecache        int32                        `json:"-"`
}

func (m *ResignResponse) Reset()         { *m = ResignResponse{} }
func (m *ResignResponse) String() string { return proto.CompactTextString(m) }
func (*ResignResponse) ProtoMessage()    {}
func (*ResignResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_c9b1f26cc432a035, []int{6}
}

func (m *ResignResponse) GetHeader() *etcdserverpb.ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

type ProclaimRequest struct {
	// leader is the leadership hold on the election.
	Leader *LeaderKey `protobuf:"bytes,1,opt,name=leader,proto3" json:"leader,omitempty"`
	// value is an update meant to overwrite the leader's current value.
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *ProclaimRequest) Reset()         { *m = ProclaimRequest{} }
func (m *ProclaimRequest) String() string { return proto.CompactTextString(m) }
func (*ProclaimRequest) ProtoMessage()    {}
func (*ProclaimRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_c9b1f26cc432a035, []int{7}
}

func (m *ProclaimRequest) GetLeader() *LeaderKey {
	if m != nil {
		return m.Leader
	}
	return nil
}

func (m *ProclaimRequest) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

type ProclaimResponse struct {
	Header               *etcdserverpb.ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                     `json:"-"`
	XXX_unrecognized     []byte                       `json:"-"`
	XXX_sizecache        int32                        `json:"-"`
}

func (m *ProclaimResponse) Reset()         { *m = ProclaimResponse{} }
func (m *ProclaimResponse) String() string { return proto.CompactTextString(m) }
func (*ProclaimResponse) ProtoMessage()    {}
func (*ProclaimResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_c9b1f26cc432a035, []int{8}
}

func (m *ProclaimResponse) GetHeader() *etcdserverpb.ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func init() {
	proto.RegisterType((*CampaignRequest)(nil), "v3electionpb.CampaignRequest")
	proto.RegisterType((*CampaignResponse)(nil), "v3electionpb.CampaignResponse")
	proto.RegisterType((*LeaderKey)(nil), "v3electionpb.LeaderKey")
	proto.RegisterType((*LeaderRequest)(nil), "v3electionpb.LeaderRequest")
	proto.RegisterType((*LeaderResponse)(nil), "v3electionpb.LeaderResponse")
	proto.RegisterType((*ResignRequest)(nil), "v3electionpb.ResignRequest")
	proto.RegisterType((*ResignResponse)(nil), "v3electionpb.ResignResponse")
	proto.RegisterType((*ProclaimRequest)(nil), "v3electionpb.ProclaimRequest")
	proto.RegisterType((*ProclaimResponse)(nil), "v3electionpb.ProclaimResponse")
}

func init() { proto.RegisterFile("v3election.proto", fileDescriptor_c9b1f26cc432a035) }

var fileDescriptor_c9b1f26cc432a035 = []byte{
	// 531 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xa4, 0x94, 0xcf, 0x6e, 0xd3, 0x40,
	0x10, 0xc6, 0x59, 0x27, 0x84, 0x32, 0xa4, 0xad, 0x65, 0x82, 0x08, 0x21, 0xb8, 0xd1, 0x72, 0xa9,
	0x72, 0xb0, 0x51, 0xc3, 0x29, 0x27, 0x04, 0x02, 0x55, 0x2a, 0x12, 0xe0, 0x03, 0x82, 0xe3, 0xda,
	0x1d, 0xb9, 0x91, 0x1d, 0xaf, 0xb1, 0x5d, 0x4b, 0xb9, 0xf2, 0x0a, 0x1c, 0xe0, 0x91, 0x38, 0x22,
	0xf1, 0x02, 0x28, 0xf0, 0x20, 0x68, 0x77, 0xed, 0xfa, 0x8f, 0x12, 0x84, 0x9a, 0xdb, 0x78, 0xe7,
	0xdb, 0xf9, 0xcd, 0x37, 0x3b, 0x09, 0xe8, 0xf9, 0x0c, 0x43, 0xf4, 0xb2, 0x05, 0x8f, 0xac, 0x38,
	0xe1, 0x19, 0x37, 0xfa, 0xd5, 0x49, 0xec, 0x8e, 0x06, 0x3e, 0xf7, 0xb9, 0x4c, 0xd8, 0x22, 0x52,
	0x9a, 0xd1, 0x11, 0x66, 0xde, 0xb9, 0xcd, 0xe2, 0x85, 0x2d, 0x82, 0x14, 0x93, 0x1c, 0x93, 0xd8,
	0xb5, 0x93, 0xd8, 0x2b, 0x04, 0xc3, 0x2b, 0xc1, 0x32, 0xf7, 0xbc, 0xd8, 0xb5, 0x83, 0xbc, 0xc8,
	0x8c, 0x7d, 0xce, 0xfd, 0x10, 0x65, 0x8e, 0x45, 0x11, 0xcf, 0x98, 0x20, 0xa5, 0x2a, 0x4b, 0xdf,
	0xc1, 0xe1, 0x0b, 0xb6, 0x8c, 0xd9, 0xc2, 0x8f, 0x1c, 0xfc, 0x74, 0x89, 0x69, 0x66, 0x18, 0xd0,
	0x8d, 0xd8, 0x12, 0x87, 0x64, 0x42, 0x8e, 0xfb, 0x8e, 0x8c, 0x8d, 0x01, 0xdc, 0x0c, 0x91, 0xa5,
	0x38, 0xd4, 0x26, 0xe4, 0xb8, 0xe3, 0xa8, 0x0f, 0x71, 0x9a, 0xb3, 0xf0, 0x12, 0x87, 0x1d, 0x29,
	0x55, 0x1f, 0x74, 0x05, 0x7a, 0x55, 0x32, 0x8d, 0x79, 0x94, 0xa2, 0xf1, 0x14, 0x7a, 0x17, 0xc8,
	0xce, 0x31, 0x91, 0x55, 0xef, 0x9c, 0x8c, 0xad, 0xba, 0x0f, 0xab, 0xd4, 0x9d, 0x4a, 0x8d, 0x53,
	0x68, 0x0d, 0x1b, 0x7a, 0xa1, 0xba, 0xa5, 0xc9, 0x5b, 0xf7, 0xad, 0xfa, 0xa8, 0xac, 0xd7, 0x32,
	0x77, 0x86, 0x2b, 0xa7, 0x90, 0xd1, 0x8f, 0x70, 0xfb, 0xea, 0x70, 0xa3, 0x0f, 0x1d, 0x3a, 0x01,
	0xae, 0x64, 0xb9, 0xbe, 0x23, 0x42, 0x71, 0x92, 0x60, 0x2e, 0x1d, 0x74, 0x1c, 0x11, 0x56, 0x5e,
	0xbb, 0x35, 0xaf, 0xf4, 0x31, 0xec, 0xab, 0xd2, 0xff, 0x18, 0x13, 0xbd, 0x80, 0x83, 0x52, 0xb4,
	0x93, 0xf1, 0x09, 0x68, 0x41, 0x5e, 0x98, 0xd6, 0x2d, 0xf5, 0xa2, 0xd6, 0x19, 0xae, 0xde, 0x8b,
	0x01, 0x3b, 0x5a, 0x90, 0xd3, 0x67, 0xb0, 0xef, 0x60, 0x5a, 0x7b, 0xb5, 0x6a, 0x56, 0xe4, 0xff,
	0x66, 0xf5, 0x0a, 0x0e, 0xca, 0x0a, 0xbb, 0xf4, 0x4a, 0x3f, 0xc0, 0xe1, 0xdb, 0x84, 0x7b, 0x21,
	0x5b, 0x2c, 0xaf, 0xdb, 0x4b, 0xb5, 0x48, 0x5a, 0x7d, 0x91, 0x4e, 0x41, 0xaf, 0x2a, 0xef, 0xd2,
	0xe3, 0xc9, 0xd7, 0x2e, 0xec, 0xbd, 0x2c, 0x1a, 0x30, 0x02, 0xd8, 0x2b, 0xf7, 0xd3, 0x78, 0xd4,
	0xec, 0xac, 0xf5, 0x53, 0x18, 0x99, 0xdb, 0xd2, 0x8a, 0x42, 0x27, 0x9f, 0x7f, 0xfe, 0xf9, 0xa2,
	0x8d, 0xe8, 0x3d, 0x3b, 0x9f, 0xd9, 0xa5, 0xd0, 0xf6, 0x0a, 0xd9, 0x9c, 0x4c, 0x05, 0xac, 0xf4,
	0xd0, 0x86, 0xb5, 0xa6, 0xd6, 0x86, 0xb5, 0xad, 0x6f, 0x81, 0xc5, 0x85, 0x4c, 0xc0, 0x3c, 0xe8,
	0xa9, 0xd9, 0x1a, 0x0f, 0x37, 0x4d, 0xbc, 0x04, 0x8d, 0x37, 0x27, 0x0b, 0x8c, 0x29, 0x31, 0x43,
	0x7a, 0xb7, 0x81, 0x51, 0x0f, 0x25, 0x20, 0x3e, 0xdc, 0x7a, 0xe3, 0xca, 0x81, 0xef, 0x42, 0x39,
	0x92, 0x94, 0x07, 0x74, 0xd0, 0xa0, 0x70, 0x55, 0x78, 0x4e, 0xa6, 0x4f, 0x88, 0x70, 0xa3, 0x16,
	0xb4, 0xcd, 0x69, 0x2c, 0x7e, 0x9b, 0xd3, 0xdc, 0xe9, 0x2d, 0x6e, 0x12, 0x29, 0x9a, 0x93, 0xe9,
	0x73, 0xfd, 0xfb, 0xda, 0x24, 0x3f, 0xd6, 0x26, 0xf9, 0xb5, 0x36, 0xc9, 0xb7, 0xdf, 0xe6, 0x0d,
	0xb7, 0x27, 0xff, 0x18, 0x67, 0x7f, 0x03, 0x00, 0x00, 0xff, 0xff, 0x9c, 0xe6, 0x7c, 0x66, 0xa9,
	0x05, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// ElectionClient is the client API for Election service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type ElectionClient interface {
	// Campaign waits to acquire leadership in an election, returning a LeaderKey
	// representing the leadership if successful. The LeaderKey can then be used
	// to issue new values on the election, transactionally guard API requests on
	// leadership still being held, and resign from the election.
	Campaign(ctx context.Context, in *CampaignRequest, opts ...grpc.CallOption) (*CampaignResponse, error)
	// Proclaim updates the leader's posted value with a new value.
	Proclaim(ctx context.Context, in *ProclaimRequest, opts ...grpc.CallOption) (*ProclaimResponse, error)
	// Leader returns the current election proclamation, if any.
	Leader(ctx context.Context, in *LeaderRequest, opts ...grpc.CallOption) (*LeaderResponse, error)
	// Observe streams election proclamations in-order as made by the election's
	// elected leaders.
	Observe(ctx context.Context, in *LeaderRequest, opts ...grpc.CallOption) (Election_ObserveClient, error)
	// Resign releases election leadership so other campaigners may acquire
	// leadership on the election.
	Resign(ctx context.Context, in *ResignRequest, opts ...grpc.CallOption) (*ResignResponse, error)
}

type electionClient struct {
	cc *grpc.ClientConn
}

func NewElectionClient(cc *grpc.ClientConn) ElectionClient {
	return &electionClient{cc}
}

func (c *electionClient) Campaign(ctx context.Context, in *CampaignRequest, opts ...grpc.CallOption) (*CampaignResponse, error) {
	out := new(CampaignResponse)
	err := c.cc.Invoke(ctx, "/v3electionpb.Election/Campaign", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *electionClient) Proclaim(ctx context.Context, in *ProclaimRequest, opts ...grpc.CallOption) (*ProclaimResponse, error) {
	out := new(ProclaimResponse)
	err := c.cc.Invoke(ctx, "/v3electionpb.Election/Proclaim", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *electionClient) Leader(ctx context.Context, in *LeaderRequest, opts ...grpc.CallOption) (*LeaderResponse, error) {
	out := new(LeaderResponse)
	err := c.cc.Invoke(ctx, "/v3electionpb.Election/Leader", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *electionClient) Observe(ctx context.Context, in *LeaderRequest, opts ...grpc.CallOption) (Election_ObserveClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Election_serviceDesc.Streams[0], "/v3electionpb.Election/Observe", opts...)
	if err != nil {
		return nil, err
	}
	x := &electionObserveClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Election_ObserveClient interface {
	Recv() (*LeaderResponse, error)
	grpc.ClientStream
}

type electionObserveClient struct {
	grpc.ClientStream
}

func (x *electionObserveClient) Recv() (*LeaderResponse, error) {
	m := new(LeaderResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *electionClient) Resign(ctx context.Context, in *ResignRequest, opts ...grpc.CallOption) (*ResignResponse, error) {
	out := new(ResignResponse)
	err := c.cc.Invoke(ctx, "/v3electionpb.Election/Resign", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ElectionServer is the etcd API for Election service.
type ElectionServer interface {
	// Campaign waits to acquire leadership in an election, returning a LeaderKey
	// representing the leadership if successful. The LeaderKey can then be used
	// to issue new values on the election, transactionally guard API requests on
	// leadership still being held, and resign from the election.
	Campaign(context.Context, *CampaignRequest) (*CampaignResponse, error)
	// Proclaim updates the leader's posted value with a new value.
	Proclaim(context.Context, *ProclaimRequest) (*ProclaimResponse, error)
	// Leader returns the current election proclamation, if any.
	Leader(context.Context, *LeaderRequest) (*LeaderResponse, error)
	// Observe streams election proclamations in-order as made by the election's
	// elected leaders.
	Observe(*LeaderRequest, Election_ObserveServer) error
	// Resign releases election leadership so other campaigners may acquire
	// leadership on the election.
	Resign(context.Context, *ResignRequest) (*ResignResponse, error)
}

// UnimplementedElectionServer can be embedded to have forward compatible implementations.
type UnimplementedElectionServer struct{}

func (*UnimplementedElectionServer) Campaign(ctx context.Context, req *CampaignRequest) (*CampaignResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Campaign not implemented")
}

func (*UnimplementedElectionServer) Proclaim(ctx context.Context, req *ProclaimRequest) (*ProclaimResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Proclaim not implemented")
}

func (*UnimplementedElectionServer) Leader(ctx context.Context, req *LeaderRequest) (*LeaderResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Leader not implemented")
}

func (*UnimplementedElectionServer) Observe(req *LeaderRequest, srv Election_ObserveServer) error {
	return status.Errorf(codes.Unimplemented, "method Observe not implemented")
}

func (*UnimplementedElectionServer) Resign(ctx context.Context, req *ResignRequest) (*ResignResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Resign not implemented")
}

func RegisterElectionServer(s *grpc.Server, srv ElectionServer) {
	s.RegisterService(&_Election_serviceDesc, srv)
}

func _Election_Campaign_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CampaignRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ElectionServer).Campaign(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/v3electionpb.Election/Campaign",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ElectionServer).Campaign(ctx, req.(*CampaignRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Election_Proclaim_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ProclaimRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ElectionServer).Proclaim(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/v3electionpb.Election/Proclaim",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ElectionServer).Proclaim(ctx, req.(*ProclaimRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Election_Leader_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LeaderRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ElectionServer).Leader(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/v3electionpb.Election/Leader",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ElectionServer).Leader(ctx, req.(*LeaderRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Election_Observe_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(LeaderRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(ElectionServer).Observe(m, &electionObserveServer{stream})
}

type Election_ObserveServer interface {
	Send(*LeaderResponse) error
	grpc.ServerStream
}

type electionObserveServer struct {
	grpc.ServerStream
}

func (x *electionObserveServer) Send(m *LeaderResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _Election_Resign_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ResignRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ElectionServer).Resign(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/v3electionpb.Election/Resign",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ElectionServer).Resign(ctx, req.(*ResignRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Election_serviceDesc = grpc.ServiceDesc{
	ServiceName: "v3electionpb.Election",
	HandlerType: (*ElectionServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Campaign",
			Handler:    _Election_Campaign_Handler,
		},
		{
			MethodName: "Proclaim",
			Handler:    _Election_Proclaim_Handler,
		},
		{
			MethodName: "Leader",
			Handler:    _Election_Leader_Handler,
		},
		{
			MethodName: "Resign",
			Handler:    _Election_Resign_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Observe",
			Handler:       _Election_Observe_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "v3election.proto",
}

func (m *CampaignRequest) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *CampaignResponse) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *LeaderKey) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *LeaderRequest) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *LeaderResponse) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *ResignRequest) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *ResignResponse) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *ProclaimRequest) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *ProclaimResponse) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *CampaignRequest) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *CampaignResponse) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *LeaderKey) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *LeaderRequest) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *LeaderResponse) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *ResignRequest) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *ResignResponse) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *ProclaimRequest) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *ProclaimResponse) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func sovV3Election(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}

func (m *CampaignRequest) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *CampaignResponse) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *LeaderKey) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *LeaderRequest) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *LeaderResponse) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *ResignRequest) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *ResignResponse) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *ProclaimRequest) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *ProclaimResponse) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

var (
	ErrInvalidLengthV3Election        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowV3Election          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupV3Election = fmt.Errorf("proto: unexpected end of group")
)
