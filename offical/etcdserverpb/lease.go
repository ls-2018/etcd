package etcdserverpb

import (
	"context"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LeaseGrantRequest struct {
	// TTL is the advisory time-to-live in seconds. Expired lease will return -1.
	TTL int64 `protobuf:"varint,1,opt,name=TTL,proto3" json:"TTL,omitempty"`
	// ID is the requested ID for the lease. If ID is set to 0, the lessor chooses an ID.
	ID int64 `protobuf:"varint,2,opt,name=ID,proto3" json:"ID,omitempty"`
}

func (m *LeaseGrantRequest) Reset()         { *m = LeaseGrantRequest{} }
func (m *LeaseGrantRequest) String() string { return proto.CompactTextString(m) }
func (*LeaseGrantRequest) ProtoMessage()    {}
func (*LeaseGrantRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{25}
}

func (m *LeaseGrantRequest) GetTTL() int64 {
	if m != nil {
		return m.TTL
	}
	return 0
}

func (m *LeaseGrantRequest) GetID() int64 {
	if m != nil {
		return m.ID
	}
	return 0
}

type LeaseGrantResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// ID is the lease ID for the granted lease.
	ID int64 `protobuf:"varint,2,opt,name=ID,proto3" json:"ID,omitempty"`
	// TTL is the server chosen lease time-to-live in seconds.
	TTL   int64  `protobuf:"varint,3,opt,name=TTL,proto3" json:"TTL,omitempty"`
	Error string `protobuf:"bytes,4,opt,name=error,proto3" json:"error,omitempty"`
}

func (m *LeaseGrantResponse) Reset()         { *m = LeaseGrantResponse{} }
func (m *LeaseGrantResponse) String() string { return proto.CompactTextString(m) }
func (*LeaseGrantResponse) ProtoMessage()    {}
func (*LeaseGrantResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{26}
}

func (m *LeaseGrantResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *LeaseGrantResponse) GetID() int64 {
	if m != nil {
		return m.ID
	}
	return 0
}

func (m *LeaseGrantResponse) GetTTL() int64 {
	if m != nil {
		return m.TTL
	}
	return 0
}

func (m *LeaseGrantResponse) GetError() string {
	if m != nil {
		return m.Error
	}
	return ""
}

type LeaseRevokeRequest struct {
	// ID is the lease ID to revoke. When the ID is revoked, all associated keys will be deleted.
	ID int64 `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
}

func (m *LeaseRevokeRequest) Reset()         { *m = LeaseRevokeRequest{} }
func (m *LeaseRevokeRequest) String() string { return proto.CompactTextString(m) }
func (*LeaseRevokeRequest) ProtoMessage()    {}
func (*LeaseRevokeRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{27}
}

func (m *LeaseRevokeRequest) GetID() int64 {
	if m != nil {
		return m.ID
	}
	return 0
}

type LeaseRevokeResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *LeaseRevokeResponse) Reset()         { *m = LeaseRevokeResponse{} }
func (m *LeaseRevokeResponse) String() string { return proto.CompactTextString(m) }
func (*LeaseRevokeResponse) ProtoMessage()    {}
func (*LeaseRevokeResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{28}
}

func (m *LeaseRevokeResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

type LeaseCheckpoint struct {
	ID           int64 `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`                                         // 租约ID
	RemainingTtl int64 `protobuf:"varint,2,opt,name=remaining_TTL,json=remainingTTL,proto3" json:"remaining_TTL,omitempty"` // 剩余的存活时间
}

func (m *LeaseCheckpoint) Reset()         { *m = LeaseCheckpoint{} }
func (m *LeaseCheckpoint) String() string { return proto.CompactTextString(m) }
func (*LeaseCheckpoint) ProtoMessage()    {}
func (*LeaseCheckpoint) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{29}
}

func (m *LeaseCheckpoint) GetID() int64 {
	if m != nil {
		return m.ID
	}
	return 0
}

func (m *LeaseCheckpoint) GetRemaining_TTL() int64 {
	if m != nil {
		return m.RemainingTtl
	}
	return 0
}

type LeaseCheckpointRequest struct {
	Checkpoints          []*LeaseCheckpoint `protobuf:"bytes,1,rep,name=checkpoints,proto3" json:"checkpoints,omitempty"`
	XXX_NoUnkeyedLiteral struct{}           `json:"-"`
	XXX_unrecognized     []byte             `json:"-"`
	XXX_sizecache        int32              `json:"-"`
}

func (m *LeaseCheckpointRequest) Reset()         { *m = LeaseCheckpointRequest{} }
func (m *LeaseCheckpointRequest) String() string { return proto.CompactTextString(m) }
func (*LeaseCheckpointRequest) ProtoMessage()    {}
func (*LeaseCheckpointRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{30}
}

func (m *LeaseCheckpointRequest) GetCheckpoints() []*LeaseCheckpoint {
	if m != nil {
		return m.Checkpoints
	}
	return nil
}

type LeaseCheckpointResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *LeaseCheckpointResponse) Reset()         { *m = LeaseCheckpointResponse{} }
func (m *LeaseCheckpointResponse) String() string { return proto.CompactTextString(m) }
func (*LeaseCheckpointResponse) ProtoMessage()    {}
func (*LeaseCheckpointResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{31}
}

func (m *LeaseCheckpointResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

type LeaseKeepAliveRequest struct {
	// ID is the lease ID for the lease to keep alive.
	ID int64 `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
}

func (m *LeaseKeepAliveRequest) Reset()         { *m = LeaseKeepAliveRequest{} }
func (m *LeaseKeepAliveRequest) String() string { return proto.CompactTextString(m) }
func (*LeaseKeepAliveRequest) ProtoMessage()    {}
func (*LeaseKeepAliveRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{32}
}

func (m *LeaseKeepAliveRequest) GetID() int64 {
	if m != nil {
		return m.ID
	}
	return 0
}

type LeaseKeepAliveResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	ID     int64           `protobuf:"varint,2,opt,name=ID,proto3" json:"ID,omitempty"` // 租约ID
	// TTL is the new time-to-live for the lease.
	TTL int64 `protobuf:"varint,3,opt,name=TTL,proto3" json:"TTL,omitempty"`
}

func (m *LeaseKeepAliveResponse) Reset()         { *m = LeaseKeepAliveResponse{} }
func (m *LeaseKeepAliveResponse) String() string { return proto.CompactTextString(m) }
func (*LeaseKeepAliveResponse) ProtoMessage()    {}
func (*LeaseKeepAliveResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{33}
}

func (m *LeaseKeepAliveResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *LeaseKeepAliveResponse) GetID() int64 {
	if m != nil {
		return m.ID
	}
	return 0
}

func (m *LeaseKeepAliveResponse) GetTTL() int64 {
	if m != nil {
		return m.TTL
	}
	return 0
}

type LeaseTimeToLiveRequest struct {
	// ID is the lease ID for the lease.
	ID int64 `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
	// keys is true to query all the keys attached to this lease.
	Keys bool `protobuf:"varint,2,opt,name=keys,proto3" json:"keys,omitempty"`
}

func (m *LeaseTimeToLiveRequest) Reset()         { *m = LeaseTimeToLiveRequest{} }
func (m *LeaseTimeToLiveRequest) String() string { return proto.CompactTextString(m) }
func (*LeaseTimeToLiveRequest) ProtoMessage()    {}
func (*LeaseTimeToLiveRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{34}
}

func (m *LeaseTimeToLiveRequest) GetID() int64 {
	if m != nil {
		return m.ID
	}
	return 0
}

func (m *LeaseTimeToLiveRequest) GetKeys() bool {
	if m != nil {
		return m.Keys
	}
	return false
}

type LeaseTimeToLiveResponse struct {
	Header *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// ID is the lease ID from the keep alive request.
	ID int64 `protobuf:"varint,2,opt,name=ID,proto3" json:"ID,omitempty"`
	// TTL is the remaining TTL in seconds for the lease; the lease will expire in under TTL+1 seconds.
	TTL int64 `protobuf:"varint,3,opt,name=TTL,proto3" json:"TTL,omitempty"`
	// GrantedTTL is the initial granted time in seconds upon lease creation/renewal.
	GrantedTTL int64 `protobuf:"varint,4,opt,name=grantedTTL,proto3" json:"grantedTTL,omitempty"`
	// Keys is the list of keys attached to this lease.
	Keys [][]byte `protobuf:"bytes,5,rep,name=keys,proto3" json:"keys,omitempty"`
}

func (m *LeaseTimeToLiveResponse) Reset()         { *m = LeaseTimeToLiveResponse{} }
func (m *LeaseTimeToLiveResponse) String() string { return proto.CompactTextString(m) }
func (*LeaseTimeToLiveResponse) ProtoMessage()    {}
func (*LeaseTimeToLiveResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{35}
}

func (m *LeaseTimeToLiveResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *LeaseTimeToLiveResponse) GetID() int64 {
	if m != nil {
		return m.ID
	}
	return 0
}

func (m *LeaseTimeToLiveResponse) GetTTL() int64 {
	if m != nil {
		return m.TTL
	}
	return 0
}

func (m *LeaseTimeToLiveResponse) GetGrantedTTL() int64 {
	if m != nil {
		return m.GrantedTTL
	}
	return 0
}

func (m *LeaseTimeToLiveResponse) GetKeys() [][]byte {
	if m != nil {
		return m.Keys
	}
	return nil
}

type LeaseLeasesRequest struct{}

func (m *LeaseLeasesRequest) Reset()         { *m = LeaseLeasesRequest{} }
func (m *LeaseLeasesRequest) String() string { return proto.CompactTextString(m) }
func (*LeaseLeasesRequest) ProtoMessage()    {}
func (*LeaseLeasesRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{36}
}

type LeaseStatus struct {
	ID int64 `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
}

func (m *LeaseStatus) Reset()         { *m = LeaseStatus{} }
func (m *LeaseStatus) String() string { return proto.CompactTextString(m) }
func (*LeaseStatus) ProtoMessage()    {}
func (*LeaseStatus) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{37}
}

func (m *LeaseStatus) GetID() int64 {
	if m != nil {
		return m.ID
	}
	return 0
}

type LeaseLeasesResponse struct {
	Header               *ResponseHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	Leases               []*LeaseStatus  `protobuf:"bytes,2,rep,name=leases,proto3" json:"leases,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *LeaseLeasesResponse) Reset()         { *m = LeaseLeasesResponse{} }
func (m *LeaseLeasesResponse) String() string { return proto.CompactTextString(m) }
func (*LeaseLeasesResponse) ProtoMessage()    {}
func (*LeaseLeasesResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{38}
}

func (m *LeaseLeasesResponse) GetHeader() *ResponseHeader {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *LeaseLeasesResponse) GetLeases() []*LeaseStatus {
	if m != nil {
		return m.Leases
	}
	return nil
}

type LeaseServer interface {
	LeaseGrant(context.Context, *LeaseGrantRequest) (*LeaseGrantResponse, error)                // 创建租约
	LeaseRevoke(context.Context, *LeaseRevokeRequest) (*LeaseRevokeResponse, error)             // 移除租约
	LeaseKeepAlive(Lease_LeaseKeepAliveServer) error                                            // 租约 续租
	LeaseTimeToLive(context.Context, *LeaseTimeToLiveRequest) (*LeaseTimeToLiveResponse, error) // 检索租约信息
	LeaseLeases(context.Context, *LeaseLeasesRequest) (*LeaseLeasesResponse, error)             // 显示所有存在的租约
}

// UnimplementedLeaseServer can be embedded to have forward compatible implementations.
type UnimplementedLeaseServer struct{}

func (*UnimplementedLeaseServer) LeaseGrant(ctx context.Context, req *LeaseGrantRequest) (*LeaseGrantResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method LeaseGrant not implemented")
}

func (*UnimplementedLeaseServer) LeaseRevoke(ctx context.Context, req *LeaseRevokeRequest) (*LeaseRevokeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method LeaseRevoke not implemented")
}

func (*UnimplementedLeaseServer) LeaseKeepAlive(srv Lease_LeaseKeepAliveServer) error {
	return status.Errorf(codes.Unimplemented, "method LeaseKeepAlive not implemented")
}

func (*UnimplementedLeaseServer) LeaseTimeToLive(ctx context.Context, req *LeaseTimeToLiveRequest) (*LeaseTimeToLiveResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method LeaseTimeToLive not implemented")
}

func (*UnimplementedLeaseServer) LeaseLeases(ctx context.Context, req *LeaseLeasesRequest) (*LeaseLeasesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method LeaseLeases not implemented")
}

func RegisterLeaseServer(s *grpc.Server, srv LeaseServer) {
	s.RegisterService(&_Lease_serviceDesc, srv)
}

type Lease_LeaseKeepAliveServer interface {
	Send(*LeaseKeepAliveResponse) error
	Recv() (*LeaseKeepAliveRequest, error)
	grpc.ServerStream
}

type leaseLeaseKeepAliveServer struct {
	grpc.ServerStream
}

func (x *leaseLeaseKeepAliveServer) Send(m *LeaseKeepAliveResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *leaseLeaseKeepAliveServer) Recv() (*LeaseKeepAliveRequest, error) {
	m := new(LeaseKeepAliveRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _Lease_LeaseTimeToLive_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LeaseTimeToLiveRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LeaseServer).LeaseTimeToLive(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Lease/LeaseTimeToLive",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LeaseServer).LeaseTimeToLive(ctx, req.(*LeaseTimeToLiveRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Lease_LeaseLeases_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LeaseLeasesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LeaseServer).LeaseLeases(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Lease/LeaseLeases",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LeaseServer).LeaseLeases(ctx, req.(*LeaseLeasesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Lease_LeaseGrant_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LeaseGrantRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LeaseServer).LeaseGrant(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Lease/LeaseGrant",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LeaseServer).LeaseGrant(ctx, req.(*LeaseGrantRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Lease_LeaseRevoke_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LeaseRevokeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LeaseServer).LeaseRevoke(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/etcdserverpb.Lease/LeaseRevoke",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LeaseServer).LeaseRevoke(ctx, req.(*LeaseRevokeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Lease_LeaseKeepAlive_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(LeaseServer).LeaseKeepAlive(&leaseLeaseKeepAliveServer{stream})
}

var _Lease_serviceDesc = grpc.ServiceDesc{
	ServiceName: "etcdserverpb.Lease",
	HandlerType: (*LeaseServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "LeaseGrant",
			Handler:    _Lease_LeaseGrant_Handler,
		},
		{
			MethodName: "LeaseRevoke",
			Handler:    _Lease_LeaseRevoke_Handler,
		},
		{
			MethodName: "LeaseTimeToLive",
			Handler:    _Lease_LeaseTimeToLive_Handler,
		},
		{
			MethodName: "LeaseLeases",
			Handler:    _Lease_LeaseLeases_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "LeaseKeepAlive",
			Handler:       _Lease_LeaseKeepAlive_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "rpc.proto",
}

type LeaseClient interface {
	LeaseGrant(ctx context.Context, in *LeaseGrantRequest, opts ...grpc.CallOption) (*LeaseGrantResponse, error)
	LeaseRevoke(ctx context.Context, in *LeaseRevokeRequest, opts ...grpc.CallOption) (*LeaseRevokeResponse, error)
	LeaseKeepAlive(ctx context.Context, opts ...grpc.CallOption) (Lease_LeaseKeepAliveClient, error)
	LeaseTimeToLive(ctx context.Context, in *LeaseTimeToLiveRequest, opts ...grpc.CallOption) (*LeaseTimeToLiveResponse, error)
	LeaseLeases(ctx context.Context, in *LeaseLeasesRequest, opts ...grpc.CallOption) (*LeaseLeasesResponse, error)
}

type leaseClient struct {
	cc *grpc.ClientConn
}

func NewLeaseClient(cc *grpc.ClientConn) LeaseClient {
	return &leaseClient{cc}
}

func (c *leaseClient) LeaseGrant(ctx context.Context, in *LeaseGrantRequest, opts ...grpc.CallOption) (*LeaseGrantResponse, error) {
	out := new(LeaseGrantResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Lease/LeaseGrant", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *leaseClient) LeaseRevoke(ctx context.Context, in *LeaseRevokeRequest, opts ...grpc.CallOption) (*LeaseRevokeResponse, error) {
	out := new(LeaseRevokeResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Lease/LeaseRevoke", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *leaseClient) LeaseKeepAlive(ctx context.Context, opts ...grpc.CallOption) (Lease_LeaseKeepAliveClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Lease_serviceDesc.Streams[0], "/etcdserverpb.Lease/LeaseKeepAlive", opts...)
	if err != nil {
		return nil, err
	}
	x := &leaseLeaseKeepAliveClient{stream}
	return x, nil
}

type Lease_LeaseKeepAliveClient interface {
	Send(*LeaseKeepAliveRequest) error
	Recv() (*LeaseKeepAliveResponse, error)
	grpc.ClientStream
}

type leaseLeaseKeepAliveClient struct {
	grpc.ClientStream
}

func (x *leaseLeaseKeepAliveClient) Send(m *LeaseKeepAliveRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *leaseLeaseKeepAliveClient) Recv() (*LeaseKeepAliveResponse, error) {
	m := new(LeaseKeepAliveResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *leaseClient) LeaseTimeToLive(ctx context.Context, in *LeaseTimeToLiveRequest, opts ...grpc.CallOption) (*LeaseTimeToLiveResponse, error) {
	out := new(LeaseTimeToLiveResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Lease/LeaseTimeToLive", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *leaseClient) LeaseLeases(ctx context.Context, in *LeaseLeasesRequest, opts ...grpc.CallOption) (*LeaseLeasesResponse, error) {
	out := new(LeaseLeasesResponse)
	err := c.cc.Invoke(ctx, "/etcdserverpb.Lease/LeaseLeases", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
