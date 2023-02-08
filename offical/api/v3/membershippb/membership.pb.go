package membershippb

import (
	"encoding/json"
	fmt "fmt"
	math "math"

	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/golang/protobuf/proto"
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

// RaftAttributes represents the raft related attributes of an etcd member.
type RaftAttributes struct {
	// peerURLs is the list of peers in the raft cluster.
	PeerUrls []string `protobuf:"bytes,1,rep,name=peer_urls,json=peerUrls,proto3" json:"peer_urls,omitempty"`
	// isLearner indicates if the member is raft learner.
	IsLearner            bool     `protobuf:"varint,2,opt,name=is_learner,json=isLearner,proto3" json:"is_learner,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RaftAttributes) Reset()         { *m = RaftAttributes{} }
func (m *RaftAttributes) String() string { return proto.CompactTextString(m) }
func (*RaftAttributes) ProtoMessage()    {}
func (*RaftAttributes) Descriptor() ([]byte, []int) {
	return fileDescriptor_949fe0d019050ef5, []int{0}
}

// Attributes represents all the non-raft related attributes of an etcd member.
type Attributes struct {
	Name                 string   `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	ClientUrls           []string `protobuf:"bytes,2,rep,name=client_urls,json=clientUrls,proto3" json:"client_urls,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Attributes) Reset()         { *m = Attributes{} }
func (m *Attributes) String() string { return proto.CompactTextString(m) }
func (*Attributes) ProtoMessage()    {}
func (*Attributes) Descriptor() ([]byte, []int) {
	return fileDescriptor_949fe0d019050ef5, []int{1}
}

type Member struct {
	ID                   uint64          `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
	RaftAttributes       *RaftAttributes `protobuf:"bytes,2,opt,name=raft_attributes,json=raftAttributes,proto3" json:"raft_attributes,omitempty"`
	MemberAttributes     *Attributes     `protobuf:"bytes,3,opt,name=member_attributes,json=memberAttributes,proto3" json:"member_attributes,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *Member) Reset()         { *m = Member{} }
func (m *Member) String() string { return proto.CompactTextString(m) }
func (*Member) ProtoMessage()    {}
func (*Member) Descriptor() ([]byte, []int) {
	return fileDescriptor_949fe0d019050ef5, []int{2}
}

type ClusterVersionSetRequest struct {
	Ver                  string   `protobuf:"bytes,1,opt,name=ver,proto3" json:"ver,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ClusterVersionSetRequest) Reset()         { *m = ClusterVersionSetRequest{} }
func (m *ClusterVersionSetRequest) String() string { return proto.CompactTextString(m) }
func (*ClusterVersionSetRequest) ProtoMessage()    {}
func (*ClusterVersionSetRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_949fe0d019050ef5, []int{3}
}

type ClusterMemberAttrSetRequest struct {
	Member_ID            uint64      `protobuf:"varint,1,opt,name=member_ID,json=memberID,proto3" json:"member_ID,omitempty"`
	MemberAttributes     *Attributes `protobuf:"bytes,2,opt,name=member_attributes,json=memberAttributes,proto3" json:"member_attributes,omitempty"`
	XXX_NoUnkeyedLiteral struct{}    `json:"-"`
	XXX_unrecognized     []byte      `json:"-"`
	XXX_sizecache        int32       `json:"-"`
}

func (m *ClusterMemberAttrSetRequest) Reset()         { *m = ClusterMemberAttrSetRequest{} }
func (m *ClusterMemberAttrSetRequest) String() string { return proto.CompactTextString(m) }
func (*ClusterMemberAttrSetRequest) ProtoMessage()    {}
func (*ClusterMemberAttrSetRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_949fe0d019050ef5, []int{4}
}

type DowngradeInfoSetRequest struct {
	Enabled              bool     `protobuf:"varint,1,opt,name=enabled,proto3" json:"enabled,omitempty"`
	Ver                  string   `protobuf:"bytes,2,opt,name=ver,proto3" json:"ver,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *DowngradeInfoSetRequest) Reset()         { *m = DowngradeInfoSetRequest{} }
func (m *DowngradeInfoSetRequest) String() string { return proto.CompactTextString(m) }
func (*DowngradeInfoSetRequest) ProtoMessage()    {}
func (*DowngradeInfoSetRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_949fe0d019050ef5, []int{5}
}

func init() {
	proto.RegisterType((*RaftAttributes)(nil), "membershippb.RaftAttributes")
	proto.RegisterType((*Attributes)(nil), "membershippb.Attributes")
	proto.RegisterType((*Member)(nil), "membershippb.Member")
	proto.RegisterType((*ClusterVersionSetRequest)(nil), "membershippb.ClusterVersionSetRequest")
	proto.RegisterType((*ClusterMemberAttrSetRequest)(nil), "membershippb.ClusterMemberAttrSetRequest")
	proto.RegisterType((*DowngradeInfoSetRequest)(nil), "membershippb.DowngradeInfoSetRequest")
}

func init() { proto.RegisterFile("membership.proto", fileDescriptor_949fe0d019050ef5) }

var fileDescriptor_949fe0d019050ef5 = []byte{
	// 367 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x94, 0x92, 0xc1, 0x4e, 0xf2, 0x40,
	0x14, 0x85, 0x99, 0x42, 0xf8, 0xdb, 0xcb, 0x1f, 0xc4, 0x09, 0x89, 0x8d, 0x68, 0x25, 0x5d, 0xb1,
	0x30, 0x98, 0xe8, 0x13, 0xa0, 0xb0, 0x20, 0x81, 0xcd, 0x18, 0xdd, 0x92, 0x56, 0x2e, 0xd8, 0xa4,
	0x74, 0xea, 0xcc, 0x54, 0xd7, 0xbe, 0x85, 0x4f, 0xe0, 0xb3, 0xb0, 0xf4, 0x11, 0x14, 0x5f, 0xc4,
	0x74, 0x5a, 0x4a, 0x49, 0xdc, 0xb8, 0xbb, 0x3d, 0xbd, 0xf7, 0x9c, 0xf3, 0x35, 0x85, 0xd6, 0x0a,
	0x57, 0x3e, 0x0a, 0xf9, 0x18, 0xc4, 0xfd, 0x58, 0x70, 0xc5, 0xe9, 0xff, 0x9d, 0x12, 0xfb, 0xc7,
	0xed, 0x25, 0x5f, 0x72, 0xfd, 0xe2, 0x22, 0x9d, 0xb2, 0x1d, 0x77, 0x02, 0x4d, 0xe6, 0x2d, 0xd4,
	0x40, 0x29, 0x11, 0xf8, 0x89, 0x42, 0x49, 0x3b, 0x60, 0xc5, 0x88, 0x62, 0x96, 0x88, 0x50, 0xda,
	0xa4, 0x5b, 0xed, 0x59, 0xcc, 0x4c, 0x85, 0x3b, 0x11, 0x4a, 0x7a, 0x0a, 0x10, 0xc8, 0x59, 0x88,
	0x9e, 0x88, 0x50, 0xd8, 0x46, 0x97, 0xf4, 0x4c, 0x66, 0x05, 0x72, 0x92, 0x09, 0xee, 0x00, 0xa0,
	0xe4, 0x44, 0xa1, 0x16, 0x79, 0x2b, 0xb4, 0x49, 0x97, 0xf4, 0x2c, 0xa6, 0x67, 0x7a, 0x06, 0x8d,
	0x87, 0x30, 0xc0, 0x48, 0x65, 0xfe, 0x86, 0xf6, 0x87, 0x4c, 0x4a, 0x13, 0xdc, 0x77, 0x02, 0xf5,
	0xa9, 0xee, 0x4d, 0x9b, 0x60, 0x8c, 0x87, 0xfa, 0xba, 0xc6, 0x8c, 0xf1, 0x90, 0x8e, 0xe0, 0x40,
	0x78, 0x0b, 0x35, 0xf3, 0x8a, 0x08, 0xdd, 0xa0, 0x71, 0x79, 0xd2, 0x2f, 0x93, 0xf6, 0xf7, 0x81,
	0x58, 0x53, 0xec, 0x03, 0x8e, 0xe0, 0x30, 0x5b, 0x2f, 0x1b, 0x55, 0xb5, 0x91, 0xbd, 0x6f, 0x54,
	0x32, 0xc9, 0xbf, 0xee, 0x4e, 0x71, 0xcf, 0xc1, 0xbe, 0x09, 0x13, 0xa9, 0x50, 0xdc, 0xa3, 0x90,
	0x01, 0x8f, 0x6e, 0x51, 0x31, 0x7c, 0x4a, 0x50, 0x2a, 0xda, 0x82, 0xea, 0x33, 0x8a, 0x1c, 0x3c,
	0x1d, 0xdd, 0x57, 0x02, 0x9d, 0x7c, 0x7d, 0x5a, 0x38, 0x95, 0x2e, 0x3a, 0x60, 0xe5, 0xa5, 0x0a,
	0x64, 0x33, 0x13, 0x34, 0xf8, 0x2f, 0x8d, 0x8d, 0x3f, 0x37, 0x1e, 0xc1, 0xd1, 0x90, 0xbf, 0x44,
	0x4b, 0xe1, 0xcd, 0x71, 0x1c, 0x2d, 0x78, 0x29, 0xde, 0x86, 0x7f, 0x18, 0x79, 0x7e, 0x88, 0x73,
	0x1d, 0x6e, 0xb2, 0xed, 0xe3, 0x16, 0xc5, 0x28, 0x50, 0xae, 0xdb, 0xeb, 0x2f, 0xa7, 0xb2, 0xde,
	0x38, 0xe4, 0x63, 0xe3, 0x90, 0xcf, 0x8d, 0x43, 0xde, 0xbe, 0x9d, 0x8a, 0x5f, 0xd7, 0xff, 0xd3,
	0xd5, 0x4f, 0x00, 0x00, 0x00, 0xff, 0xff, 0xdc, 0x93, 0x7d, 0x0b, 0x87, 0x02, 0x00, 0x00,
}

var (
	ErrInvalidLengthMembership        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowMembership          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupMembership = fmt.Errorf("proto: unexpected end of group")
)

func (m *RaftAttributes) Marshal() (dAtA []byte, err error)              { return json.Marshal(m) }
func (m *Attributes) Marshal() (dAtA []byte, err error)                  { return json.Marshal(m) }
func (m *Member) Marshal() (dAtA []byte, err error)                      { return json.Marshal(m) }
func (m *ClusterVersionSetRequest) Marshal() (dAtA []byte, err error)    { return json.Marshal(m) }
func (m *ClusterMemberAttrSetRequest) Marshal() (dAtA []byte, err error) { return json.Marshal(m) }
func (m *DowngradeInfoSetRequest) Marshal() (dAtA []byte, err error)     { return json.Marshal(m) }
func (m *RaftAttributes) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *Attributes) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *Member) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *ClusterVersionSetRequest) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *ClusterMemberAttrSetRequest) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *DowngradeInfoSetRequest) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}
func (m *RaftAttributes) Unmarshal(dAtA []byte) error              { return json.Unmarshal(dAtA, m) }
func (m *Attributes) Unmarshal(dAtA []byte) error                  { return json.Unmarshal(dAtA, m) }
func (m *Member) Unmarshal(dAtA []byte) error                      { return json.Unmarshal(dAtA, m) }
func (m *ClusterVersionSetRequest) Unmarshal(dAtA []byte) error    { return json.Unmarshal(dAtA, m) }
func (m *ClusterMemberAttrSetRequest) Unmarshal(dAtA []byte) error { return json.Unmarshal(dAtA, m) }
func (m *DowngradeInfoSetRequest) Unmarshal(dAtA []byte) error     { return json.Unmarshal(dAtA, m) }
