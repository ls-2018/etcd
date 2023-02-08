package authpb

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

type Permission_Type int32

const (
	READ      Permission_Type = 0
	WRITE     Permission_Type = 1
	READWRITE Permission_Type = 2
)

var PermissionTypeName = map[int32]string{
	0: "READ",
	1: "WRITE",
	2: "READWRITE",
}

var PermissionTypeValue = map[string]int32{
	"READ":      0,
	"WRITE":     1,
	"READWRITE": 2,
}

func (x Permission_Type) String() string {
	return proto.EnumName(PermissionTypeName, int32(x))
}

func (Permission_Type) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_8bbd6f3875b0e874, []int{2, 0}
}

type UserAddOptions struct {
	NoPassword           bool     `protobuf:"varint,1,opt,name=no_password,json=noPassword,proto3" json:"no_password,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *UserAddOptions) Reset()         { *m = UserAddOptions{} }
func (m *UserAddOptions) String() string { return proto.CompactTextString(m) }
func (*UserAddOptions) ProtoMessage()    {}
func (*UserAddOptions) Descriptor() ([]byte, []int) {
	return fileDescriptor_8bbd6f3875b0e874, []int{0}
}

type User struct {
	Name                 string          `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Password             string          `protobuf:"bytes,2,opt,name=password,proto3" json:"password,omitempty"`
	Roles                []string        `protobuf:"bytes,3,rep,name=roles,proto3" json:"roles,omitempty"`
	Options              *UserAddOptions `protobuf:"bytes,4,opt,name=options,proto3" json:"options,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *User) Reset()         { *m = User{} }
func (m *User) String() string { return proto.CompactTextString(m) }
func (*User) ProtoMessage()    {}
func (*User) Descriptor() ([]byte, []int) {
	return fileDescriptor_8bbd6f3875b0e874, []int{1}
}

// Permission 是赋予角色的权限.
type Permission struct {
	PermType             Permission_Type `protobuf:"varint,1,opt,name=permType,proto3,enum=authpb.Permission_Type" json:"permType,omitempty"`
	Key                  string          `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	RangeEnd             string          `protobuf:"bytes,3,opt,name=range_end,json=rangeEnd,proto3" json:"range_end,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *Permission) Reset()         { *m = Permission{} }
func (m *Permission) String() string { return proto.CompactTextString(m) }
func (*Permission) ProtoMessage()    {}
func (*Permission) Descriptor() ([]byte, []int) {
	return fileDescriptor_8bbd6f3875b0e874, []int{2}
}

// Role is a single entry in the bucket authRoles
type Role struct {
	Name                 string        `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	KeyPermission        []*Permission `protobuf:"bytes,2,rep,name=keyPermission,proto3" json:"keyPermission,omitempty"`
	XXX_NoUnkeyedLiteral struct{}      `json:"-"`
	XXX_unrecognized     []byte        `json:"-"`
	XXX_sizecache        int32         `json:"-"`
}

func (m *Role) Reset()         { *m = Role{} }
func (m *Role) String() string { return proto.CompactTextString(m) }
func (*Role) ProtoMessage()    {}
func (*Role) Descriptor() ([]byte, []int) {
	return fileDescriptor_8bbd6f3875b0e874, []int{3}
}

func init() {
	proto.RegisterEnum("authpb.Permission_Type", PermissionTypeName, PermissionTypeValue)
	proto.RegisterType((*UserAddOptions)(nil), "authpb.UserAddOptions")
	proto.RegisterType((*User)(nil), "authpb.User")
	proto.RegisterType((*Permission)(nil), "authpb.Permission")
	proto.RegisterType((*Role)(nil), "authpb.Role")
}

func init() { proto.RegisterFile("auth.proto", fileDescriptor_8bbd6f3875b0e874) }

var fileDescriptor_8bbd6f3875b0e874 = []byte{
	// 338 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x6c, 0x91, 0xcf, 0x4e, 0xea, 0x40,
	0x14, 0xc6, 0x3b, 0xb4, 0x70, 0xdb, 0xc3, 0x85, 0x90, 0x13, 0x72, 0x6f, 0x83, 0x49, 0x6d, 0xba,
	0x6a, 0x5c, 0x54, 0x85, 0x8d, 0x5b, 0x8c, 0x2c, 0x5c, 0x49, 0x26, 0x18, 0x97, 0xa4, 0xa4, 0x13,
	0x24, 0xc0, 0x4c, 0x33, 0x83, 0x31, 0x6c, 0x7c, 0x0e, 0x17, 0x3e, 0x10, 0x4b, 0x1e, 0x41, 0xf0,
	0x45, 0x4c, 0x67, 0xf8, 0x13, 0xa2, 0xbb, 0xef, 0x7c, 0xe7, 0xfb, 0x66, 0x7e, 0x99, 0x01, 0x48,
	0x5f, 0x16, 0xcf, 0x49, 0x2e, 0xc5, 0x42, 0x60, 0xa5, 0xd0, 0xf9, 0xa8, 0xd5, 0x1c, 0x8b, 0xb1,
	0xd0, 0xd6, 0x65, 0xa1, 0xcc, 0x36, 0xba, 0x86, 0xfa, 0xa3, 0x62, 0xb2, 0x9b, 0x65, 0x0f, 0xf9,
	0x62, 0x22, 0xb8, 0xc2, 0x73, 0xa8, 0x72, 0x31, 0xcc, 0x53, 0xa5, 0x5e, 0x85, 0xcc, 0x7c, 0x12,
	0x92, 0xd8, 0xa5, 0xc0, 0x45, 0x7f, 0xe7, 0x44, 0x6f, 0xe0, 0x14, 0x15, 0x44, 0x70, 0x78, 0x3a,
	0x67, 0x3a, 0xf1, 0x97, 0x6a, 0x8d, 0x2d, 0x70, 0x0f, 0xcd, 0x92, 0xf6, 0x0f, 0x33, 0x36, 0xa1,
	0x2c, 0xc5, 0x8c, 0x29, 0xdf, 0x0e, 0xed, 0xd8, 0xa3, 0x66, 0xc0, 0x2b, 0xf8, 0x23, 0xcc, 0xcd,
	0xbe, 0x13, 0x92, 0xb8, 0xda, 0xfe, 0x97, 0x18, 0xe0, 0xe4, 0x94, 0x8b, 0xee, 0x63, 0xd1, 0x07,
	0x01, 0xe8, 0x33, 0x39, 0x9f, 0x28, 0x35, 0x11, 0x1c, 0x3b, 0xe0, 0xe6, 0x4c, 0xce, 0x07, 0xcb,
	0xdc, 0xa0, 0xd4, 0xdb, 0xff, 0xf7, 0x27, 0x1c, 0x53, 0x49, 0xb1, 0xa6, 0x87, 0x20, 0x36, 0xc0,
	0x9e, 0xb2, 0xe5, 0x0e, 0xb1, 0x90, 0x78, 0x06, 0x9e, 0x4c, 0xf9, 0x98, 0x0d, 0x19, 0xcf, 0x7c,
	0xdb, 0xa0, 0x6b, 0xa3, 0xc7, 0xb3, 0xe8, 0x02, 0x1c, 0x5d, 0x73, 0xc1, 0xa1, 0xbd, 0xee, 0x5d,
	0xc3, 0x42, 0x0f, 0xca, 0x4f, 0xf4, 0x7e, 0xd0, 0x6b, 0x10, 0xac, 0x81, 0x57, 0x98, 0x66, 0x2c,
	0x45, 0x03, 0x70, 0xa8, 0x98, 0xb1, 0x5f, 0x9f, 0xe7, 0x06, 0x6a, 0x53, 0xb6, 0x3c, 0x62, 0xf9,
	0xa5, 0xd0, 0x8e, 0xab, 0x6d, 0xfc, 0x09, 0x4c, 0x4f, 0x83, 0xb7, 0xfe, 0x6a, 0x13, 0x58, 0xeb,
	0x4d, 0x60, 0xad, 0xb6, 0x01, 0x59, 0x6f, 0x03, 0xf2, 0xb9, 0x0d, 0xc8, 0xfb, 0x57, 0x60, 0x8d,
	0x2a, 0xfa, 0x23, 0x3b, 0xdf, 0x01, 0x00, 0x00, 0xff, 0xff, 0x61, 0x66, 0xc6, 0x9d, 0xf4, 0x01,
	0x00, 0x00,
}

func (m *UserAddOptions) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *User) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *Permission) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *Role) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *UserAddOptions) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *User) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *Permission) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *Role) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *UserAddOptions) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *User) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *Permission) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *Role) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

var (
	ErrInvalidLengthAuth        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowAuth          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupAuth = fmt.Errorf("proto: unexpected end of group")
)
