package mvccpb

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

type Event_EventType int32

const (
	PUT    Event_EventType = 0
	DELETE Event_EventType = 1
)

var Event_EventType_name = map[int32]string{
	0: "PUT",
	1: "DELETE",
}

var Event_EventType_value = map[string]int32{
	"PUT":    0,
	"DELETE": 1,
}

func (x Event_EventType) String() string {
	return proto.EnumName(Event_EventType_name, int32(x))
}

func (Event_EventType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_2216fe83c9c12408, []int{1, 0}
}

type KeyValue struct {
	Key            string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	CreateRevision int64  `protobuf:"varint,2,opt,name=create_revision,json=createRevision,proto3" json:"create_revision,omitempty"`
	ModRevision    int64  `protobuf:"varint,3,opt,name=mod_revision,json=modRevision,proto3" json:"mod_revision,omitempty"`
	// Version是key的版本.删除键会将该键的版本重置为0,对键的任何修改都会增加它的版本.
	Version int64  `protobuf:"varint,4,opt,name=version,proto3" json:"version,omitempty"`
	Value   string `protobuf:"bytes,5,opt,name=value,proto3" json:"value,omitempty"`
	Lease   int64  `protobuf:"varint,6,opt,name=lease,proto3" json:"lease,omitempty"`
}

func (m *KeyValue) Reset()         { *m = KeyValue{} }
func (m *KeyValue) String() string { return proto.CompactTextString(m) }
func (*KeyValue) ProtoMessage()    {}
func (*KeyValue) Descriptor() ([]byte, []int) {
	return fileDescriptor_2216fe83c9c12408, []int{0}
}

type Event struct {
	// type is the kind of event. If type is a PUT, it indicates
	// new data has been stored to the key. If type is a DELETE,
	// it indicates the key was deleted.
	Type Event_EventType `protobuf:"varint,1,opt,name=type,proto3,enum=mvccpb.Event_EventType" json:"type,omitempty"`
	// kv holds the KeyValue for the event.
	// A PUT event contains current kv pair.
	// A PUT event with kv.Version=1 indicates the creation of a key.
	// A DELETE/EXPIRE event contains the deleted key with
	// its modification revision set to the revision of deletion.
	Kv *KeyValue `protobuf:"bytes,2,opt,name=kv,proto3" json:"kv,omitempty"`
	// prev_kv holds the key-value pair before the event happens.
	PrevKv *KeyValue `protobuf:"bytes,3,opt,name=prev_kv,json=prevKv,proto3" json:"prev_kv,omitempty"`
}

func (m *Event) Reset()         { *m = Event{} }
func (m *Event) String() string { return proto.CompactTextString(m) }
func (*Event) ProtoMessage()    {}
func (*Event) Descriptor() ([]byte, []int) {
	return fileDescriptor_2216fe83c9c12408, []int{1}
}

var xxx_messageInfo_Event proto.InternalMessageInfo

func init() {
	proto.RegisterEnum("mvccpb.Event_EventType", Event_EventType_name, Event_EventType_value)
	proto.RegisterType((*KeyValue)(nil), "mvccpb.KeyValue")
	proto.RegisterType((*Event)(nil), "mvccpb.Event")
}

func init() { proto.RegisterFile("kv.proto", fileDescriptor_2216fe83c9c12408) }

var fileDescriptor_2216fe83c9c12408 = []byte{
	// 303 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x6c, 0x90, 0x41, 0x4e, 0xc2, 0x40,
	0x14, 0x86, 0x3b, 0x14, 0x0a, 0x3e, 0x08, 0x36, 0x13, 0x12, 0x27, 0x2e, 0x26, 0x95, 0x8d, 0x18,
	0x13, 0x4c, 0xf0, 0x06, 0xc6, 0xae, 0x70, 0x61, 0x1a, 0x74, 0x4b, 0x4a, 0x79, 0x21, 0xa4, 0x94,
	0x69, 0x4a, 0x9d, 0xa4, 0x37, 0x71, 0xef, 0xde, 0x73, 0xb0, 0xe4, 0x08, 0x52, 0x2f, 0x62, 0xfa,
	0xc6, 0xe2, 0xc6, 0xcd, 0xe4, 0xfd, 0xff, 0xff, 0x65, 0xe6, 0x7f, 0x03, 0x9d, 0x58, 0x8f, 0xd3,
	0x4c, 0xe5, 0x8a, 0x3b, 0x89, 0x8e, 0xa2, 0x74, 0x71, 0x39, 0x58, 0xa9, 0x95, 0x22, 0xeb, 0xae,
	0x9a, 0x4c, 0x3a, 0xfc, 0x64, 0xd0, 0x99, 0x62, 0xf1, 0x1a, 0x6e, 0xde, 0x90, 0xbb, 0x60, 0xc7,
	0x58, 0x08, 0xe6, 0xb1, 0x51, 0x2f, 0xa8, 0x46, 0x7e, 0x0d, 0xe7, 0x51, 0x86, 0x61, 0x8e, 0xf3,
	0x0c, 0xf5, 0x7a, 0xb7, 0x56, 0x5b, 0xd1, 0xf0, 0xd8, 0xc8, 0x0e, 0xfa, 0xc6, 0x0e, 0x7e, 0x5d,
	0x7e, 0x05, 0xbd, 0x44, 0x2d, 0xff, 0x28, 0x9b, 0xa8, 0x6e, 0xa2, 0x96, 0x27, 0x44, 0x40, 0x5b,
	0x63, 0x46, 0x69, 0x93, 0xd2, 0x5a, 0xf2, 0x01, 0xb4, 0x74, 0x55, 0x40, 0xb4, 0xe8, 0x65, 0x23,
	0x2a, 0x77, 0x83, 0xe1, 0x0e, 0x85, 0x43, 0xb4, 0x11, 0xc3, 0x0f, 0x06, 0x2d, 0x5f, 0xe3, 0x36,
	0xe7, 0xb7, 0xd0, 0xcc, 0x8b, 0x14, 0xa9, 0x6e, 0x7f, 0x72, 0x31, 0x36, 0x7b, 0x8e, 0x29, 0x34,
	0xe7, 0xac, 0x48, 0x31, 0x20, 0x88, 0x7b, 0xd0, 0x88, 0x35, 0x75, 0xef, 0x4e, 0xdc, 0x1a, 0xad,
	0x17, 0x0f, 0x1a, 0xb1, 0xe6, 0x37, 0xd0, 0x4e, 0x33, 0xd4, 0xf3, 0x58, 0x53, 0xf9, 0xff, 0x30,
	0xa7, 0x02, 0xa6, 0x7a, 0xe8, 0xc1, 0xd9, 0xe9, 0x7e, 0xde, 0x06, 0xfb, 0xf9, 0x65, 0xe6, 0x5a,
	0x1c, 0xc0, 0x79, 0xf4, 0x9f, 0xfc, 0x99, 0xef, 0xb2, 0x07, 0xb1, 0x3f, 0x4a, 0xeb, 0x70, 0x94,
	0xd6, 0xbe, 0x94, 0xec, 0x50, 0x4a, 0xf6, 0x55, 0x4a, 0xf6, 0xfe, 0x2d, 0xad, 0x85, 0x43, 0xff,
	0x7e, 0xff, 0x13, 0x00, 0x00, 0xff, 0xff, 0xb5, 0x45, 0x92, 0x5d, 0xa1, 0x01, 0x00, 0x00,
}

func (m *KeyValue) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *Event) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *KeyValue) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *Event) Size() (n int) {
	marshal, _ := json.Marshal(m)
	return len(marshal)
}

func (m *KeyValue) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *Event) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

var (
	ErrInvalidLengthKv        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowKv          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupKv = fmt.Errorf("proto: unexpected end of group")
)
