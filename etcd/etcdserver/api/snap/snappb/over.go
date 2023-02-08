package snappb

import (
	"encoding/json"
)

type temp struct {
	Crc  uint32 `protobuf:"varint,1,opt,name=crc" json:"crc"`
	Data string `protobuf:"bytes,2,opt,name=data" json:"data,omitempty"`
}

func (m *Snapshot) Marshal() (dAtA []byte, err error) {
	t := temp{
		Crc:  m.Crc,
		Data: string(m.Data),
	}
	return json.Marshal(t)
}

func (m *Snapshot) Unmarshal(dAtA []byte) error {
	t := temp{
		Crc:  m.Crc,
		Data: string(m.Data),
	}

	err := json.Unmarshal(dAtA, m)
	m.Crc = t.Crc
	m.Data = []byte(t.Data)
	return err
}
