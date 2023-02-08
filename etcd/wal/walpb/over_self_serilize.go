package walpb

import (
	"encoding/json"
)

type Temp struct {
	Type int64
	Crc  uint32
	Data string
}

func (m *Record) Marshal() (dAtA []byte, err error) {
	return json.Marshal(Temp{
		Type: m.Type,
		Crc:  m.Crc,
		Data: string(m.Data),
	})
}

func (m *Record) Unmarshal(dAtA []byte) error {
	a := Temp{}
	err := json.Unmarshal(dAtA, &a)
	if err != nil {
		return err
	}
	m.Type = a.Type
	m.Crc = a.Crc
	m.Data = []byte(a.Data)
	return nil
}

func (m *Snapshot) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *Snapshot) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}
