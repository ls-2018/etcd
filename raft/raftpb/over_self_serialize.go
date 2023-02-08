package raftpb

import (
	"encoding/json"
)

type A struct {
	Type    ConfChangeType
	NodeID  uint64
	Context string
	ID      uint64
}

func (m *ConfChangeV1) Marshal() (dAtA []byte, err error) {
	a := A{
		Type:    m.Type,
		NodeID:  m.NodeID,
		Context: string(m.Context),
		ID:      m.ID,
	}
	return json.Marshal(a)
}

func (m *ConfState) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *HardState) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *Message) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

type temp struct {
	Data     string
	Metadata SnapshotMetadata
}

func (m *Snapshot) Marshal() (dAtA []byte, err error) {
	t := temp{
		Data:     string(m.Data),
		Metadata: m.Metadata,
	}
	return json.Marshal(t)
}

func (m *SnapshotMetadata) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

type B struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  string
}

func (m *Entry) Marshal() (dAtA []byte, err error) {
	b := B{
		Term:  m.Term,
		Index: m.Index,
		Type:  m.Type,
		Data:  string(m.Data),
	}
	return json.Marshal(b)
}

func (m *ConfChangeSingle) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *ConfChangeV2) Marshal() (dAtA []byte, err error) {
	return json.Marshal(m)
}

func (m *Entry) Unmarshal(dAtA []byte) error {
	b := B{
		Term:  m.Term,
		Index: m.Index,
		Type:  m.Type,
		Data:  string(m.Data),
	}
	err := json.Unmarshal(dAtA, &b)
	m.Index = b.Index
	m.Type = b.Type
	m.Term = b.Term
	m.Data = []byte(b.Data) // ok
	return err
}

func (m *SnapshotMetadata) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *Snapshot) Unmarshal(dAtA []byte) error {
	t := temp{}
	err := json.Unmarshal(dAtA, &t)
	if err == nil {
		m.Data = []byte(t.Data)
		m.Metadata = t.Metadata
	}
	return err
}

func (m *Message) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *HardState) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *ConfState) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *ConfChangeV1) Unmarshal(dAtA []byte) error {
	a := A{}
	err := json.Unmarshal(dAtA, &a)
	m.Context = a.Context
	m.Type = a.Type
	m.ID = a.ID
	m.NodeID = a.NodeID
	return err
}

func (m *ConfChangeSingle) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}

func (m *ConfChangeV2) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}
