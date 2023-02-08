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

package wal

import (
	"bufio"
	"bytes"
	"hash"
	"io"
	"sync"

	"github.com/ls-2018/etcd_cn/etcd/wal/walpb"
	"github.com/ls-2018/etcd_cn/pkg/crc"
	"github.com/ls-2018/etcd_cn/pkg/pbutil"
	"github.com/ls-2018/etcd_cn/raft/raftpb"
)

const minSectorSize = 512

type decoder struct {
	mu  sync.Mutex
	brs []*bufio.Reader // 要读取的所有wal文件

	// lastValidOff file offset following the last valid decoded record
	lastValidOff int64 // 下一次decode的偏移量
	crc          hash.Hash32
}

func newDecoder(r ...io.Reader) *decoder {
	readers := make([]*bufio.Reader, len(r))
	for i := range r {
		readers[i] = bufio.NewReader(r[i])
	}
	return &decoder{
		brs: readers,
		crc: crc.New(0, crcTable),
	}
}

func (d *decoder) decode(rec *walpb.Record) error {
	rec.Reset()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.decodeRecord(rec)
}

// raft max message size is set to 1 MB in etcd etcd
// assume projects set reasonable message size limit,
// thus entry size should never exceed 10 MB

func (d *decoder) decodeRecord(rec *walpb.Record) error {
	if len(d.brs) == 0 {
		return io.EOF
	}

	line, _, err := bufio.NewReader(d.brs[0]).ReadLine()
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	length := len(line)
	a := make([]byte, length)
	all := 0
	for _, item := range line {
		if item == 0 {
			all += 1
		}
	}
	if all == length {
		return io.EOF
	}
	if bytes.Equal(a, line) {
		return io.EOF
	}

	if err := rec.Unmarshal(line); err != nil {
		return err
	}

	// skip crc checking if the record type is crcType
	if rec.Type != crcType {
		d.crc.Write(rec.Data)
		if err := rec.Validate(d.crc.Sum32()); err != nil {
			return err
		}
	}
	d.lastValidOff += int64(len(line)) + 1
	return nil
}

func (d *decoder) updateCRC(prevCrc uint32) {
	d.crc = crc.New(prevCrc, crcTable)
}

func (d *decoder) lastCRC() uint32 {
	return d.crc.Sum32()
}

func (d *decoder) lastOffset() int64 { return d.lastValidOff }

func mustUnmarshalEntry(d []byte) raftpb.Entry {
	var e raftpb.Entry
	pbutil.MustUnmarshal(&e, d)
	return e
}

func mustUnmarshalState(d []byte) raftpb.HardState {
	var s raftpb.HardState
	pbutil.MustUnmarshal(&s, d)
	return s
}
