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
	"hash"
	"io"
	"os"
	"sync"

	"github.com/ls-2018/etcd_cn/etcd/wal/walpb"
	"github.com/ls-2018/etcd_cn/pkg/crc"
	"github.com/ls-2018/etcd_cn/pkg/ioutil"
)

// walPageBytes
const walPageBytes = 8 * minSectorSize // 8字节对齐
// encoder模块把会增量的计算crc和数据一起写入到wal文件中. 下面为encoder数据结构undefined
type encoder struct {
	mu        sync.Mutex
	bw        *ioutil.PageWriter
	crc       hash.Hash32
	buf       []byte // 缓存空间,默认为1M,降低数据分配的压力undefined,序列化时使用
	uint64buf []byte // 将数据变成特定格式的数据,大端、小端
}

func newEncoder(w io.Writer, prevCrc uint32, pageOffset int) *encoder {
	return &encoder{
		bw:  ioutil.NewPageWriter(w, walPageBytes, pageOffset),
		crc: crc.New(prevCrc, crcTable),
		// 1MB buffer
		buf:       make([]byte, 1024*1024),
		uint64buf: make([]byte, 8),
	}
}

// newFileEncoder 使用当前文件偏移,创建一个encoder用于写数据
func newFileEncoder(f *os.File, prevCrc uint32) (*encoder, error) {
	// prevCrc之前的crc码
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	return newEncoder(f, prevCrc, int(offset)), nil
}

func (e *encoder) encode(rec *walpb.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.crc.Write(rec.Data)
	rec.Crc = e.crc.Sum32()
	var (
		data []byte
		err  error
	)

	data, err = rec.Marshal()
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = e.bw.Write(data)
	return err
}

func (e *encoder) flush() error {
	e.mu.Lock()
	_, err := e.bw.FlushN()
	e.mu.Unlock()
	return err
}
