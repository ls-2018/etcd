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
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ls-2018/etcd_cn/raft"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/fileutil"
	"github.com/ls-2018/etcd_cn/etcd/wal/walpb"
	"github.com/ls-2018/etcd_cn/pkg/pbutil"
	"github.com/ls-2018/etcd_cn/raft/raftpb"

	"go.uber.org/zap"
)

const (
	metadataType     int64         = iota + 1 // 元数据类型,元数据会保存当前的node id和cluster id.
	entryType                                 // 日志条目
	stateType                                 // 存放的是集群当前的状态HardState,如果集群的状态有变化,就会在WAL中存放一个新集群状态数据.里面包括当前Term,当前竞选者、当前已经commit的日志.
	crcType                                   // 存放crc校验字段.读取数据时,会根据这个记录里的crc字段对前面已经读出来的数据进行校验.
	snapshotType                              // 存放snapshot的日志点.包括日志的Index和Term.
	warnSyncDuration = time.Second            // 是指在记录警告之前分配给fsync的时间量.
)

var (
	// SegmentSizeBytes is the preallocated size of each wal segment file.
	// The actual size might be larger than this. In general, the default
	// value should be used, but this is defined as an exported variable
	// so that tests can set a different segment size.
	SegmentSizeBytes int64 = 64 * 1000 * 1000 // 64MB

	ErrMetadataConflict             = errors.New("wal: conflicting metadata found")
	ErrFileNotFound                 = errors.New("wal: file not found")
	ErrCRCMismatch                  = errors.New("wal: crc mismatch")
	ErrSnapshotMismatch             = errors.New("wal: snapshot mismatch")
	ErrSnapshotNotFound             = errors.New("wal: snapshot not found")
	ErrSliceOutOfRange              = errors.New("wal: slice bounds out of range")
	ErrMaxWALEntrySizeLimitExceeded = errors.New("wal: max entry size limit exceeded")
	ErrDecoderNotFound              = errors.New("wal: decoder not found")
	crcTable                        = crc32.MakeTable(crc32.Castagnoli)
)

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
// WAL是稳定存储的一个逻辑表示.WAL要么处于读取模式,要么处于追加模式,但不能同时进行.
// 一个新创建的WAL处于追加模式,并准备好追加记录.一个刚打开的WAL处于读模式,并准备好读取记录.
// 在读出所有以前的记录后,WAL将准备好进行追加.
type WAL struct {
	lg           *zap.Logger
	dir          string           // wal文件的存储目录
	dirFile      *os.File         // 是一个用于重命名时同步的wal目录的fd.
	metadata     []byte           // wal文件构建后会写的第一个metadata记录
	state        raftpb.HardState // wal文件构建后会写的第一个state记录
	start        walpb.Snapshot   // wal开始的snapshot,代表读取wal时从这个snapshot的记录之后开始
	decoder      *decoder         // wal记录的反序列化器
	readClose    func() error     // 关闭反序列化器
	unsafeNoSync bool             //  非安全存储 默认是 false
	mu           sync.Mutex
	enti         uint64                 // 保存到wal的最新日志索引
	encoder      *encoder               // encoder to encode records
	locks        []*fileutil.LockedFile // 底层数据文件列表
	fp           *filePipeline
}

// Create 创建一个准备用于添加记录的WAL.给定的元数据被记录在每个WAL文件的头部,并且可以在文件打开后用ReadAll检索.
func Create(lg *zap.Logger, dirpath string, metadata []byte) (*WAL, error) {
	if Exist(dirpath) {
		return nil, os.ErrExist
	}

	if lg == nil {
		lg = zap.NewNop()
	}

	// 保持临时的WAL目录,这样WAL的初始化就会显得很原子化.
	tmpdirpath := filepath.Clean(dirpath) + ".tmp"
	if fileutil.Exist(tmpdirpath) {
		if err := os.RemoveAll(tmpdirpath); err != nil {
			return nil, err
		}
	}
	defer os.RemoveAll(tmpdirpath)

	if err := fileutil.CreateDirAll(tmpdirpath); err != nil {
		lg.Warn(
			"无法创建wal临时目录",
			zap.String("tmp-dir-path", tmpdirpath),
			zap.String("dir-path", dirpath),
			zap.Error(err),
		)
		return nil, err
	}

	p := filepath.Join(tmpdirpath, walName(0, 0))
	f, err := fileutil.LockFile(p, os.O_WRONLY|os.O_CREATE, fileutil.PrivateFileMode) // 阻塞
	if err != nil {
		lg.Warn(
			"未能存入一个初始WAL文件",
			zap.String("path", p),
			zap.Error(err),
		)
		return nil, err
	}
	// 跳到末尾
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		lg.Warn(
			"未能寻找到一个初始的WAL文件",
			zap.String("path", p),
			zap.Error(err),
		)
		return nil, err
	}
	// 预分配文件,大小为SegmentSizeBytes（64MB）
	if err = fileutil.Preallocate(f.File, SegmentSizeBytes, true); err != nil {
		lg.Warn(
			"未能预先分配一个初始的WAL文件",
			zap.String("path", p),
			zap.Int64("segment-bytes", SegmentSizeBytes),
			zap.Error(err),
		)
		return nil, err
	}

	w := &WAL{
		lg:       lg,
		dir:      dirpath,
		metadata: metadata,
	}
	w.encoder, err = newFileEncoder(f.File, 0)
	if err != nil {
		return nil, err
	}
	w.locks = append(w.locks, f)
	if err = w.saveCrc(0); err != nil {
		return nil, err
	}
	// 将metadataType类型的record记录在wal的header处
	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: metadata}); err != nil {
		return nil, err
	}
	// 保存空的snapshot
	if err = w.SaveSnapshot(walpb.Snapshot{}); err != nil {
		return nil, err
	}
	logDirPath := w.dir
	// 重命名,之前以.tmp结尾的文件,初始化完成之后重命名,类似原子操作
	if w, err = w.renameWAL(tmpdirpath); err != nil {
		lg.Warn(
			fmt.Sprintf("重命名失败  .%s.tmp --> %s", tmpdirpath, w.dir),
			zap.String("tmp-dir-path", tmpdirpath),
			zap.String("dir-path", logDirPath),
			zap.Error(err),
		)
		return nil, err
	}
	var perr error
	defer func() {
		if perr != nil {
			w.cleanupWAL(lg)
		}
	}()

	// 目录被重新命名;同步父目录以保持重命名.
	pdir, perr := fileutil.OpenDir(filepath.Dir(w.dir)) // ./raftexample/db
	if perr != nil {
		lg.Warn(
			"未能打开父数据目录",
			zap.String("parent-dir-path", filepath.Dir(w.dir)),
			zap.String("dir-path", w.dir),
			zap.Error(perr),
		)
		return nil, perr
	}
	dirCloser := func() error {
		if perr = pdir.Close(); perr != nil {
			lg.Warn(
				"failed to close the parent data directory file",
				zap.String("parent-dir-path", filepath.Dir(w.dir)),
				zap.String("dir-path", w.dir),
				zap.Error(perr),
			)
			return perr
		}
		return nil
	}
	if perr = fileutil.Fsync(pdir); perr != nil {
		dirCloser()
		lg.Warn(
			"未能同步父数据目录文件",
			zap.String("parent-dir-path", filepath.Dir(w.dir)),
			zap.String("dir-path", w.dir),
			zap.Error(perr),
		)
		return nil, perr
	}
	// 关闭目录
	if err = dirCloser(); err != nil {
		return nil, err
	}

	return w, nil
}

// SetUnsafeNoFsync ok
func (w *WAL) SetUnsafeNoFsync() {
	w.unsafeNoSync = true // 非安全存储 默认是 false
}

func (w *WAL) cleanupWAL(lg *zap.Logger) {
	var err error
	if err = w.Close(); err != nil {
		lg.Panic("failed to close WAL during cleanup", zap.Error(err))
	}
	brokenDirName := fmt.Sprintf("%s.broken.%v", w.dir, time.Now().Format("20060102.150405.999999"))
	if err = os.Rename(w.dir, brokenDirName); err != nil {
		lg.Panic(
			"failed to rename WAL during cleanup",
			zap.Error(err),
			zap.String("source-path", w.dir),
			zap.String("rename-path", brokenDirName),
		)
	}
}

// raftexample/db/raftexample-1.tmp ---> raftexample/db/raftexample-1
func (w *WAL) renameWAL(tmpdirpath string) (*WAL, error) {
	if err := os.RemoveAll(w.dir); err != nil { // 删除 raftexample/db/raftexample-1
		return nil, err
	}
	// 在非Windows平台上,重命名时要按住锁.释放锁并试图快速重新获得它可能是不稳定的,因为在此过程中,进程可能会分叉产生一个进程.
	// Go运行时将fds设置为执行时关闭,但在分叉和执行之间存在一个窗口,另一个进程持有锁.
	if err := os.Rename(tmpdirpath, w.dir); err != nil { // raftexample/db/raftexample-1.tmp ---> raftexample/db/raftexample-1
		if _, ok := err.(*os.LinkError); ok {
			return w.renameWALUnlock(tmpdirpath)
		}
		return nil, err
	}
	w.fp = newFilePipeline(w.lg, w.dir, SegmentSizeBytes)
	df, err := fileutil.OpenDir(w.dir)
	w.dirFile = df
	return w, err
}

func (w *WAL) renameWALUnlock(tmpdirpath string) (*WAL, error) {
	// rename of directory with locked files doesn't work on windows/cifs;
	// close the WAL to release the locks so the directory can be renamed.
	w.lg.Info(
		"closing WAL to release flock and retry directory renaming",
		zap.String("from", tmpdirpath),
		zap.String("to", w.dir),
	)
	w.Close()

	if err := os.Rename(tmpdirpath, w.dir); err != nil {
		return nil, err
	}

	// reopen and relock
	newWAL, oerr := Open(w.lg, w.dir, walpb.Snapshot{})
	if oerr != nil {
		return nil, oerr
	}
	if _, _, _, err := newWAL.ReadAll(); err != nil {
		newWAL.Close()
		return nil, err
	}
	return newWAL, nil
}

// Open 在给定的快照处打开WAL.这个快照应该是先前保存在WAL中的,否则下面的ReadAll会失败.
// 返回的WAL已经准备好读取,第一条记录将是给定sap之后的那条.在读出所有之前的记录之前,不能对WAL进行追加.
func Open(lg *zap.Logger, dirpath string, snap walpb.Snapshot) (*WAL, error) {
	w, err := openAtIndex(lg, dirpath, snap, true)
	if err != nil {
		return nil, err
	}
	if w.dirFile, err = fileutil.OpenDir(w.dir); err != nil { // ./raftexample/db/raftexample-1
		return nil, err
	}
	return w, nil
}

// OpenForRead only opens the wal files for read.
// Write on a read only wal panics.
func OpenForRead(lg *zap.Logger, dirpath string, snap walpb.Snapshot) (*WAL, error) {
	return openAtIndex(lg, dirpath, snap, false)
}

// 在指定位置打开wal
func openAtIndex(lg *zap.Logger, dirpath string, snap walpb.Snapshot, write bool) (*WAL, error) {
	if lg == nil {
		lg = zap.NewNop()
	}
	names, nameIndex, err := selectWALFiles(lg, dirpath, snap) // 选择合适的wal文件
	if err != nil {
		return nil, err
	}

	rs, ls, closer, err := openWALFiles(lg, dirpath, names, nameIndex, write) // 打开所有wal文件
	if err != nil {
		return nil, err
	}

	// 创建一个WAL准备读取
	w := &WAL{
		lg:        lg,
		dir:       dirpath,
		start:     snap,
		decoder:   newDecoder(rs...),
		readClose: closer,
		locks:     ls,
	}

	if write { // true
		// 写入重用读出的文件描述符;不要关闭,以便
		w.readClose = nil
		if _, _, err := parseWALName(filepath.Base(w.tail().Name())); err != nil {
			closer()
			return nil, err
		}
		w.fp = newFilePipeline(lg, w.dir, SegmentSizeBytes)
	}

	return w, nil
}

// 选择合适的wal文件
func selectWALFiles(lg *zap.Logger, dirpath string, snap walpb.Snapshot) ([]string, int, error) {
	names, err := readWALNames(lg, dirpath) // 返回指定目录下的所有wal文件
	if err != nil {
		return nil, -1, err
	}

	nameIndex, ok := searchIndex(lg, names, snap.Index) // 查找小于快照的第一个wal日志
	if !ok || !isValidSeq(lg, names[nameIndex:]) {      // 校验wal索引是否是增序
		err = ErrFileNotFound
		return nil, -1, err
	}

	return names, nameIndex, nil
}

// ok
func openWALFiles(lg *zap.Logger, dirpath string, names []string, nameIndex int, write bool) ([]io.Reader, []*fileutil.LockedFile, func() error, error) {
	rcs := make([]io.ReadCloser, 0)
	rs := make([]io.Reader, 0)
	ls := make([]*fileutil.LockedFile, 0)
	for _, name := range names[nameIndex:] {
		p := filepath.Join(dirpath, name)
		if write {
			l, err := fileutil.TryLockFile(p, os.O_RDWR, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(lg, rcs...)
				return nil, nil, nil, err
			}
			ls = append(ls, l)
			rcs = append(rcs, l)
		} else {
			rf, err := os.OpenFile(p, os.O_RDONLY, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(lg, rcs...)
				return nil, nil, nil, err
			}
			ls = append(ls, nil)
			rcs = append(rcs, rf)
		}
		rs = append(rs, rcs[len(rcs)-1])
	}

	closer := func() error { return closeAll(lg, rcs...) }

	return rs, ls, closer, nil
}

// ReadAll 读取所有的wal里的日志
func (w *WAL) ReadAll() (metadata []byte, state raftpb.HardState, ents []raftpb.Entry, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	rec := &walpb.Record{}
	if w.decoder == nil {
		return nil, state, nil, ErrDecoderNotFound
	}
	decoder := w.decoder
	var match bool
	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case entryType:
			e := mustUnmarshalEntry(rec.Data)
			// 0 <= e.Index-w.start.Index - 1 < len(ents)
			if e.Index > w.start.Index {
				// 防止 "panic:运行时错误:切片边界超出范围[:13038096702221461992],容量为0"
				up := e.Index - w.start.Index - 1 //
				if up > uint64(len(ents)) {
					// 在调用append前返回错误导致运行时恐慌
					return nil, state, nil, ErrSliceOutOfRange
				}
				// 下面这一行有可能覆盖一些 "未提交 "的条目.
				// wal只关注写入日志,不会校验日志的index是否重复,
				ents = append(ents[:up], e)
			}
			w.enti = e.Index // 保存到wal的最新日志索引

		case stateType: // 集群状态变化
			state = mustUnmarshalState(rec.Data)

		case metadataType:
			if metadata != nil && !bytes.Equal(metadata, rec.Data) {
				state.Reset()
				return nil, state, nil, ErrMetadataConflict
			}
			metadata = rec.Data

		case crcType: // 4
			crc := decoder.crc.Sum32()
			// current crc of decoder must match the crc of the record.
			// do no need to match 0 crc, since the decoder is a new one at this case.
			if crc != 0 && rec.Validate(crc) != nil {
				state.Reset()
				return nil, state, nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)

		case snapshotType:
			var snap walpb.Snapshot
			pbutil.MustUnmarshal(&snap, rec.Data)
			if snap.Index == w.start.Index {
				if snap.Term != w.start.Term {
					state.Reset()
					return nil, state, nil, ErrSnapshotMismatch
				}
				match = true
			}

		default:
			state.Reset()
			return nil, state, nil, fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}

	switch w.tail() {
	case nil:
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			state.Reset()
			return nil, state, nil, err
		}
	default:
		// 如果WAL是以写模式打开的,我们必须读取所有的条目.
		if err != io.EOF {
			state.Reset()
			return nil, state, nil, err
		}

		if _, err = w.tail().Seek(w.decoder.lastOffset(), io.SeekStart); err != nil { // 跳到末尾
			return nil, state, nil, err
		}
		if err = fileutil.ZeroToEnd(w.tail().File); err != nil { // 清空wal文件当前之后的数据,并固定分配文件空间
			return nil, state, nil, err
		}
	}

	err = nil
	if !match { // wal 中没有发现当前的快照记录
		err = ErrSnapshotNotFound
	}

	// 关闭decoder,禁止读取
	if w.readClose != nil {
		w.readClose()
		w.readClose = nil
	}
	w.start = walpb.Snapshot{}

	w.metadata = metadata

	if w.tail() != nil { // wal文件
		// 创建编码器(与解码器连锁crc),启用追加功能
		w.encoder, err = newFileEncoder(w.tail().File, w.decoder.lastCRC())
		if err != nil {
			return
		}
	}
	w.decoder = nil

	return metadata, state, ents, err
}

// ValidSnapshotEntries 返回给定目录下wal日志中的所有有效快照条目.如果快照条目的索引小于或等于最近提交的hardstate,则为有效.
func ValidSnapshotEntries(lg *zap.Logger, walDir string) ([]walpb.Snapshot, error) {
	var snaps []walpb.Snapshot
	var state raftpb.HardState
	var err error

	rec := &walpb.Record{}
	names, err := readWALNames(lg, walDir) // 获取wal目录下的所有wal文件
	if err != nil {
		return nil, err
	}

	// 在读模式下打开WAL文件,这样,当在其他地方以写模式打开同样的WAL时,就不会有冲突.
	rs, _, closer, err := openWALFiles(lg, walDir, names, 0, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closer != nil {
			closer()
		}
	}()

	// 从WAL文件的读者中创建一个新的解码器
	decoder := newDecoder(rs...)

	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case snapshotType: // 5
			var loadedSnap walpb.Snapshot
			pbutil.MustUnmarshal(&loadedSnap, rec.Data)
			snaps = append(snaps, loadedSnap)
		case stateType: // 3
			state = mustUnmarshalState(rec.Data)
		case crcType: // 4
			crc := decoder.crc.Sum32()
			// 解码器的当前crc必须与记录的crc相匹配
			if crc != 0 && rec.Validate(crc) != nil {
				return nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		}
	}
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	// 过滤任何打快照的行为
	n := 0
	for _, s := range snaps {
		if s.Index <= state.Commit {
			snaps[n] = s
			n++
		}
	}
	snaps = snaps[:n:n]
	return snaps, nil
}

// Verify reads through the given WAL and verifies that it is not corrupted.
// It creates a new decoder to read through the records of the given WAL.
// It does not conflict with any open WAL, but it is recommended not to
// call this function after opening the WAL for writing.
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
// If the loaded snap doesn't match with the expected one, it will
// return error ErrSnapshotMismatch.
func Verify(lg *zap.Logger, walDir string, snap walpb.Snapshot) (*raftpb.HardState, error) {
	var metadata []byte
	var err error
	var match bool
	var state raftpb.HardState

	rec := &walpb.Record{}

	if lg == nil {
		lg = zap.NewNop()
	}
	names, nameIndex, err := selectWALFiles(lg, walDir, snap)
	if err != nil {
		return nil, err
	}

	// open wal files in read mode, so that there is no conflict
	// when the same WAL is opened elsewhere in write mode
	rs, _, closer, err := openWALFiles(lg, walDir, names, nameIndex, false)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closer != nil {
			closer()
		}
	}()

	// create a new decoder from the readers on the WAL files
	decoder := newDecoder(rs...)

	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case metadataType:
			if metadata != nil && !bytes.Equal(metadata, rec.Data) {
				return nil, ErrMetadataConflict
			}
			metadata = rec.Data
		case crcType:
			crc := decoder.crc.Sum32()
			// Current crc of decoder must match the crc of the record.
			// We need not match 0 crc, since the decoder is a new one at this point.
			if crc != 0 && rec.Validate(crc) != nil {
				return nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		case snapshotType:
			var loadedSnap walpb.Snapshot
			pbutil.MustUnmarshal(&loadedSnap, rec.Data)
			if loadedSnap.Index == snap.Index {
				if loadedSnap.Term != snap.Term {
					return nil, ErrSnapshotMismatch
				}
				match = true
			}
		// We ignore all entry and state type records as these
		// are not necessary for validating the WAL contents
		case entryType:
		case stateType:
			pbutil.MustUnmarshal(&state, rec.Data)
		default:
			return nil, fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}

	// We do not have to read out all the WAL entries
	// as the decoder is opened in read mode.
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	if !match {
		return nil, ErrSnapshotNotFound
	}

	return &state, nil
}

// cut 当日志数据大于默认的64M时就会生成新的文件写入日志,新文件的第一条记录就是上一个wal文件最后的crc
func (w *WAL) cut() error {
	// close old wal file; truncate to avoid wasting space if an early cut
	off, serr := w.tail().Seek(0, io.SeekCurrent)
	if serr != nil {
		return serr
	}

	if err := w.tail().Truncate(off); err != nil {
		return err
	}

	if err := w.sync(); err != nil { // 日志截断？
		return err
	}

	fpath := filepath.Join(w.dir, walName(w.seq()+1, w.enti+1))

	// create a temp wal file with name sequence + 1, or truncate the existing one
	newTail, err := w.fp.Open()
	if err != nil {
		return err
	}

	// update writer and save the previous crc
	w.locks = append(w.locks, newTail)
	prevCrc := w.encoder.crc.Sum32()
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
	if err != nil {
		return err
	}

	if err = w.saveCrc(prevCrc); err != nil {
		return err
	}

	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: w.metadata}); err != nil {
		return err
	}

	if err = w.saveState(&w.state); err != nil {
		return err
	}

	// atomically move temp wal file to wal file
	if err = w.sync(); err != nil { // 移动临时wal文件到wal文件
		return err
	}

	off, err = w.tail().Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	if err = os.Rename(newTail.Name(), fpath); err != nil {
		return err
	}

	if err = fileutil.Fsync(w.dirFile); err != nil {
		return err
	}

	// reopen newTail with its new path so calls to Name() match the wal filename format
	newTail.Close()

	if newTail, err = fileutil.LockFile(fpath, os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return err
	}
	if _, err = newTail.Seek(off, io.SeekStart); err != nil {
		return err
	}

	w.locks[len(w.locks)-1] = newTail

	prevCrc = w.encoder.crc.Sum32()
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
	if err != nil {
		return err
	}

	w.lg.Info("created a new WAL segment", zap.String("path", fpath))
	return nil
}

// 强制wal日志刷盘
func (w *WAL) sync() error {
	if w.encoder != nil {
		if err := w.encoder.flush(); err != nil {
			return err
		}
	}
	fmt.Println("wal flush")

	if w.unsafeNoSync { //  非安全存储 默认是 false
		return nil
	}

	start := time.Now()
	// Fdatasync类似于fsync(),但不会刷新修改后的元数据,除非为了允许正确处理后续的数据检索而需要这些元数据.
	err := fileutil.Fdatasync(w.tail().File)

	took := time.Since(start)
	if took > warnSyncDuration {
		w.lg.Warn("缓慢 fdatasync", zap.Duration("took", took), zap.Duration("expected-duration", warnSyncDuration))
	}
	return err
}

// Sync 强制wal日志刷盘
func (w *WAL) Sync() error {
	return w.sync() // 强制刷盘
}

// ReleaseLockTo 释放锁,这些锁的索引比给定的索引小,但其中最大的一个除外.
// 例如,如果WAL持有锁1,2,3,4,5,6,ReleaseLockTo(4)将释放 锁1,2,但保留3.ReleaseLockTo(5)将释放1,2,3,但保留4.
func (w *WAL) ReleaseLockTo(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.locks) == 0 {
		return nil
	}

	var smaller int
	found := false
	for i, l := range w.locks {
		_, lockIndex, err := parseWALName(filepath.Base(l.Name()))
		if err != nil {
			return err
		}
		if lockIndex >= index {
			smaller = i - 1
			found = true
			break
		}
	}

	// if no lock index is greater than the release index, we can
	// release lock up to the last one(excluding).
	if !found {
		smaller = len(w.locks) - 1
	}

	if smaller <= 0 {
		return nil
	}

	for i := 0; i < smaller; i++ {
		if w.locks[i] == nil {
			continue
		}
		w.locks[i].Close()
	}
	w.locks = w.locks[smaller:]

	return nil
}

// Close closes the current WAL file and directory.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fp != nil {
		w.fp.Close()
		w.fp = nil
	}

	if w.tail() != nil {
		if err := w.sync(); err != nil { // 文件关闭时
			return err
		}
	}
	for _, l := range w.locks {
		if l == nil {
			continue
		}
		if err := l.Close(); err != nil {
			w.lg.Error("failed to close WAL", zap.Error(err))
		}
	}

	return w.dirFile.Close()
}

// 将日志保存到wal,更新wal写入的最新索引
func (w *WAL) saveEntry(e *raftpb.Entry) error {
	b := pbutil.MustMarshal(e)
	rec := &walpb.Record{Type: entryType, Data: b}
	if err := w.encoder.encode(rec); err != nil {
		return err
	}
	w.enti = e.Index
	return nil
}

// 写当前的存储状态
func (w *WAL) saveState(s *raftpb.HardState) error {
	if raft.IsEmptyHardState(*s) {
		return nil
	}
	w.state = *s
	b := pbutil.MustMarshal(s)
	rec := &walpb.Record{Type: stateType, Data: b}
	return w.encoder.encode(rec)
}

// Save 日志发送给Follower的同时,Leader会将日志落盘,即写到WAL中,
// 将raft交给上层应用的一些commit信息保存到wal
func (w *WAL) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	// 获取wal的写锁
	w.mu.Lock()
	defer w.mu.Unlock()
	// HardState变化或者新的日志条目则需要写wal
	if raft.IsEmptyHardState(st) && len(ents) == 0 {
		return nil
	}
	// 是否需要同步刷新磁盘
	mustSync := raft.MustSync(st, w.state, len(ents))

	// 将日志保存到wal,更新wal写入的最新索引
	for i := range ents {
		fmt.Printf("待刷盘---> wal.Save %s\n", string(ents[i].Data))
		if err := w.saveEntry(&ents[i]); err != nil {
			return err
		}
	}
	// 持久化HardState, HardState表示服务器当前状态,定义在raft.pb.go,主要包含Term、Vote、Commit
	if err := w.saveState(&st); err != nil {
		return err
	}
	// 判断文件大小是否超过最大值
	// 获取最后一个LockedFile的大小（已经使用的）
	curOff, err := w.tail().Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if curOff < SegmentSizeBytes {
		if mustSync {
			return w.sync() // 写日志时,判断是否刷盘
		}
		return nil
	}
	// 否则执行切割（也就是说明,WAL文件是可以超过64MB的）
	return w.cut()
}

// SaveSnapshot 保存一条生成快照的日志
func (w *WAL) SaveSnapshot(e walpb.Snapshot) error {
	b := pbutil.MustMarshal(&e)

	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &walpb.Record{Type: snapshotType, Data: b}
	if err := w.encoder.encode(rec); err != nil {
		return err
	}
	// 只有当快照领先于最后的索引时才更新enti
	if w.enti < e.Index {
		w.enti = e.Index
	}
	return w.sync() // 保存快照时,刷盘
}

// 保存
func (w *WAL) saveCrc(prevCrc uint32) error {
	return w.encoder.encode(&walpb.Record{Type: crcType, Crc: prevCrc})
}

// 返回最后一个锁文件
func (w *WAL) tail() *fileutil.LockedFile {
	if len(w.locks) > 0 {
		return w.locks[len(w.locks)-1] // 返回最后一个锁文件
	}
	return nil
}

func (w *WAL) seq() uint64 {
	t := w.tail()
	if t == nil {
		return 0
	}
	seq, _, err := parseWALName(filepath.Base(t.Name()))
	if err != nil {
		w.lg.Fatal("解析WAL名称失败", zap.String("name", t.Name()), zap.Error(err))
	}
	return seq
}

func closeAll(lg *zap.Logger, rcs ...io.ReadCloser) error {
	stringArr := make([]string, 0)
	for _, f := range rcs {
		if err := f.Close(); err != nil {
			lg.Warn("failed to close: ", zap.Error(err))
			stringArr = append(stringArr, err.Error())
		}
	}
	if len(stringArr) == 0 {
		return nil
	}
	return errors.New(strings.Join(stringArr, ", "))
}
