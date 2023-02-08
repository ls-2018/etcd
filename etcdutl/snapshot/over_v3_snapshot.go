// Copyright 2018 The etcd Authors
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

package snapshot

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/ls-2018/etcd_cn/raft"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/fileutil"
	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/etcd/config"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/membership"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/snap"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2store"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/cindex"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"
	"github.com/ls-2018/etcd_cn/etcd/verify"
	"github.com/ls-2018/etcd_cn/etcd/wal"
	"github.com/ls-2018/etcd_cn/etcd/wal/walpb"
	"github.com/ls-2018/etcd_cn/offical/etcdserverpb"
	"github.com/ls-2018/etcd_cn/raft/raftpb"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

type Manager interface {
	Status(dbPath string) (Status, error) // 快照信息
	Restore(cfg RestoreConfig) error
}

// NewV3 v3版本的快照管理
func NewV3(lg *zap.Logger) Manager {
	if lg == nil {
		lg = zap.NewExample()
	}
	return &v3Manager{lg: lg}
}

type v3Manager struct {
	lg *zap.Logger

	name      string
	srcDbPath string
	walDir    string
	snapDir   string
	cl        *membership.RaftCluster

	skipHashCheck bool
}

func hasChecksum(n int64) bool {
	return (n % 512) == sha256.Size
}

// RestoreConfig configures snapshot restore operation.
type RestoreConfig struct {
	// SnapshotPath is the path of snapshot file to restore from.
	SnapshotPath string

	// Name is the human-readable name of this member.
	Name string

	// OutputDataDir is the target data directory to save restored data.
	// OutputDataDir should not conflict with existing etcd data directory.
	// If OutputDataDir already exists, it will return an error to prevent
	// unintended data directory overwrites.
	// If empty, defaults to "[Name].etcd" if not given.
	OutputDataDir string
	// OutputWALDir is the target WAL data directory.
	// If empty, defaults to "[OutputDataDir]/member/wal" if not given.
	OutputWALDir string

	// PeerURLs is a list of member's peer URLs to advertise to the rest of the cluster.
	PeerURLs []string

	// InitialCluster is the initial cluster configuration for restore bootstrap.
	InitialCluster string
	// InitialClusterToken is the initial cluster token for etcd cluster during restore bootstrap.
	InitialClusterToken string

	// SkipHashCheck is "true" to ignore snapshot integrity hash value
	// (required if copied from data directory).
	SkipHashCheck bool
}

// Restore restores a new etcd data directory from given snapshot file.
func (s *v3Manager) Restore(cfg RestoreConfig) error {
	pURLs, err := types.NewURLs(cfg.PeerURLs)
	if err != nil {
		return err
	}
	var ics types.URLsMap
	ics, err = types.NewURLsMap(cfg.InitialCluster)
	if err != nil {
		return err
	}

	srv := config.ServerConfig{
		Logger:              s.lg,
		Name:                cfg.Name,
		PeerURLs:            pURLs,
		InitialPeerURLsMap:  ics,
		InitialClusterToken: cfg.InitialClusterToken,
	}
	if err = srv.VerifyBootstrap(); err != nil {
		return err
	}

	s.cl, err = membership.NewClusterFromURLsMap(s.lg, cfg.InitialClusterToken, ics)
	if err != nil {
		return err
	}

	dataDir := cfg.OutputDataDir
	if dataDir == "" {
		dataDir = cfg.Name + ".etcd"
	}
	if fileutil.Exist(dataDir) && !fileutil.DirEmpty(dataDir) {
		return fmt.Errorf("data-dir %q not empty or could not be read", dataDir)
	}

	walDir := cfg.OutputWALDir
	if walDir == "" {
		walDir = filepath.Join(dataDir, "member", "wal")
	} else if fileutil.Exist(walDir) {
		return fmt.Errorf("wal-dir %q exists", walDir)
	}

	s.name = cfg.Name
	s.srcDbPath = cfg.SnapshotPath
	s.walDir = walDir
	s.snapDir = filepath.Join(dataDir, "member", "snap")
	s.skipHashCheck = cfg.SkipHashCheck

	s.lg.Info(
		"restoring snapshot",
		zap.String("path", s.srcDbPath),
		zap.String("wal-dir", s.walDir),
		zap.String("data-dir", dataDir),
		zap.String("snap-dir", s.snapDir),
		zap.Stack("stack"),
	)

	if err = s.saveDB(); err != nil {
		return err
	}
	hardstate, err := s.saveWALAndSnap()
	if err != nil {
		return err
	}

	if err := s.updateCIndex(hardstate.Commit, hardstate.Term); err != nil {
		return err
	}

	s.lg.Info(
		"restored snapshot",
		zap.String("path", s.srcDbPath),
		zap.String("wal-dir", s.walDir),
		zap.String("data-dir", dataDir),
		zap.String("snap-dir", s.snapDir),
	)

	return verify.VerifyIfEnabled(verify.Config{
		ExactIndex: true,
		Logger:     s.lg,
		DataDir:    dataDir,
	})
}

func (s *v3Manager) outDbPath() string {
	return filepath.Join(s.snapDir, "db")
}

// saveDB 将数据库快照复制到快照目录中.
func (s *v3Manager) saveDB() error {
	err := s.copyAndVerifyDB()
	if err != nil {
		return err
	}

	be := backend.NewDefaultBackend(s.outDbPath())
	defer be.Close()

	err = membership.TrimMembershipFromBackend(s.lg, be)
	if err != nil {
		return err
	}

	return nil
}

func (s *v3Manager) copyAndVerifyDB() error {
	srcf, ferr := os.Open(s.srcDbPath)
	if ferr != nil {
		return ferr
	}
	defer srcf.Close()

	// 获取快照完整性哈希值
	if _, err := srcf.Seek(-sha256.Size, io.SeekEnd); err != nil {
		return err
	}
	sha := make([]byte, sha256.Size)
	if _, err := srcf.Read(sha); err != nil {
		return err
	}
	if _, err := srcf.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if err := fileutil.CreateDirAll(s.snapDir); err != nil {
		return err
	}

	outDbPath := s.outDbPath()

	db, dberr := os.OpenFile(outDbPath, os.O_RDWR|os.O_CREATE, 0o600)
	if dberr != nil {
		return dberr
	}
	dbClosed := false
	defer func() {
		if !dbClosed {
			db.Close()
			dbClosed = true
		}
	}()
	if _, err := io.Copy(db, srcf); err != nil {
		return err
	}

	// truncate away integrity hash, if any.
	off, serr := db.Seek(0, io.SeekEnd)
	if serr != nil {
		return serr
	}
	hasHash := hasChecksum(off)
	if hasHash {
		if err := db.Truncate(off - sha256.Size); err != nil {
			return err
		}
	}

	if !hasHash && !s.skipHashCheck {
		return fmt.Errorf("snapshot missing hash but --skip-hash-check=false")
	}

	if hasHash && !s.skipHashCheck {
		// check for match
		if _, err := db.Seek(0, io.SeekStart); err != nil {
			return err
		}
		h := sha256.New()
		if _, err := io.Copy(h, db); err != nil {
			return err
		}
		dbsha := h.Sum(nil)
		if !reflect.DeepEqual(sha, dbsha) {
			return fmt.Errorf("expected sha256 %v, got %v", sha, dbsha)
		}
	}

	// db hash is OK, can now modify DB so it can be part of a new cluster
	db.Close()
	return nil
}

// saveWALAndSnap creates a WAL for the initial cluster
//
// TODO: This code ignores learners !!!
func (s *v3Manager) saveWALAndSnap() (*raftpb.HardState, error) {
	if err := fileutil.CreateDirAll(s.walDir); err != nil {
		return nil, err
	}

	// add members again to persist them to the store we create.
	st := v2store.New(etcdserver.StoreClusterPrefix, etcdserver.StoreKeysPrefix)
	s.cl.SetStore(st)
	be := backend.NewDefaultBackend(s.outDbPath())
	defer be.Close()
	s.cl.SetBackend(be)
	for _, m := range s.cl.Members() {
		s.cl.AddMember(m, true)
	}

	m := s.cl.MemberByName(s.name)
	md := &etcdserverpb.Metadata{NodeID: uint64(m.ID), ClusterID: uint64(s.cl.ID())}
	metadata, merr := md.Marshal()
	if merr != nil {
		return nil, merr
	}
	w, walerr := wal.Create(s.lg, s.walDir, metadata)
	if walerr != nil {
		return nil, walerr
	}
	defer w.Close()

	peers := make([]raft.Peer, len(s.cl.MemberIDs()))
	for i, id := range s.cl.MemberIDs() {
		ctx, err := json.Marshal((*s.cl).Member(id))
		if err != nil {
			return nil, err
		}
		peers[i] = raft.Peer{ID: uint64(id), Context: ctx}
	}

	ents := make([]raftpb.Entry, len(peers))
	nodeIDs := make([]uint64, len(peers))
	for i, p := range peers {
		nodeIDs[i] = p.ID
		cc := raftpb.ConfChangeV1{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  p.ID,
			Context: string(p.Context),
		}
		d, err := cc.Marshal()
		if err != nil {
			return nil, err
		}
		ents[i] = raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Term:  1,
			Index: uint64(i + 1),
			Data:  d, // ok
		}
	}

	commit, term := uint64(len(ents)), uint64(1)
	hardState := raftpb.HardState{
		Term:   term,
		Vote:   peers[0].ID,
		Commit: commit,
	}
	if err := w.Save(hardState, ents); err != nil {
		return nil, err
	}

	b, berr := st.Save()
	if berr != nil {
		return nil, berr
	}
	confState := raftpb.ConfState{
		Voters: nodeIDs,
	}
	raftSnap := raftpb.Snapshot{
		Data: b,
		Metadata: raftpb.SnapshotMetadata{
			Index:     commit,
			Term:      term,
			ConfState: confState,
		},
	}
	sn := snap.New(s.lg, s.snapDir)
	if err := sn.SaveSnap(raftSnap); err != nil {
		return nil, err
	}
	snapshot := walpb.Snapshot{Index: commit, Term: term, ConfState: &confState}
	return &hardState, w.SaveSnapshot(snapshot)
}

func (s *v3Manager) updateCIndex(commit uint64, term uint64) error {
	be := backend.NewDefaultBackend(s.outDbPath())
	defer be.Close()

	cindex.UpdateConsistentIndex(be.BatchTx(), commit, term, false)
	return nil
}

// ----------------------------------------  OVER  ----------------------------------------v----------

type Status struct {
	Hash      uint32 `json:"hash"`      // bolt.db哈希值
	Revision  int64  `json:"revision"`  // 修订版本
	TotalKey  int    `json:"totalKey"`  // 总key数
	TotalSize int64  `json:"totalSize"` // 实际存储大小
}

// Status 返回blot.db信息
func (s *v3Manager) Status(dbPath string) (ds Status, err error) {
	if _, err = os.Stat(dbPath); err != nil {
		return ds, err
	}

	db, err := bolt.Open(dbPath, 0o400, &bolt.Options{ReadOnly: true})
	if err != nil {
		return ds, err
	}
	defer db.Close()

	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	// 只读事务
	// Bolt 将其key以字节排序的顺序存储在存储桶中.这使得对这些键的顺序迭代非常快.要遍历键我们将使用光标
	if err = db.View(func(tx *bolt.Tx) error {
		// 首先检查快照文件的完整性
		var dbErrStrings []string
		for dbErr := range tx.Check() {
			dbErrStrings = append(dbErrStrings, dbErr.Error())
		}
		if len(dbErrStrings) > 0 {
			return fmt.Errorf("快照文件完整性检查失败.发现%d错误.\n"+strings.Join(dbErrStrings, "\n"), len(dbErrStrings))
		}
		ds.TotalSize = tx.Size()
		c := tx.Cursor()
		for next, _ := c.First(); next != nil; next, _ = c.Next() {
			b := tx.Bucket(next)
			if b == nil {
				return fmt.Errorf("无法获得桶的哈希值 %s", string(next))
			}
			if _, err := h.Write(next); err != nil {
				return fmt.Errorf("不能写入bucket %s : %v", string(next), err)
			}
			iskeyb := string(next) == "key"
			if err := b.ForEach(func(k, v []byte) error {
				if _, err := h.Write(k); err != nil {
					return fmt.Errorf("cannot write to bucket %s", err.Error())
				}
				if _, err := h.Write(v); err != nil {
					return fmt.Errorf("cannot write to bucket %s", err.Error())
				}
				if iskeyb {
					rev := bytesToRev(k)
					ds.Revision = rev.main
				}
				ds.TotalKey++
				return nil
			}); err != nil {
				return fmt.Errorf("不能写入bucket %s : %v", string(next), err)
			}
		}
		return nil
	}); err != nil {
		return ds, err
	}

	ds.Hash = h.Sum32()
	return ds, nil
}
