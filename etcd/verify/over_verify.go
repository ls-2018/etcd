// Copyright 2021 The etcd Authors
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

package verify

import (
	"fmt"
	"os"

	"github.com/ls-2018/etcd_cn/etcd/datadir"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/cindex"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"
	wal2 "github.com/ls-2018/etcd_cn/etcd/wal"
	"github.com/ls-2018/etcd_cn/etcd/wal/walpb"
	"github.com/ls-2018/etcd_cn/raft/raftpb"
	"go.uber.org/zap"
)

const (
	ENV_VERIFY           = "ETCD_VERIFY"
	ENV_VERIFY_ALL_VALUE = "all"
)

type Config struct {
	// DataDir is a root directory where the data being verified are stored.
	DataDir string

	// ExactIndex requires consistent_index in backend exactly match the last committed WAL entry.
	// Usually backend's consistent_index needs to be <= WAL.commit, but for backups the match
	// is expected to be exact.
	ExactIndex bool

	Logger *zap.Logger
}

// Verify performs consistency checks of given etcd data-directory.
// The errors are reported as the returned error, but for some situations
// the function can also panic.
// The function is expected to work on not-in-use data model, i.e.
// no file-locks should be taken. Verify does not modified the data.
func Verify(cfg Config) error {
	lg := cfg.Logger
	if lg == nil {
		lg = zap.NewNop()
	}

	var err error
	lg.Info("verification of persisted state", zap.String("data-dir", cfg.DataDir))
	defer func() {
		if err != nil {
			lg.Error("verification of persisted state failed",
				zap.String("data-dir", cfg.DataDir),
				zap.Error(err))
		} else if r := recover(); r != nil {
			lg.Error("verification of persisted state failed",
				zap.String("data-dir", cfg.DataDir))
			panic(r)
		} else {
			lg.Info("verification of persisted state successful", zap.String("data-dir", cfg.DataDir))
		}
	}()

	beConfig := backend.DefaultBackendConfig()
	beConfig.Path = datadir.ToBackendFileName(cfg.DataDir)
	beConfig.Logger = cfg.Logger

	be := backend.New(beConfig)
	defer be.Close()

	snapshot, hardstate, err := validateWal(cfg)
	if err != nil {
		return err
	}

	// TODO: Perform validation of consistency of membership between
	// backend/members & WAL confstate (and maybe storev2 if still exists).

	return validateConsistentIndex(cfg, hardstate, snapshot, be)
}

// VerifyIfEnabled 根据ETCD_VERIFY环境设置执行校验.
func VerifyIfEnabled(cfg Config) error {
	if os.Getenv(ENV_VERIFY) == ENV_VERIFY_ALL_VALUE {
		return Verify(cfg)
	}
	return nil
}

// MustVerifyIfEnabled 根据ETCD_VERIFY环境设置执行验证,发现问题就退出.
func MustVerifyIfEnabled(cfg Config) {
	if err := VerifyIfEnabled(cfg); err != nil {
		cfg.Logger.Fatal("验证失败",
			zap.String("data-dir", cfg.DataDir),
			zap.Error(err))
	}
}

func validateConsistentIndex(cfg Config, hardstate *raftpb.HardState, snapshot *walpb.Snapshot, be backend.Backend) error {
	tx := be.BatchTx()
	index, term := cindex.ReadConsistentIndex(tx)
	if cfg.ExactIndex && index != hardstate.Commit {
		return fmt.Errorf("backend.ConsistentIndex (%v) expected == WAL.HardState.commit (%v)", index, hardstate.Commit)
	}
	if cfg.ExactIndex && term != hardstate.Term {
		return fmt.Errorf("backend.Term (%v) expected == WAL.HardState.term, (%v)", term, hardstate.Term)
	}
	if index > hardstate.Commit {
		return fmt.Errorf("backend.ConsistentIndex (%v)必须是<= WAL.HardState.commit (%v)", index, hardstate.Commit)
	}
	if term > hardstate.Term {
		return fmt.Errorf("backend.Term (%v)必须是<= WAL.HardState.term, (%v)", term, hardstate.Term)
	}

	if index < snapshot.Index {
		return fmt.Errorf("backend.ConsistentIndex (%v)必须是>= last snapshot index (%v)", index, snapshot.Index)
	}

	cfg.Logger.Info("verification: consistentIndex OK", zap.Uint64("backend-consistent-index", index), zap.Uint64("hardstate-commit", hardstate.Commit))
	return nil
}

func validateWal(cfg Config) (*walpb.Snapshot, *raftpb.HardState, error) {
	walDir := datadir.ToWalDir(cfg.DataDir)

	walSnaps, err := wal2.ValidSnapshotEntries(cfg.Logger, walDir)
	if err != nil {
		return nil, nil, err
	}

	snapshot := walSnaps[len(walSnaps)-1]
	hardstate, err := wal2.Verify(cfg.Logger, walDir, snapshot)
	if err != nil {
		return nil, nil, err
	}
	return &snapshot, hardstate, nil
}
