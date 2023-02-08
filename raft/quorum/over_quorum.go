// Copyright 2019 The etcd Authors
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

package quorum

import (
	"math"
	"strconv"
)

// Index raft日志索引
type Index uint64

func (i Index) String() string {
	if i == math.MaxUint64 {
		return "∞"
	}
	return strconv.FormatUint(uint64(i), 10)
}

type AckedIndexer interface {
	AckedIndex(voterID uint64) (idx Index, found bool)
}

//go:generate stringer -type=VoteResult
type VoteResult uint8

const (
	VotePending VoteResult = 1 + iota // 竞选中
	VoteLost                          // 竞选失败
	VoteWon                           // 竞选获胜
)
