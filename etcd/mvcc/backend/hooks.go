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

package backend

type HookFunc func(tx BatchTx)

// Hooks 允许事务有效期内执行的额外逻辑.
type Hooks interface {
	OnPreCommitUnsafe(tx BatchTx) // 事务提交前执行的钩子
}

type hooks struct {
	onPreCommitUnsafe HookFunc
}

func (h hooks) OnPreCommitUnsafe(tx BatchTx) {
	h.onPreCommitUnsafe(tx)
}

func NewHooks(onPreCommitUnsafe HookFunc) Hooks {
	return hooks{onPreCommitUnsafe: onPreCommitUnsafe}
}
