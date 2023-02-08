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

package mvcc

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/btree"
	"go.uber.org/zap"
)

var ErrRevisionNotFound = errors.New("mvcc: 修订版本没有找到")

// keyIndex
// key的删除操作将在末尾追加删除版本
// 当前代,并创建一个新的空代.
type keyIndex struct {
	Key         string       // Key
	Modified    revision     // 一个key 最新修改的revision .
	Generations []generation // 每次新建都会创建一个,删除然后新建也会生成一个
}

// generation  包含一个key的多个版本.
type generation struct {
	VersionCount int64      // 记录对当前key 有几个版本
	Created      revision   // 第一次创建时的索引信息
	Revs         []revision // 当值存在以后,对该值的修改记录
}

type revision struct {
	Main int64 // 一个全局递增的主版本号,随put/txn/delete事务递增,一个事务内的key main版本号是一致的
	Sub  int64 // 一个事务内的子版本号,从0开始随事务内put/delete操作递增
}

func (ki *keyIndex) restore(lg *zap.Logger, created, modified revision, ver int64) {
	if len(ki.Generations) != 0 {
		lg.Panic(
			"'restore' got an unexpected non-empty Generations",
			zap.Int("Generations-size", len(ki.Generations)),
		)
	}
	ki.Modified = modified
	g := generation{Created: created, VersionCount: ver, Revs: []revision{modified}}
	ki.Generations = append(ki.Generations, g)
}

// tombstone puts a revision, pointing to a tombstone, to the keyIndex.
// It also creates a new empty generation in the keyIndex.
// It returns ErrRevisionNotFound when tombstone on an empty generation.
func (ki *keyIndex) tombstone(lg *zap.Logger, main int64, sub int64) error {
	// 当然如果 keyIndex 中的最大版本号被打了删除标记 (tombstone), 就会从 treeIndex 中删除这个 keyIndex,否则会出现内存泄露.
	if ki.isEmpty() {
		lg.Panic(
			"'tombstone' got an unexpected empty keyIndex",
			zap.String("Key", string(ki.Key)),
		)
	}
	if ki.Generations[len(ki.Generations)-1].isEmpty() {
		return ErrRevisionNotFound
	}
	ki.put(lg, main, sub)
	ki.Generations = append(ki.Generations, generation{})
	return nil
}

// since returns revisions since the given rev. Only the revision with the
// largest Sub revision will be returned if multiple revisions have the same
// Main revision.
func (ki *keyIndex) since(lg *zap.Logger, rev int64) []revision {
	if ki.isEmpty() {
		lg.Panic("'since' 得到一个意外的空keyIndex", zap.String("Key", ki.Key))
	}
	since := revision{rev, 0}
	var gi int
	// find the Generations to start checking
	for gi = len(ki.Generations) - 1; gi > 0; gi-- {
		g := ki.Generations[gi]
		if g.isEmpty() {
			continue
		}
		if since.GreaterThan(g.Created) {
			break
		}
	}

	var revs []revision
	var last int64
	for ; gi < len(ki.Generations); gi++ {
		for _, r := range ki.Generations[gi].Revs {
			if since.GreaterThan(r) {
				continue
			}
			if r.Main == last {
				// replace the revision with a new one that has higher Sub value,
				// because the original one should not be seen by external
				revs[len(revs)-1] = r
				continue
			}
			revs = append(revs, r)
			last = r.Main
		}
	}
	return revs
}

// compact compacts a keyIndex by removing the versions with smaller or equal
// revision than the given atRev except the largest one (If the largest one is
// a tombstone, it will not be kept).
// If a generation becomes empty during compaction, it will be removed.
func (ki *keyIndex) compact(lg *zap.Logger, atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		lg.Panic(
			"'compact' got an unexpected empty keyIndex",
			zap.String("Key", string(ki.Key)),
		)
	}

	genIdx, revIndex := ki.doCompact(atRev, available)

	g := &ki.Generations[genIdx]
	if !g.isEmpty() {
		// remove the previous contents.
		if revIndex != -1 {
			g.Revs = g.Revs[revIndex:]
		}
		// remove any tombstone
		if len(g.Revs) == 1 && genIdx != len(ki.Generations)-1 {
			delete(available, g.Revs[0])
			genIdx++
		}
	}

	// remove the previous Generations.
	ki.Generations = ki.Generations[genIdx:]
}

// keep finds the revision to be kept if compact is called at given atRev.
func (ki *keyIndex) keep(atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		return
	}

	genIdx, revIndex := ki.doCompact(atRev, available)
	g := &ki.Generations[genIdx]
	if !g.isEmpty() {
		// remove any tombstone
		if revIndex == len(g.Revs)-1 && genIdx != len(ki.Generations)-1 {
			delete(available, g.Revs[revIndex])
		}
	}
}

func (ki *keyIndex) doCompact(atRev int64, available map[revision]struct{}) (genIdx int, revIndex int) {
	// walk until reaching the first revision smaller or equal to "atRev",
	// and add the revision to the available map
	f := func(rev revision) bool {
		if rev.Main <= atRev {
			available[rev] = struct{}{}
			return false
		}
		return true
	}

	genIdx, g := 0, &ki.Generations[0]
	// find first generation includes atRev or Created after atRev
	for genIdx < len(ki.Generations)-1 {
		if tomb := g.Revs[len(g.Revs)-1].Main; tomb > atRev {
			break
		}
		genIdx++
		g = &ki.Generations[genIdx]
	}

	revIndex = g.walk(f)

	return genIdx, revIndex
}

// --------------------------------------------- OVER ---------------------------------------------------------------

// get 获取满足给定atRev的键的修改、创建的revision和版本.Rev必须大于或等于给定的atRev.
func (ki *keyIndex) get(lg *zap.Logger, atRev int64) (modified, created revision, ver int64, err error) {
	if ki.isEmpty() { // 判断有没有修订版本
		lg.Panic("'get'得到一个意外的空keyIndex", zap.String("Key", ki.Key))
	}
	g := ki.findGeneration(atRev)
	if g.isEmpty() {
		return revision{}, revision{}, 0, ErrRevisionNotFound
	}

	n := g.walk(func(rev revision) bool { return rev.Main > atRev }) // 返回第一个小于等于 该修订版本的最新的索引
	if n != -1 {
		return g.Revs[n], g.Created, g.VersionCount - int64(len(g.Revs)-n-1), nil
	}

	return revision{}, revision{}, 0, ErrRevisionNotFound
}

func (ki *keyIndex) Less(b btree.Item) bool {
	return strings.Compare(ki.Key, b.(*keyIndex).Key) == -1
}

func (ki *keyIndex) equal(b *keyIndex) bool {
	if !strings.EqualFold(ki.Key, b.Key) {
		return false
	}
	if ki.Modified != b.Modified {
		return false
	}
	if len(ki.Generations) != len(b.Generations) {
		return false
	}
	for i := range ki.Generations {
		ag, bg := ki.Generations[i], b.Generations[i]
		if !ag.equal(bg) {
			return false
		}
	}
	return true
}

func (ki *keyIndex) String() string {
	var s string
	for _, g := range ki.Generations {
		s += g.String()
	}
	return s
}

func (g *generation) isEmpty() bool { return g == nil || len(g.Revs) == 0 }

// 遍历返回符合条件的索引,倒序遍历
func (g *generation) walk(f func(rev revision) bool) int {
	l := len(g.Revs)
	for i := range g.Revs {
		ok := f(g.Revs[l-i-1])
		if !ok {
			return l - i - 1
		}
	}
	return -1
}

func (g generation) equal(b generation) bool {
	if g.VersionCount != b.VersionCount {
		return false
	}
	if len(g.Revs) != len(b.Revs) {
		return false
	}

	for i := range g.Revs {
		ar, br := g.Revs[i], b.Revs[i]
		if ar != br {
			return false
		}
	}
	return true
}

func (g *generation) String() string {
	return fmt.Sprintf("g: 创建[%d] 版本数[%d], 修订记录 %#v\n", g.Created, g.VersionCount, g.Revs)
}

// OK
func (ki *keyIndex) isEmpty() bool {
	// 只有一个历史版本,且
	return len(ki.Generations) == 1 && ki.Generations[0].isEmpty()
}

// findGeneration 找到给定rev所属的keyIndex的生成.如果给定的rev在两代之间,这意味着在给定的rev上键不存在,它将返回nil.
// 如果修订版本是接下来要写的,就返回当前代
func (ki *keyIndex) findGeneration(rev int64) *generation {
	lastg := len(ki.Generations) - 1
	cg := lastg
	// 倒着查找
	for cg >= 0 {
		if len(ki.Generations[cg].Revs) == 0 {
			cg--
			continue
		}
		g := ki.Generations[cg]
		if cg != lastg {
			// 每次生成的key的最新修订版本
			// 不是最新的一组,但最大的都比 rev
			if tomb := g.Revs[len(g.Revs)-1].Main; tomb <= rev {
				// tomb应该是删除的代数
				return nil
			}
		}
		// 0    rev     last
		if g.Revs[0].Main <= rev {
			// 找到对应修订版本 所属的gen版本
			return &ki.Generations[cg]
		}
		cg--
	}
	return nil
}

// put 将一个修订放到keyIndex中.
func (ki *keyIndex) put(lg *zap.Logger, main int64, sub int64) {
	rev := revision{Main: main, Sub: sub}
	if !rev.GreaterThan(ki.Modified) {
		lg.Panic(
			"'put'有一个意想不到的小修改",
			zap.Int64("given-revision-Main", rev.Main),
			zap.Int64("given-revision-Sub", rev.Sub),
			zap.Int64("Modified-revision-Main", ki.Modified.Main),
			zap.Int64("Modified-revision-Sub", ki.Modified.Sub),
		)
	}
	if len(ki.Generations) == 0 {
		ki.Generations = append(ki.Generations, generation{})
	}
	g := &ki.Generations[len(ki.Generations)-1]
	if len(g.Revs) == 0 { // create a new Key
		g.Created = rev
	}
	g.Revs = append(g.Revs, rev)
	g.VersionCount++
	ki.Modified = rev
}
