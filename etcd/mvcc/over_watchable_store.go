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
	"sync"
	"time"

	"github.com/ls-2018/etcd_cn/etcd/lease"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/buckets"
	"github.com/ls-2018/etcd_cn/offical/api/v3/mvccpb"
	"github.com/ls-2018/etcd_cn/pkg/traceutil"

	"go.uber.org/zap"
)

var (
	// chanBufLen is the length of the buffered chan
	// for sending out watched events.
	// See https://github.com/etcd-io/etcd/issues/11906 for more detail.
	chanBufLen = 128

	// maxWatchersPerSync is the number of watchers to sync in a single batch
	maxWatchersPerSync = 512
)

type watchable interface {
	watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, cancelFunc)
	progress(w *watcher)
	rev() int64
}

type watchableStore struct {
	*store
	mu       sync.RWMutex
	victims  []watcherBatch // 因为channel阻塞而暂存的
	victimc  chan struct{}  // 如果watcher实例关联的ch通道被阻塞了,则对应的watcherBatch实例会暂时记录到该字段中
	unsynced watcherGroup   // 用于存储未同步完成的实例
	synced   watcherGroup   // 用于存储同步完成的实例
	stopc    chan struct{}
	wg       sync.WaitGroup
}

// cancelFunc updates unsynced and synced maps when running
// cancel operations.
type cancelFunc func()

func New(lg *zap.Logger, b backend.Backend, le lease.Lessor, cfg StoreConfig) WatchableKV {
	return newWatchableStore(lg, b, le, cfg)
}

func newWatchableStore(lg *zap.Logger, b backend.Backend, le lease.Lessor, cfg StoreConfig) *watchableStore {
	if lg == nil {
		lg = zap.NewNop()
	}
	s := &watchableStore{
		store:    NewStore(lg, b, le, cfg),
		victimc:  make(chan struct{}, 1), // 如果watcher实例关联的ch通道被阻塞了,则对应的watcherBatch实例会暂时记录到该字段中
		unsynced: newWatcherGroup(),      // 用于存储未同步完成的实例
		synced:   newWatcherGroup(),      // 用于存储已经同步完成的实例
		stopc:    make(chan struct{}),
	}
	s.store.ReadView = &readView{s}   // 调用storage中全局view查询
	s.store.WriteView = &writeView{s} // 调用storage中全局view查询
	if s.le != nil {
		// 使用此存储作为删除器,因此撤销触发器监听事件
		s.le.SetRangeDeleter(func() lease.TxnDelete {
			return s.Write(traceutil.TODO())
		})
	}
	s.wg.Add(2)
	go s.syncWatchersLoop()
	go s.syncVictimsLoop() // 用于循环清除watchableStore中的victims
	return s
}

func (s *watchableStore) Close() error {
	close(s.stopc)
	s.wg.Wait()
	return s.store.Close()
}

func (s *watchableStore) NewWatchStream() WatchStream {
	return &watchStream{
		watchable: s,
		ch:        make(chan WatchResponse, chanBufLen),
		cancels:   make(map[WatchID]cancelFunc),
		watchers:  make(map[WatchID]*watcher),
	}
}

// watcher 初始化
func (s *watchableStore) watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, cancelFunc) {
	wa := &watcher{
		key:         string(key),
		end:         string(end),
		minRev:      startRev,
		id:          id,
		ch:          ch, // 将变更事件塞进去,可能会与其他watcher 共享
		filterFuncs: fcs,
	}

	s.mu.Lock()
	s.revMu.RLock()
	// 为0 或者是超过最新的修订版本 , 都设置为下一个修订版本
	synced := startRev > s.store.currentRev || startRev == 0
	if synced {
		wa.minRev = s.store.currentRev + 1
		if startRev > wa.minRev {
			wa.minRev = startRev
		}
		s.synced.add(wa) // 把当前watcher 定义为已经同步完的
	} else {
		s.unsynced.add(wa) // 把当前watcher 定义为 没有同步完的
	}
	s.revMu.RUnlock()
	s.mu.Unlock()

	return wa, func() { s.cancelWatcher(wa) }
}

// 移除watcher
func (s *watchableStore) cancelWatcher(wa *watcher) {
	for {
		s.mu.Lock()
		if s.unsynced.delete(wa) {
			break
		} else if s.synced.delete(wa) {
			break
		} else if wa.compacted { // 因日志压缩,该watcher已被移除
			break
		} else if wa.ch == nil { // 判断是否还能发送数据
			// already canceled (e.g., cancel/close race)
			break
		}

		if !wa.victim {
			s.mu.Unlock()
			panic("观察者不是受害者,但不在观察组中")
		}

		var victimBatch watcherBatch
		for _, wb := range s.victims {
			if wb[wa] != nil {
				victimBatch = wb
				break
			}
		}
		if victimBatch != nil {
			delete(victimBatch, wa)
			break
		}

		// victim being processed so not accessible; retry
		s.mu.Unlock()
		time.Sleep(time.Millisecond)
	}

	wa.ch = nil
	s.mu.Unlock()
}

func (s *watchableStore) Restore(b backend.Backend) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.store.Restore(b)
	if err != nil {
		return err
	}

	for wa := range s.synced.watchers {
		wa.restore = true
		s.unsynced.add(wa)
	}
	s.synced = newWatcherGroup()
	return nil
}

// syncWatchersLoop 每隔100ms ,将所有未通知的事件通知给所有的监听者.
func (s *watchableStore) syncWatchersLoop() {
	defer s.wg.Done()

	for {
		s.mu.RLock()
		st := time.Now()
		lastUnsyncedWatchers := s.unsynced.size() // 获取当前的unsynced watcherGroup的大小
		s.mu.RUnlock()

		unsyncedWatchers := 0
		if lastUnsyncedWatchers > 0 {
			// 存在需要进行同步的watcher实例,调用syncWatchers()方法对unsynced watcherGroup中的watcher进行批量同步
			// 会尝试发送
			unsyncedWatchers = s.syncWatchers()
		}
		syncDuration := time.Since(st)

		waitDuration := 100 * time.Millisecond
		// 阻塞中的worker
		if unsyncedWatchers != 0 && lastUnsyncedWatchers > unsyncedWatchers {
			waitDuration = syncDuration
		}

		select {
		case <-time.After(waitDuration):
		case <-s.stopc:
			return
		}
	}
}

// syncVictimsLoop 尝试将因通道阻塞的数据 重新发送,如失败,可将watcher重新放入unsyncedGroup,
// 如果发送成功,则根据相应的watcher的同步情况,将watcher实例迁移到(un)synced watcherGroup中.
func (s *watchableStore) syncVictimsLoop() {
	defer s.wg.Done()

	for {
		for s.moveVictims() != 0 {
			// 尝试更新有问题的watcher  受损的
		}
		s.mu.RLock()
		isEmpty := len(s.victims) == 0
		s.mu.RUnlock()

		var tickc <-chan time.Time
		if !isEmpty {
			tickc = time.After(10 * time.Millisecond)
		}

		select {
		case <-tickc:
		case <-s.victimc: // 读数据,表示不再受损
		case <-s.stopc:
			return
		}
	}
}

// 尝试更新阻塞中的事件数据
func (s *watchableStore) moveVictims() (moved int) {
	s.mu.Lock()
	victims := s.victims
	s.victims = nil
	s.mu.Unlock()

	var newVictim watcherBatch
	for _, wb := range victims {
		// try to send responses again
		// 尝试发送受损的响应【因通道阻塞导致的】
		for w, eb := range wb {
			rev := w.minRev - 1
			if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: rev}) {
			} else {
				if newVictim == nil {
					newVictim = make(watcherBatch)
				}
				newVictim[w] = eb
				continue
			}
			moved++
		}

		s.mu.Lock()
		s.store.revMu.RLock()
		curRev := s.store.currentRev
		for w, eb := range wb {
			if newVictim != nil && newVictim[w] != nil {
				// 不能发送响应数据的,继续保持受损状态
				continue
			}
			w.victim = false
			if eb.moreRev != 0 {
				w.minRev = eb.moreRev
			}
			// 当该watcher不再受损 ,通过watcher的修订版本与全局的修订版本,判断该watcher是存入synced还是unsynced
			if w.minRev <= curRev {
				s.unsynced.add(w)
			} else {
				s.synced.add(w)
			}
		}
		s.store.revMu.RUnlock()
		s.mu.Unlock()
	}

	if len(newVictim) > 0 {
		s.mu.Lock()
		s.victims = append(s.victims, newVictim)
		s.mu.Unlock()
	}

	return moved
}

// syncWatchers 向所有未同步完成的watcher 发消息
//	1. choose a set of watchers from the unsynced watcher group
//	2. iterate over the set to get the minimum revision and remove compacted watchers
//	3. use minimum revision to get all Key-value pairs and send those events to watchers
//	4. remove synced watchers in set from unsynced group and move to synced group
func (s *watchableStore) syncWatchers() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unsynced.size() == 0 {
		return 0
	}

	s.store.revMu.RLock()
	defer s.store.revMu.RUnlock()

	curRev := s.store.currentRev
	compactionRev := s.store.compactMainRev
	// 根据unsynced watcherGroup中记录的watcher个数对其进行分批返回,同时获取该批watcher实例中查找最小的minRev字段,maxWatchersPerSync默认为512
	wg, minRev := s.unsynced.choose(maxWatchersPerSync, curRev, compactionRev)
	minBytes, maxBytes := newRevBytes(), newRevBytes()
	revToBytes(revision{Main: minRev}, minBytes)
	revToBytes(revision{Main: curRev + 1}, maxBytes)

	tx := s.store.b.ReadTx()
	tx.RLock()
	revs, vs := tx.UnsafeRange(buckets.Key, minBytes, maxBytes, 0) // 对key Bucket进行范围查找
	evs := kvsToEvents(s.store.lg, wg, revs, vs)                   // 负责将BoltDB中查询的键值对信息转换成相应的event实例
	tx.RUnlock()

	var victims watcherBatch
	wb := newWatcherBatch(wg, evs)
	for w := range wg.watchers { // 事件发送值每一个watcher对应的Channel中
		w.minRev = curRev + 1

		eb, ok := wb[w]
		if !ok {
			// bring un-notified watcher to synced
			s.synced.add(w)
			s.unsynced.delete(w)
			continue
		}

		if eb.moreRev != 0 {
			w.minRev = eb.moreRev
		}

		if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: curRev}) {
			// 往通道里发送的消息
		} else {
			// case 1 确实是发送失败
			// case 2 通道阻塞了,暂时标记位victim
			if victims == nil {
				victims = make(watcherBatch)
			}
			w.victim = true
		}

		if w.victim {
			victims[w] = eb
		} else {
			if eb.moreRev != 0 {
				continue
			}
			s.synced.add(w)
		}
		s.unsynced.delete(w)
	}
	s.addVictim(victims)

	return s.unsynced.size()
}

// 将BoltDB中查询的键值对信息转换成相应的Event实例,通过判断BoltDB查询的键值对是否存在于watcherGroup的key中,记录mvccpb.PUT or mvccpb.DELETE
func kvsToEvents(lg *zap.Logger, wg *watcherGroup, revs, vals [][]byte) (evs []mvccpb.Event) {
	for i, v := range vals {
		var kv mvccpb.KeyValue
		if err := kv.Unmarshal(v); err != nil {
			lg.Panic("failed to unmarshal mvccpb.KeyValue", zap.Error(err))
		}

		if !wg.contains(string(kv.Key)) {
			continue
		}

		ty := mvccpb.PUT
		if isTombstone(revs[i]) {
			ty = mvccpb.DELETE
			// patch in mod revision so watchers won't skip
			kv.ModRevision = bytesToRev(revs[i]).Main
		}
		evs = append(evs, mvccpb.Event{Kv: &kv, Type: ty})
	}
	return evs
}

// notify 当前的修订版本,当前的变更事件   用于通知对应的watcher
func (s *watchableStore) notify(rev int64, evs []mvccpb.Event) {
	var victim watcherBatch
	// type watcherBatch map[*watcher]*eventBatch
	// 找到所有的watch,synced使用了map和红黑树来快速找到监听的key
	for watcher, eb := range newWatcherBatch(&s.synced, evs) {
		if eb.revs != 1 {
			s.store.lg.Panic("在watch通知中出现多次修订", zap.Int("number-of-revisions", eb.revs))
		}
		if watcher.send(WatchResponse{WatchID: watcher.id, Events: eb.evs, Revision: rev}) {
		} else {
			// 移动缓慢的观察者到victim
			watcher.minRev = rev + 1
			if victim == nil {
				victim = make(watcherBatch)
			}
			watcher.victim = true
			victim[watcher] = eb
			s.synced.delete(watcher)
		}
	}
	s.addVictim(victim) // 将因为chan满没发出的消息缓存,然后使用unsynced再将消息发送出去
}

// 添加不健康的事件          watcher:待发送的消息
func (s *watchableStore) addVictim(victim watcherBatch) {
	if victim == nil {
		return
	}
	s.victims = append(s.victims, victim)
	select {
	case s.victimc <- struct{}{}:
	default:
	}
}

func (s *watchableStore) rev() int64 {
	return s.store.Rev()
}

func (s *watchableStore) progress(w *watcher) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.synced.watchers[w]; ok {
		w.send(WatchResponse{WatchID: w.id, Revision: s.rev()})
		// If the ch is full, this watcher is receiving events.
		// We do not need to send progress at all.
	}
}

type watcher struct {
	key       string
	end       string
	victim    bool // 当ch被阻塞时,并正在进行受害者处理时被设置.
	compacted bool // 当 watcher 因为压缩而被移除时,compacted被设置
	// restore is true when the watcher is being restored from leader snapshot
	// which means that this watcher has just been moved from "synced" to "unsynced"
	// watcher group, possibly with a future revision when it was first added
	// to the synced watcher
	// "unsynced" watcher revision must always be <= current revision,
	// except when the watcher were to be moved from "synced" watcher group
	restore     bool
	minRev      int64                // 开始监听的修订版本
	id          WatchID              // watcher id
	filterFuncs []FilterFunc         // 事件过滤
	ch          chan<- WatchResponse // 将变更事件塞进去,可能会与其他watcher 共享
}

// 向客户端发送事件
func (w *watcher) send(wr WatchResponse) bool {
	progressEvent := len(wr.Events) == 0

	if len(w.filterFuncs) != 0 {
		ne := make([]mvccpb.Event, 0, len(wr.Events))
		for i := range wr.Events {
			filtered := false
			for _, filter := range w.filterFuncs {
				if filter(wr.Events[i]) {
					filtered = true
					break
				}
			}
			if !filtered {
				ne = append(ne, wr.Events[i])
			}
		}
		wr.Events = ne
	}

	// 所有的事件都被过滤掉了
	if !progressEvent && len(wr.Events) == 0 {
		return true
	}
	select {
	case w.ch <- wr: // 正常的事件
		return true
	default:
		return false
	}
}
