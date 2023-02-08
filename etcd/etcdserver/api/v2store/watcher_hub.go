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

package v2store

import (
	"container/list"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2error"
)

// A watcherHub  一个watcherHub包含所有订阅的watcher,watcher是一个以watched路径为key,以watcher为值的map,
// EventHistory为watcherHub保存旧的事件.
// 它被用来帮助watcher获得一个连续的事件历史.观察者可能会错过在第一个观察命令结束和第二个命令开始之间发生的事件.
type watcherHub struct {
	count        int64                 // 当前的监听者数量
	mutex        sync.Mutex            //
	watchers     map[string]*list.List // 所有在xxx目录下的监听者
	EventHistory *EventHistory         // 历史事件
}

// newWatchHub 创建一个watcherHub.容量决定了我们将在eventHistory中保留多少个事件.
func newWatchHub(capacity int) *watcherHub {
	return &watcherHub{
		watchers:     make(map[string]*list.List),
		EventHistory: newEventHistory(capacity),
	}
}

// Watch 返回一个Watcher.
func (wh *watcherHub) watch(key string, recursive, stream bool, index, storeIndex uint64) (Watcher, *v2error.Error) {
	event, err := wh.EventHistory.scan(key, recursive, index)
	if err != nil {
		err.Index = storeIndex
		return nil, err
	}

	w := &watcher{
		eventChan:  make(chan *Event, 100), // use a buffered channel
		recursive:  recursive,
		stream:     stream,
		sinceIndex: index,
		startIndex: storeIndex,
		hub:        wh,
	}

	wh.mutex.Lock()
	defer wh.mutex.Unlock()
	// If the event exists in the known history, append the EtcdIndex and return immediately
	if event != nil {
		ne := event.Clone()
		ne.EtcdIndex = storeIndex
		w.eventChan <- ne
		return w, nil
	}

	l, ok := wh.watchers[key]

	var elem *list.Element

	if ok { // add the new watcher to the back of the list
		elem = l.PushBack(w)
	} else { // create a new list and add the new watcher
		l = list.New()
		elem = l.PushBack(w)
		wh.watchers[key] = l
	}

	w.remove = func() {
		if w.removed { // avoid removing it twice
			return
		}
		w.removed = true
		l.Remove(elem)
		atomic.AddInt64(&wh.count, -1)
		if l.Len() == 0 {
			delete(wh.watchers, key)
		}
	}

	atomic.AddInt64(&wh.count, 1)
	return w, nil
}

func (wh *watcherHub) add(e *Event) {
	wh.EventHistory.addEvent(e)
}

// notify 接收一个事件,通知watcher
func (wh *watcherHub) notify(e *Event) {
	e = wh.EventHistory.addEvent(e)
	segments := strings.Split(e.NodeExtern.Key, "/") //  /0/members/8e9e05c52164694d/raftAttributes
	currPath := "/"
	// if the path is "/foo/bar", --> "/","/foo", "/foo/bar"
	for _, segment := range segments {
		currPath = path.Join(currPath, segment)
		// 通知对当前路径变化 感兴趣的观察者
		// e.NodeExtern.Key    /0/members/8e9e05c52164694d/raftAttributes
		// nodePath :		/
		// nodePath :		/0
		// nodePath :		/0/members
		// nodePath :		/0/members/8e9e05c52164694d
		// nodePath :		/0/members/8e9e05c52164694d/raftAttributes
		wh.notifyWatchers(e, currPath, false)
	}
}

// ok
func (wh *watcherHub) notifyWatchers(e *Event, nodePath string, deleted bool) {
	wh.mutex.Lock()
	defer wh.mutex.Unlock()

	l, ok := wh.watchers[nodePath]
	if ok {
		curr := l.Front()
		// e.NodeExtern.Key    /0/members/8e9e05c52164694d/raftAttributes
		// nodePath :		/0/members/8e9e05c52164694d/raftAttributes
		for curr != nil {
			next := curr.Next()
			w, _ := curr.Value.(*watcher)
			originalPath := e.NodeExtern.Key == nodePath
			// 是不是起源,或者该目录不是隐藏节点
			if (originalPath || !isHidden(nodePath, e.NodeExtern.Key)) && w.notify(e, originalPath, deleted) {
				if !w.stream {
					// 如果不是流观察者---> 删除,-1
					w.removed = true
					l.Remove(curr)
					atomic.AddInt64(&wh.count, -1)
				}
			}
			curr = next
		}

		if l.Len() == 0 {
			// 通知之后,就删除
			delete(wh.watchers, nodePath)
		}
	}
}

func (wh *watcherHub) clone() *watcherHub {
	clonedHistory := wh.EventHistory.clone()

	return &watcherHub{
		EventHistory: clonedHistory,
	}
}

// isHidden 检查关键路径是否被认为是隐藏的观察路径,即最后一个元素是隐藏的,或者它在一个隐藏的目录中.
func isHidden(watchPath, keyPath string) bool {
	if len(watchPath) > len(keyPath) {
		return false
	}
	afterPath := path.Clean("/" + keyPath[len(watchPath):]) // 去后边的路径
	return strings.Contains(afterPath, "/_")
}
