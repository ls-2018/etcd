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

type Watcher interface {
	EventChan() chan *Event
	StartIndex() uint64 // watch创建时的EtcdIndex
	Remove()
}

type watcher struct {
	eventChan  chan *Event // 注册之后,通过这个chan返回给监听者
	stream     bool        // 是否是流观察、还是一次性观察
	recursive  bool        // 是否是递归
	sinceIndex uint64      // 从那个索引之后开始监听
	startIndex uint64
	hub        *watcherHub
	removed    bool   // 是否移除
	remove     func() // 移除时,回调函数
}

func (w *watcher) EventChan() chan *Event {
	return w.eventChan
}

func (w *watcher) StartIndex() uint64 {
	return w.startIndex
}

// notify 函数通知观察者.如果观察者在给定的路径中感兴趣,该函数将返回true.
func (w *watcher) notify(e *Event, originalPath bool, deleted bool) bool {
	// originalPath 对应 1deleted 对应 3

	// 观察者在三种情况下和一个条件下对路径感兴趣,该条件是事件发生在观察者的sinceIndex之后.
	// 1.事件发生的路径是观察者正在观察的路径.例如,如果观察者在"/foo "观察,而事件发生在"/foo",观察者必须是对该事件感兴趣.
	// 2.观察者是一个递归观察者,它对其观察路径之后发生的事件感兴趣.例如,如果观察者A在"/foo "处观察,并且它是一个递归观察者,它将对发生在"/foo/bar "的事件感兴趣.
	// 3.当我们删除一个目录时,我们需要强制通知所有在我们需要删除的文件处观察的观察者.例如,一个观察者正在观察"/foo/bar".而我们删除了"/foo".即使"/foo "不是它正在监听的路径,该监听者也应该得到通知.
	if (w.recursive || originalPath || deleted) && e.Index() >= w.sinceIndex {
		// 如果eventChan的容量已满,我们就不能在这里进行阻塞,否则etcd会挂起.当通知的速率高于我们的发送速率时,eventChan的容量就满了.如果发生这种情况,我们会关闭该通道.
		select {
		case w.eventChan <- e:
		default:
			w.remove() // 移除、关闭chan
		}
		return true
	}
	return false
}

// Remove 移除监听者
func (w *watcher) Remove() {
	w.hub.mutex.Lock()
	defer w.hub.mutex.Unlock()

	close(w.eventChan)
	if w.remove != nil {
		w.remove()
	}
}

// nopWatcher is a watcher that receives nothing, always blocking.
type nopWatcher struct{}

func NewNopWatcher() Watcher                 { return &nopWatcher{} }
func (w *nopWatcher) EventChan() chan *Event { return nil }
func (w *nopWatcher) StartIndex() uint64     { return 0 }
func (w *nopWatcher) Remove()                {}
