// Copyright 2016 The etcd Authors
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

// Package v3alarm manages health status alarms in etcd.
package v3alarm

import (
	"sync"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/backend"
	"github.com/ls-2018/etcd_cn/etcd/mvcc/buckets"
	pb "github.com/ls-2018/etcd_cn/offical/etcdserverpb"

	"go.uber.org/zap"
)

type BackendGetter interface {
	Backend() backend.Backend
}

type alarmSet map[types.ID]*pb.AlarmMember

type AlarmStore struct {
	lg    *zap.Logger
	mu    sync.Mutex
	types map[pb.AlarmType]alarmSet
	// {
	//  "AlarmType_NONE": {
	//  },
	//  "AlarmType_NOSPACE": {
	//    "1": {
	//      "MemberID": "1",
	//      "AlarmType": "AlarmType_NOSPACE"
	//    }
	//  },
	//  "AlarmType_CORRUPT": {}
	//}
	bg BackendGetter
}

func NewAlarmStore(lg *zap.Logger, bg BackendGetter) (*AlarmStore, error) {
	if lg == nil {
		lg = zap.NewNop()
	}
	ret := &AlarmStore{lg: lg, types: make(map[pb.AlarmType]alarmSet), bg: bg}
	err := ret.restore()
	return ret, err
}

// Activate 记录、入库警报
func (a *AlarmStore) Activate(id types.ID, at pb.AlarmType) *pb.AlarmMember {
	a.mu.Lock()
	defer a.mu.Unlock()

	newAlarm := &pb.AlarmMember{MemberID: uint64(id), Alarm: at}
	if m := a.addToMap(newAlarm); m != newAlarm {
		return m
	}

	v, err := newAlarm.Marshal()
	if err != nil {
		a.lg.Panic("序列化报警成员失败", zap.Error(err))
	}

	b := a.bg.Backend()
	b.BatchTx().Lock()
	b.BatchTx().UnsafePut(buckets.Alarm, v, nil)
	b.BatchTx().Unlock()

	return newAlarm
}

func (a *AlarmStore) Deactivate(id types.ID, at pb.AlarmType) *pb.AlarmMember {
	a.mu.Lock()
	defer a.mu.Unlock()

	t := a.types[at]
	if t == nil {
		t = make(alarmSet)
		a.types[at] = t
	}
	m := t[id]
	if m == nil {
		return nil
	}

	delete(t, id)

	v, err := m.Marshal()
	if err != nil {
		a.lg.Panic("反序列化报警成员失败", zap.Error(err))
	}

	b := a.bg.Backend()
	b.BatchTx().Lock()
	b.BatchTx().UnsafeDelete(buckets.Alarm, v)
	b.BatchTx().Unlock()

	return m
}

// Get 获取指定类型的警报,NONE 是所有
func (a *AlarmStore) Get(at pb.AlarmType) (ret []*pb.AlarmMember) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if at == pb.AlarmType_NONE {
		for _, t := range a.types {
			for _, m := range t {
				ret = append(ret, m)
			}
		}
		return ret
	}
	for _, m := range a.types[at] {
		ret = append(ret, m)
	}
	return ret
}

func (a *AlarmStore) restore() error {
	b := a.bg.Backend()
	tx := b.BatchTx()

	tx.Lock()
	tx.UnsafeCreateBucket(buckets.Alarm)
	err := tx.UnsafeForEach(buckets.Alarm, func(k, v []byte) error {
		var m pb.AlarmMember
		if err := m.Unmarshal(k); err != nil {
			return err
		}
		a.addToMap(&m)
		return nil
	})
	tx.Unlock()

	b.ForceCommit()
	return err
}

func (a *AlarmStore) addToMap(newAlarm *pb.AlarmMember) *pb.AlarmMember {
	t := a.types[newAlarm.Alarm]
	if t == nil {
		t = make(alarmSet)
		a.types[newAlarm.Alarm] = t
	}
	m := t[types.ID(newAlarm.MemberID)]
	if m != nil {
		return m
	}
	t[types.ID(newAlarm.MemberID)] = newAlarm
	return newAlarm
}
