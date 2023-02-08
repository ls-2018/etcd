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

package raftpb

import (
	"fmt"
	"strings"

	"github.com/gogo/protobuf/proto"
)

// ConfChangeI 配置变更
type ConfChangeI interface {
	AsV2() ConfChangeV2
	AsV1() (ConfChangeV1, bool)
}

var (
	_ ConfChangeI = ConfChangeV1{}
	_ ConfChangeI = ConfChangeV2{}
)

// EnterJoint 返回两个布尔.当且仅当此配置变更将使用联合共识时,第二个bool为真,
// 如果它包含一个以上的变更或明确要求使用联合共识,则属于这种情况.
// 第一个bool只有在第二个bool为真时才会出现,它表示联合状态是否会被自动留下.
func (c ConfChangeV2) EnterJoint() (autoLeave bool, ok bool) {
	// 注:理论上,更多的配置变化可以符合 "simple "协议的要求,但这取决于这些变化所基于的配置.
	// 例如,如果两个节点都是基本配置的一部分,增加两个learner是不可以的(也就是说,在应用配置变化的过程中,
	// 两个voter变成了learner).在实践中,这些区别应该是不重要的,所以我们保持简单,随意使用联合共识.
	if c.Transition != ConfChangeTransitionAuto || len(c.Changes) > 1 {
		// 使用联合共识
		var autoLeave bool
		switch c.Transition {
		case ConfChangeTransitionAuto:
			autoLeave = true
		case ConfChangeTransitionJointImplicit:
			autoLeave = true
		case ConfChangeTransitionJointExplicit:
		default:
			panic(fmt.Sprintf("未知的过渡状态: %+v", c))
		}
		return autoLeave, true
	}
	return false, false
}

// LeaveJoint is true if the configuration change leaves a joint configuration.
// This is the case if the ConfChangeV2 is zero, with the possible exception of
// the Context field.
// 是真,如果配置改变留下了一个联合配置.如果ConfChangeV2为零,就会出现这种情况,但Context字段可能例外.
func (c ConfChangeV2) LeaveJoint() bool {
	// NB: c已经是一个拷贝
	c.Context = ""
	return proto.Equal(&c, &ConfChangeV2{})
}

// ------------------------------ over ----------------------------------------

func MarshalConfChange(c ConfChangeI) (EntryType, []byte, error) {
	var typ EntryType
	var ccdata []byte
	var err error
	if ccv1, ok := c.AsV1(); ok {
		typ = EntryConfChange
		ccdata, err = ccv1.Marshal()
	} else {
		ccv2 := c.AsV2()
		typ = EntryConfChangeV2
		ccdata, err = ccv2.Marshal()
	}
	return typ, ccdata, err
}

func (c ConfChangeV1) AsV2() ConfChangeV2 {
	return ConfChangeV2{
		Changes: []ConfChangeSingle{{
			Type:   c.Type,
			NodeID: c.NodeID,
		}},
		Context: c.Context,
	}
}

func (c ConfChangeV1) AsV1() (ConfChangeV1, bool) {
	return c, true
}

func (c ConfChangeV2) AsV2() ConfChangeV2 { return c }

func (c ConfChangeV2) AsV1() (ConfChangeV1, bool) { return ConfChangeV1{}, false }

// ConfChangesToString 与ConfChangesFromString正好相反.
func ConfChangesToString(ccs []ConfChangeSingle) string {
	var buf strings.Builder
	for i, cc := range ccs {
		if i > 0 {
			buf.WriteByte(' ')
		}
		switch cc.Type {
		case ConfChangeAddNode:
			buf.WriteString("add")
		case ConfChangeAddLearnerNode:
			buf.WriteString("learner")
		case ConfChangeRemoveNode:
			buf.WriteString("remove")
		case ConfChangeUpdateNode:
			buf.WriteString("update")
		default:
			buf.WriteString("unknown")
		}
		fmt.Fprintf(&buf, "%d", cc.NodeID)
	}
	return buf.String()
}
