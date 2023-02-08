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

// Package traceutil implements tracing utilities using "context".
package traceutil

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

const (
	TraceKey     = "trace"
	StartTimeKey = "StartTime"
)

// Field is a kv pair to record additional details of the trace.
type Field struct {
	Key   string      `json:"key,omitempty"`
	Value interface{} `json:"value,omitempty"`
}

func (f *Field) format() string {
	return fmt.Sprintf("%s:%v; ", f.Key, f.Value)
}

type Trace struct {
	Operation    string      `json:"operation,omitempty"`
	lg           *zap.Logger `json:"lg,omitempty"`
	Fields       []Field     `json:"fields,omitempty"`
	StartTime    time.Time   `json:"start_time"`
	Steps        []Step      `json:"steps,omitempty"`
	StepDisabled bool        `json:"step_disabled,omitempty"`
	IsEmpty      bool        `json:"is_empty,omitempty"`
}

type Step struct {
	Time            time.Time `json:"time"`
	Msg             string    `json:"msg,omitempty"`
	Fields          []Field   `json:"fields,omitempty"`
	IsSubTraceStart bool      `json:"is_sub_trace_start,omitempty"`
	IsSubTraceEnd   bool      `json:"is_sub_trace_end,omitempty"`
}

func New(op string, lg *zap.Logger, fields ...Field) *Trace {
	return &Trace{Operation: op, lg: lg, StartTime: time.Now(), Fields: fields}
}

// TODO returns a non-nil, empty Trace
func TODO() *Trace {
	return &Trace{IsEmpty: true}
}

func Get(ctx context.Context) *Trace {
	if trace, ok := ctx.Value(TraceKey).(*Trace); ok && trace != nil {
		return trace
	}
	return TODO()
}

func (t *Trace) GetStartTime() time.Time {
	return t.StartTime
}

func (t *Trace) SetStartTime(time time.Time) {
	t.StartTime = time
}

func (t *Trace) InsertStep(at int, time time.Time, msg string, fields ...Field) {
	newStep := Step{Time: time, Msg: msg, Fields: fields}
	if at < len(t.Steps) {
		t.Steps = append(t.Steps[:at+1], t.Steps[at:]...)
		t.Steps[at] = newStep
	} else {
		t.Steps = append(t.Steps, newStep)
	}
}

// StartSubTrace adds Step to trace as a start sign of sublevel trace
// All Steps in the subtrace will log out the input Fields of this function
func (t *Trace) StartSubTrace(fields ...Field) {
	t.Steps = append(t.Steps, Step{Fields: fields, IsSubTraceStart: true})
}

// StopSubTrace adds Step to trace as a end sign of sublevel trace
// All Steps in the subtrace will log out the input Fields of this function
func (t *Trace) StopSubTrace(fields ...Field) {
	t.Steps = append(t.Steps, Step{Fields: fields, IsSubTraceEnd: true})
}

// Step adds Step to trace
func (t *Trace) Step(msg string, fields ...Field) {
	if !t.StepDisabled {
		t.Steps = append(t.Steps, Step{Time: time.Now(), Msg: msg, Fields: fields})
	}
}

// StepWithFunction 将测量输入函数作为一个单一的步骤
func (t *Trace) StepWithFunction(f func(), msg string, fields ...Field) {
	t.disableStep()
	f()
	t.enableStep()
	t.Step(msg, fields...)
}

func (t *Trace) AddField(fields ...Field) {
	for _, f := range fields {
		if !t.updateFieldIfExist(f) {
			t.Fields = append(t.Fields, f)
		}
	}
}

func (t *Trace) updateFieldIfExist(f Field) bool {
	for i, v := range t.Fields {
		if v.Key == f.Key {
			t.Fields[i].Value = f.Value
			return true
		}
	}
	return false
}

// disableStep sets the flag to prevent the trace from adding Steps
func (t *Trace) disableStep() {
	t.StepDisabled = true
}

// enableStep re-enable the trace to add Steps
func (t *Trace) enableStep() {
	t.StepDisabled = false
}
