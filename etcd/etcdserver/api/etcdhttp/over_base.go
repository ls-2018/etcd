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

package etcdhttp

import (
	"encoding/json"
	"expvar"
	"fmt"
	"net/http"

	"github.com/ls-2018/etcd_cn/etcd/etcdserver"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2error"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/v2http/httptypes"
	"github.com/ls-2018/etcd_cn/offical/api/v3/version"
	"go.uber.org/zap"
)

const (
	varsPath    = "/debug/vars"
	versionPath = "/version"
)

// HandleBasic 添加处理程序到一个mux服务JSON etcd客户端请求不访问v2存储.
func HandleBasic(lg *zap.Logger, mux *http.ServeMux, server etcdserver.ServerPeer) {
	mux.HandleFunc(varsPath, serveVars)
	mux.HandleFunc(versionPath, versionHandler(server.Cluster(), serveVersion)) // {"etcdserver":"3.5.2","etcdcluster":"3.5.0"}
}

// ok
func serveVersion(w http.ResponseWriter, r *http.Request, clusterV string) {
	if !allowMethod(w, r, "GET") {
		return
	}
	vs := version.Versions{
		Server:  version.Version,
		Cluster: clusterV,
	}
	// clusterV = server.Cluster().String()
	// {"etcdserver":"3.5.2","etcdcluster":"3.5.0"}

	w.Header().Set("Content-Type", "application/json")
	b, err := json.Marshal(&vs)
	if err != nil {
		panic(fmt.Sprintf("序列化失败 (%v)", err))
	}
	w.Write(b)
}

// ok
func serveVars(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, "GET") {
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) { // 同一时刻只能有一个请求执行
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}

// ok
func allowMethod(w http.ResponseWriter, r *http.Request, m string) bool {
	if m == r.Method {
		return true
	}
	w.Header().Set("Allow", m)
	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	return false
}

func WriteError(lg *zap.Logger, w http.ResponseWriter, r *http.Request, err error) {
	if err == nil {
		return
	}
	switch e := err.(type) {
	case *v2error.Error:
		e.WriteTo(w)

	case *httptypes.HTTPError:
		if et := e.WriteTo(w); et != nil {
			if lg != nil {
				lg.Debug(
					"写失败 v2 HTTP",
					zap.String("remote-addr", r.RemoteAddr),
					zap.String("internal-etcd-error", e.Error()),
					zap.Error(et),
				)
			}
		}

	default:
		switch err {
		case etcdserver.ErrTimeoutDueToLeaderFail, etcdserver.ErrTimeoutDueToConnectionLost, etcdserver.ErrNotEnoughStartedMembers,
			etcdserver.ErrUnhealthy:
			if lg != nil {
				lg.Warn(
					"v2 响应错误",
					zap.String("remote-addr", r.RemoteAddr),
					zap.String("internal-etcd-error", err.Error()),
				)
			}

		default:
			if lg != nil {
				lg.Warn(
					"未知的v2响应错误",
					zap.String("remote-addr", r.RemoteAddr),
					zap.String("internal-etcd-error", err.Error()),
				)
			}
		}

		herr := httptypes.NewHTTPError(http.StatusInternalServerError, "Internal Server Error")
		if et := herr.WriteTo(w); et != nil {
			if lg != nil {
				lg.Debug(
					"写失败 v2 HTTP",
					zap.String("remote-addr", r.RemoteAddr),
					zap.String("internal-etcd-error", err.Error()),
					zap.Error(et),
				)
			}
		}
	}
}
