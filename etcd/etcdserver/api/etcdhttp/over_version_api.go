package etcdhttp

import (
	"net/http"

	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api"
)

// ok
func versionHandler(c api.Cluster, fn func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		v := c.Version()
		if v != nil {
			fn(w, r, v.String())
		} else {
			fn(w, r, "not_decided")
		}
	}
}
