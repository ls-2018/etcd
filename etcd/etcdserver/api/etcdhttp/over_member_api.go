package etcdhttp

import (
	"encoding/json"
	"net/http"

	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api"
	"go.uber.org/zap"
)

func newPeerMembersHandler(lg *zap.Logger, cluster api.Cluster) http.Handler {
	return &peerMembersHandler{
		lg:      lg,
		cluster: cluster,
	}
}

func (h *peerMembersHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, "GET") {
		return
	}
	w.Header().Set("X-Etcd-Cluster-ID", h.cluster.ID().String())

	if r.URL.Path != peerMembersPath {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	ms := h.cluster.Members()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(ms); err != nil {
		h.lg.Warn("编码成员信息失败", zap.Error(err))
	}
}
