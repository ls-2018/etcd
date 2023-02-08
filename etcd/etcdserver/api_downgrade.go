package etcdserver

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/membership"
	"go.uber.org/zap"
)

func (s *EtcdServer) DowngradeEnabledHandler() http.Handler {
	return &downgradeEnabledHandler{
		lg:      s.Logger(),
		cluster: s.cluster,
		server:  s,
	}
}

type downgradeEnabledHandler struct {
	lg      *zap.Logger
	cluster api.Cluster
	server  *EtcdServer
}

func (h *downgradeEnabledHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.cluster.ID().String())

	if r.URL.Path != DowngradeEnabledPath {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.server.Cfg.ReqTimeout())
	defer cancel()

	// serve with linearized downgrade info
	if err := h.server.linearizeReadNotify(ctx); err != nil {
		http.Error(w, fmt.Sprintf("failed linearized read: %v", err),
			http.StatusInternalServerError)
		return
	}
	enabled := h.server.DowngradeInfo().Enabled
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(strconv.FormatBool(enabled)))
}

func (s *EtcdServer) DowngradeInfo() *membership.DowngradeInfo { return s.cluster.DowngradeInfo() }
