package rafthttp

import (
	"net/http"
	"path"
	"strings"

	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/offical/api/v3/version"

	"go.uber.org/zap"
)

type streamHandler struct {
	lg         *zap.Logger
	tr         *Transport
	peerGetter peerGetter
	r          Raft
	id         types.ID
	cid        types.ID
}

func newStreamHandler(t *Transport, pg peerGetter, r Raft, id, cid types.ID) http.Handler {
	h := &streamHandler{
		lg:         t.Logger,
		tr:         t,
		peerGetter: pg,
		r:          r,
		id:         id,
		cid:        cid,
	}
	if h.lg == nil {
		h.lg = zap.NewNop()
	}
	return h
}

// 添加远端节点的一个通信地址
func (h *streamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.Header().Set("Allow", "GET")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Server-Version", version.Version)
	w.Header().Set("X-Etcd-Cluster-ID", h.cid.String())

	if err := checkClusterCompatibilityFromHeader(h.lg, h.tr.ID, r.Header, h.cid); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed) // 状态 前提条件未通过
		return
	}

	var t streamType
	switch path.Dir(r.URL.Path) {
	case streamTypeMsgAppV2.endpoint(h.lg): // /raft/stream/msgappv2
		t = streamTypeMsgAppV2
	case streamTypeMessage.endpoint(h.lg): // /raft/stream/message
		t = streamTypeMessage
	default:
		h.lg.Debug("忽略意外的流请求路径",
			zap.String("local-member-id", h.tr.ID.String()),
			zap.String("remote-peer-id-stream-handler", h.id.String()),
			zap.String("path", r.URL.Path),
		)
		http.Error(w, "无效的路径", http.StatusNotFound)
		return
	}

	fromStr := path.Base(r.URL.Path)
	from, err := types.IDFromString(fromStr)
	if err != nil {
		h.lg.Warn(
			"无法将路径解析为ID",
			zap.String("local-member-id", h.tr.ID.String()),
			zap.String("remote-peer-id-stream-handler", h.id.String()),
			zap.String("path", fromStr),
			zap.Error(err),
		)
		http.Error(w, "invalid from", http.StatusNotFound)
		return
	}
	if h.r.IsIDRemoved(uint64(from)) {
		h.lg.Warn(
			"拒绝流,该节点已被移除",
			zap.String("local-member-id", h.tr.ID.String()),
			zap.String("remote-peer-id-stream-handler", h.id.String()),
			zap.String("remote-peer-id-from", from.String()),
		)
		http.Error(w, "该节点已被移除", http.StatusGone)
		return
	}
	p := h.peerGetter.Get(from)
	if p == nil {
		// 这可能发生在以下情况:
		// 1.用户启动的远端节点属于不同的集群,且集群ID相同.
		// 2. 本地etcd落后于集群,无法识别在当前进度之后加入的成员.
		if urls := r.Header.Get("X-PeerURLs"); urls != "" {
			h.tr.AddRemote(from, strings.Split(urls, ","))
		}
		h.lg.Warn(
			"在集群中没有找到远端节点",
			zap.String("local-member-id", h.tr.ID.String()),
			zap.String("remote-peer-id-stream-handler", h.id.String()),
			zap.String("remote-peer-id-from", from.String()),
			zap.String("cluster-id", h.cid.String()),
		)
		http.Error(w, "发送方没有发现该节点", http.StatusNotFound)
		return
	}

	wto := h.id.String()
	if gto := r.Header.Get("X-Raft-To"); gto != wto {
		h.lg.Warn(
			"忽略流请求; ID 不匹配",
			zap.String("local-member-id", h.tr.ID.String()),
			zap.String("remote-peer-id-stream-handler", h.id.String()),
			zap.String("remote-peer-id-header", gto),
			zap.String("remote-peer-id-from", from.String()),
			zap.String("cluster-id", h.cid.String()),
		)
		http.Error(w, "to field mismatch", http.StatusPreconditionFailed)
		return
	}
	/* 这个地方需要注意一下,此处并没有包把应答报文发出去,但是具体处理逻辑需要参考net/http中Flush */
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

	c := newCloseNotifier()
	conn := &outgoingConn{
		t:       t, // 连接类型
		Writer:  w,
		Flusher: w.(http.Flusher),
		Closer:  c,
		localID: h.tr.ID,
		peerID:  from,
	}
	p.attachOutgoingConn(conn) // 会发streamWriter run中connc操作 用于
	<-c.closeNotify()          // 等待close channel,若一直没数据可读则阻塞
}
