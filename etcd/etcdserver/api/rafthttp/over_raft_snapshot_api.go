package rafthttp

import (
	"context"
	"fmt"
	"net/http"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/ls-2018/etcd_cn/client_sdk/pkg/types"
	"github.com/ls-2018/etcd_cn/etcd/etcdserver/api/snap"
	"github.com/ls-2018/etcd_cn/raft/raftpb"
	"go.uber.org/zap"
)

type snapshotHandler struct {
	lg          *zap.Logger
	tr          Transporter
	r           Raft
	snapshotter *snap.Snapshotter

	localID types.ID
	cid     types.ID
}

func newSnapshotHandler(t *Transport, r Raft, snapshotter *snap.Snapshotter, cid types.ID) http.Handler {
	h := &snapshotHandler{
		lg:          t.Logger,
		tr:          t,
		r:           r,
		snapshotter: snapshotter,
		localID:     t.ID,
		cid:         cid,
	}
	if h.lg == nil {
		h.lg = zap.NewNop()
	}
	return h
}

// ServeHTTP serves HTTP request to receive and process snapshot message.
// 如果请求发送者在没有关闭基础TCP连接的情况下死亡.处理程序将继续等待请求主体,直到TCP keepalive发现连接在几分钟后被破坏.
// 这是可接受的,因为通过其他 TCP 连接发送的快照信息仍然可以被接收和处理.接收和处理.
// 2. 这种情况应该很少发生,所以不做进一步优化.
func (h *snapshotHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.cid.String())

	if err := checkClusterCompatibilityFromHeader(h.lg, h.localID, r.Header, h.cid); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	addRemoteFromRequest(h.tr, r)

	dec := &messageDecoder{r: r.Body}
	// 快照可能超过512MB.
	m, err := dec.decodeLimit(snapshotLimitByte) // 8字节[消息长度]+消息+snap
	from := types.ID(m.From).String()
	if err != nil {
		msg := fmt.Sprintf("解码raft消息失败 (%v)", err)
		h.lg.Warn("解码raft消息失败", zap.String("local-member-id", h.localID.String()), zap.String("remote-snapshot-sender-id", from), zap.Error(err))
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	msgSize := m.Size()

	if m.Type != raftpb.MsgSnap {
		h.lg.Warn(
			"不期待的消息类型",
			zap.String("local-member-id", h.localID.String()),
			zap.String("remote-snapshot-sender-id", from),
			zap.String("message-type", m.Type.String()),
		)
		http.Error(w, "不期待的消息类型", http.StatusBadRequest)
		return
	}

	h.lg.Info(
		"开始接受快照",
		zap.String("local-member-id", h.localID.String()),
		zap.String("remote-snapshot-sender-id", from),
		zap.Uint64("incoming-snapshot-index", m.Snapshot.Metadata.Index),
		zap.Int("incoming-snapshot-message-size-bytes", msgSize),
		zap.String("incoming-snapshot-message-size", humanize.Bytes(uint64(msgSize))),
	)

	n, err := h.snapshotter.SaveDBFrom(r.Body, m.Snapshot.Metadata.Index)
	if err != nil {
		msg := fmt.Sprintf("保存快照失败 (%v)", err)
		h.lg.Warn(
			"保存快照失败",
			zap.String("local-member-id", h.localID.String()),
			zap.String("remote-snapshot-sender-id", from),
			zap.Uint64("incoming-snapshot-index", m.Snapshot.Metadata.Index),
			zap.Error(err),
		)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	downloadTook := time.Since(start)
	h.lg.Info(
		"接受并保存数据库快照",
		zap.String("local-member-id", h.localID.String()),
		zap.String("remote-snapshot-sender-id", from),
		zap.Uint64("incoming-snapshot-index", m.Snapshot.Metadata.Index),
		zap.Int64("incoming-snapshot-size-bytes", n),
		zap.String("incoming-snapshot-size", humanize.Bytes(uint64(n))),
		zap.String("download-took", downloadTook.String()),
	)

	if err := h.r.Process(context.TODO(), m); err != nil {
		switch v := err.(type) {
		case writerToResponse:
			v.WriteTo(w)
		default:
			msg := fmt.Sprintf("处理消息失败 (%v)", err)
			h.lg.Warn("处理消息失败", zap.String("local-member-id", h.localID.String()),
				zap.String("remote-snapshot-sender-id", from),
				zap.Error(err),
			)
			http.Error(w, msg, http.StatusInternalServerError)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
