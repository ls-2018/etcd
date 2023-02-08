package etcdserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/ls-2018/etcd_cn/etcd/mvcc"
	"github.com/ls-2018/etcd_cn/offical/api/v3/v3rpc/rpctypes"
	pb "github.com/ls-2018/etcd_cn/offical/etcdserverpb"
	"go.uber.org/zap"
)

const PeerHashKVPath = "/members/hashkv"

type hashKVHandler struct {
	lg     *zap.Logger
	server *EtcdServer
}

func (s *EtcdServer) HashKVHandler() http.Handler {
	return &hashKVHandler{lg: s.Logger(), server: s}
}

func (h *hashKVHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.URL.Path != PeerHashKVPath {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}

	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "读取body失败", http.StatusBadRequest)
		return
	}

	req := &pb.HashKVRequest{}
	if err := json.Unmarshal(b, req); err != nil {
		h.lg.Warn("反序列化请求数据失败", zap.Error(err))
		http.Error(w, "反序列化请求数据失败", http.StatusBadRequest)
		return
	}
	hash, rev, compactRev, err := h.server.KV().HashByRev(req.Revision)
	if err != nil {
		h.lg.Warn(
			"获取hash值失败",
			zap.Int64("requested-revision", req.Revision),
			zap.Error(err),
		)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := &pb.HashKVResponse{Header: &pb.ResponseHeader{Revision: rev}, Hash: hash, CompactRevision: compactRev}
	respBytes, err := json.Marshal(resp)
	if err != nil {
		h.lg.Warn("failed to marshal hashKV response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.server.Cluster().ID().String())
	w.Header().Set("Content-Type", "application/json")
	w.Write(respBytes)
}

// getPeerHashKVHTTP 通过对给定网址的http调用在给定的rev中获取kv存储的哈希值.
func (s *EtcdServer) getPeerHashKVHTTP(ctx context.Context, url string, rev int64) (*pb.HashKVResponse, error) {
	cc := &http.Client{Transport: s.peerRt}
	hashReq := &pb.HashKVRequest{Revision: rev} // revision是哈希操作的键值存储修订版.
	hashReqBytes, err := json.Marshal(hashReq)
	if err != nil {
		return nil, err
	}
	requestUrl := url + PeerHashKVPath
	req, err := http.NewRequest(http.MethodGet, requestUrl, bytes.NewReader(hashReqBytes))
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	req.Cancel = ctx.Done()

	resp, err := cc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusBadRequest {
		if strings.Contains(string(b), mvcc.ErrCompacted.Error()) {
			return nil, rpctypes.ErrCompacted
		}
		if strings.Contains(string(b), mvcc.ErrFutureRev.Error()) {
			return nil, rpctypes.ErrFutureRev
		}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unknown error: %s", string(b))
	}

	hashResp := &pb.HashKVResponse{}
	if err := json.Unmarshal(b, hashResp); err != nil {
		return nil, err
	}
	return hashResp, nil
}
