package transport

import (
	"net"
	"time"
)

type ListenerOptions struct {
	Listener     net.Listener
	ListenConfig net.ListenConfig

	socketOpts       *SocketOpts // 套接字选项
	tlsInfo          *TLSInfo    // 证书信息
	skipTLSInfoCheck bool
	writeTimeout     time.Duration // 设置读写超时
	readTimeout      time.Duration
}

func newListenOpts(opts ...ListenerOption) *ListenerOptions {
	lnOpts := &ListenerOptions{}
	lnOpts.applyOpts(opts)
	return lnOpts
}

func (lo *ListenerOptions) applyOpts(opts []ListenerOption) {
	for _, opt := range opts {
		opt(lo)
	}
}

// IsTimeout returns true if the listener has a read/write timeout defined.
func (lo *ListenerOptions) IsTimeout() bool { return lo.readTimeout != 0 || lo.writeTimeout != 0 }

// IsSocketOpts returns true if the listener options includes socket options.
func (lo *ListenerOptions) IsSocketOpts() bool {
	if lo.socketOpts == nil {
		return false
	}
	return lo.socketOpts.ReusePort || lo.socketOpts.ReuseAddress
}

// IsTLS returns true if listner options includes TLSInfo.
func (lo *ListenerOptions) IsTLS() bool {
	if lo.tlsInfo == nil {
		return false
	}
	return !lo.tlsInfo.Empty()
}

// ListenerOption 是可以应用于listener的选项.
type ListenerOption func(*ListenerOptions)

// WithTimeout 允许对listener应用一个读或写的超时.
func WithTimeout(read, write time.Duration) ListenerOption {
	return func(lo *ListenerOptions) {
		lo.writeTimeout = write
		lo.readTimeout = read
	}
}

// WithSocketOpts 定义了将应用于listener的套接字选项.
func WithSocketOpts(s *SocketOpts) ListenerOption {
	return func(lo *ListenerOptions) { lo.socketOpts = s }
}

// WithTLSInfo 向listener添加TLS证书.
func WithTLSInfo(t *TLSInfo) ListenerOption {
	return func(lo *ListenerOptions) { lo.tlsInfo = t }
}

// WithSkipTLSInfoCheck when true a transport can be created with an https scheme
// without passing TLSInfo, circumventing not presented error. Skipping this check
// also requires that TLSInfo is not passed.
func WithSkipTLSInfoCheck(skip bool) ListenerOption {
	return func(lo *ListenerOptions) { lo.skipTLSInfoCheck = skip }
}
