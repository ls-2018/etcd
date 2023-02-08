package transport

import (
	"syscall"
)

type Controls []func(network, addr string, conn syscall.RawConn) error

func (ctls Controls) Control(network, addr string, conn syscall.RawConn) error {
	for _, s := range ctls {
		if err := s(network, addr, conn); err != nil {
			return err
		}
	}
	return nil
}

type SocketOpts struct {
	// [1] https://man7.org/linux/man-pages/man7/socket.7.html
	// 启用在listener上设置套接字选项SO_REUSEPORT.允许重新绑定一个已经在使用的端口.
	// 用户应该记住.在这种情况下.数据文件上的锁可能会导致意外情况的发生.用户应该注意防止锁竞争.
	ReusePort bool
	// ReuseAddress启用了一个套接字选项SO_REUSEADDR.允许绑定到`TIME_WAIT`状态下的地址.在etcd因过多的`TIME_WAIT'而缓慢重启的情况下.这对提高MTTR很有用.
	// [1] https://man7.org/linux/man-pages/man7/socket.7.html
	ReuseAddress bool
}

func getControls(sopts *SocketOpts) Controls {
	ctls := Controls{}
	if sopts.ReuseAddress {
		ctls = append(ctls, setReuseAddress)
	}
	if sopts.ReusePort {
		ctls = append(ctls, setReusePort)
	}
	return ctls
}

func (sopts *SocketOpts) Empty() bool {
	return !sopts.ReuseAddress && !sopts.ReusePort
}
