package main

import (
	"fmt"
	"net"

	"go.uber.org/zap"
)

func main() {
	fmt.Println(net.ParseIP("www.baidu.com"))
	fmt.Println(net.ParseIP("127.168.1.2"))
	//	<nil>
	//	127.168.1.2
	zap.NewNop().Debug("asdasdasdasd")
	zap.NewNop().Fatal("asdasdasdasd")
}
