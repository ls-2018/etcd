package conn

import (
	"fmt"
	"net"
	"os/exec"
	"strings"

	"github.com/ls-2018/etcd_cn/code_debug/conf"
)

func PrintConn(line string, c net.Conn) {
	if conf.Perf {
		return
	}

	_, port, _ := net.SplitHostPort(c.RemoteAddr().String())
	res := SubCommand([]string{"zsh", "-c", fmt.Sprintf("lsof -itcp -n|grep '%s->'", port)})
	if res == "" {
		return
	}
	pid := SubCommand([]string{"zsh", "-c", fmt.Sprintf("echo '%s'| awk '{print $2}'", res)})
	if pid == "" {
		return
	}
	commandRes := SubCommand([]string{"zsh", "-c", fmt.Sprintf("ps -ef|grep -v grep|grep -v zsh |grep '%s'", pid)})
	if commandRes == "" {
		return
	}
	command := SubCommand([]string{"zsh", "-c", fmt.Sprintf("echo '%s'| awk '{$1=$2=$3=$4=$5=$6=$7=\"\"; print $0}'", commandRes)})
	if command == "" {
		return
	}
	pr := fmt.Sprintf("%s RemoteAddr:%s-->localAddr:%s [%s]", line, c.RemoteAddr().String(), c.LocalAddr().String(), strings.Trim(command, " \n"))
	Green(pr)
}

func SubCommand(opt []string) (result string) {
	cmd := exec.Command(opt[0], opt[1:]...)
	// 命令的错误输出和标准输出都连接到同一个管道
	stdout, err := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout
	if err != nil {
		panic(err)
	}
	if err = cmd.Start(); err != nil {
		panic(err)
	}
	for {
		tmp := make([]byte, 1024)
		_, err := stdout.Read(tmp)
		res := strings.Split(string(tmp), "\n")
		for _, v := range res {
			if len(v) > 0 && v[0] != '\u0000' {
				// fmt.Println(v)
				result += v
				// log.Debug(v)
			}
		}
		if err != nil {
			break
		}
	}
	return result
	//if err = cmd.Wait(); err != nil {
	//	log.Fatal(err)
	//}
}

func Green(pr string) {
	fmt.Println(fmt.Sprintf("\033[1;32;42m %s \033[0m", pr))
}
