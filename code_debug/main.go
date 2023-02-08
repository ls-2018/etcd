package main

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	math_bits "math/bits"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ls-2018/etcd_cn/offical/etcdserverpb"
)

func mai2n() {
	fmt.Println(net.ParseIP("http://127.0.0.1:8080"))
	fmt.Println(net.ParseIP("127.0.0.1:8080"))
	fmt.Println(net.ParseIP("www.baidu.com"))
	fmt.Println(strconv.Atoi("12h"))
	fmt.Println(strconv.FormatUint(uint64(123456), 16))
	var ch chan int
	ch = nil
	select {
	case a := <-ch:
		fmt.Println("<-ch", a)
	default:

	}
	var x uint64
	x = 1
	fmt.Println((math_bits.Len64(x|1) + 6) / 7)
	fmt.Println("over")
	hash := md5.New()
	hash.Write([]byte("hello"))
	fmt.Println(fmt.Sprintf("%x", hash.Sum(nil)))

	a := `{"header":{"ID":7587861231285799685},"put":{"key":"YQ==","value":"Yg=="}}`
	b := `{"ID":7587861231285799684,"Method":"PUT","Path":"/0/version","Val":"3.5.0","Dir":false,"PrevValue":"","PrevIndex":0,"Expiration":0,"Wait":false,"Since":0,"Recursive":false,"Sorted":false,"Quorum":false,"Time":0,"Stream":false}`
	fmt.Println(json.Unmarshal([]byte(a), &etcdserverpb.InternalRaftRequest{}))
	fmt.Println(json.Unmarshal([]byte(b), &etcdserverpb.InternalRaftRequest{}))
	var c time.Time
	fmt.Println(c.IsZero())
	var d JointConfig
	fmt.Println(d[1]["a"])
}

type (
	Config      map[string]string
	JointConfig [2]Config
)

func main3() {
	fmt.Println(strings.Compare("a", "b"))
	fmt.Println(strings.Compare("a", "a"))
	fmt.Println(strings.Compare("b", "ab"))
	a := []*A{
		{Key: "a"},
		{Key: "b"},
		{Key: "c"},
		{Key: "d"},
	}
	sort.Sort(permSlice(a))
	for _, i := range a {
		fmt.Println(i.Key)
	}

	// 在已有的权限中,
	idx := sort.Search(len(a), func(i int) bool {
		// a,a 0
		// a b -1
		// b a 1
		// a,b,c,d,e
		// c
		return strings.Compare(a[i].Key, "gc") >= 0
	})
	fmt.Println(idx)
}

type A struct {
	Key string
}

type permSlice []*A

func (perms permSlice) Len() int {
	return len(perms)
}

func (perms permSlice) Less(i, j int) bool {
	// a,a 0
	// a b -1
	// b a 1

	return strings.Compare(perms[i].Key, perms[j].Key) < 0
}

func (perms permSlice) Swap(i, j int) {
	perms[i], perms[j] = perms[j], perms[i]
}

func main() {
}
