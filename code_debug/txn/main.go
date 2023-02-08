package main

import (
	"context"
	"fmt"
	"log"

	clientv3 "github.com/ls-2018/etcd_cn/client_sdk/v3"
)

func main() {
	endpoints := []string{"127.0.0.1:2379"}
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// 创建租约
	lease := clientv3.NewLease(cli)

	// 设置租约时间
	leaseResp, err := lease.Grant(context.TODO(), 10) // 秒
	if err != nil {
		fmt.Printf("设置租约时间失败:%s\n", err.Error())
	}
	_, err = cli.Put(context.Background(), "a", "x", clientv3.WithLease(leaseResp.ID))
	if err != nil {
		log.Fatal(err)
	}
	resp, err := cli.Txn(context.TODO()).If(
		clientv3.Compare(clientv3.LeaseValue("a"), "=", leaseResp.ID),
	).Then(
		clientv3.OpPut("b", "v30"),
	).Commit()
	if err != nil {
		log.Fatal(err)
	}

	if err != nil {
		log.Fatal(err)
	}
	for _, rp := range resp.Responses {
		res := rp.GetResponseRange()
		if res == nil {
			continue
		}
		for _, ev := range res.Kvs {
			fmt.Printf("%s -> %s, create revision = %d\n",
				ev.Key,
				ev.Value,
				ev.CreateRevision)
		}
	}
}
