package main

import (
	"fmt"
	"log"
	"time"

	clientv3 "github.com/ls-2018/etcd_cn/client_sdk/v3"
	recipe "github.com/ls-2018/etcd_cn/client_sdk/v3/experimental/recipes"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"localhost:2379"},
	})
	if err != nil {
		log.Fatalf("error New (%v)", err)
	}

	go func() {
		q := recipe.NewQueue(cli, "testq")
		for i := 0; i < 100; i++ {
			if err := q.Enqueue(fmt.Sprintf("%d", i)); err != nil {
				log.Fatalf("error enqueuing (%v)", err)
			}
		}
	}()

	go func() {
		q := recipe.NewQueue(cli, "testq")
		for i := 0; i < 100; i++ {
			if err := q.Enqueue(fmt.Sprintf("%d", i)); err != nil {
				log.Fatalf("error enqueuing (%v)", err)
			}
		}
	}()

	q := recipe.NewQueue(cli, "testq")
	for i := 0; i < 200; i++ {
		s, err := q.Dequeue()
		if err != nil {
			log.Fatalf("error dequeueing (%v)", err)
		}
		fmt.Println(s)
	}

	time.Sleep(time.Second * 3)
}
