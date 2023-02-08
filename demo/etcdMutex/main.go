package main

import (
	"context"
	"fmt"
	"log"
	"time"

	clientv3 "github.com/ls-2018/etcd_cn/client_sdk/v3"
	"github.com/ls-2018/etcd_cn/client_sdk/v3/concurrency"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// m1来抢锁
	go func() {
		// 创建两个竞争的Session,这里Session有效期使用10s
		s1, err := concurrency.NewSession(cli, concurrency.WithTTL(10))
		if err != nil {
			log.Fatal(err)
		}
		defer s1.Close()

		// pfx 是"/my-lock"
		m1 := concurrency.NewMutex(s1, "/my-lock") // 创建结构体

		// acquire lock for s1
		if err := m1.Lock(context.TODO()); err != nil {
			log.Fatal(err)
		}

		fmt.Println("m1---获得了锁")

		time.Sleep(time.Second * 3)

		// 释放锁
		if err := m1.Unlock(context.TODO()); err != nil {
			log.Fatal(err)
		}
		fmt.Println("m1++释放了锁")
	}()

	// m2来抢锁
	go func() {
		s2, err := concurrency.NewSession(cli, concurrency.WithTTL(10))
		if err != nil {
			log.Fatal(err)
		}
		defer s2.Close()
		m2 := concurrency.NewMutex(s2, "/my-lock")
		if err := m2.Lock(context.TODO()); err != nil {
			log.Fatal(err)
		}
		fmt.Println("m2---获得了锁")

		// mock业务执行的时间
		time.Sleep(time.Second * 3)

		// 释放锁
		if err := m2.Unlock(context.TODO()); err != nil {
			log.Fatal(err)
		}

		fmt.Println("m2++释放了锁")
	}()

	time.Sleep(time.Second * 10)
}
