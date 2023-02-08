package main

import (
	"log"

	bolt "go.etcd.io/bbolt"
)

func main() {
	f()
}

func f() error {
	// 打开boltdb文件,获取db对象
	db, err := bolt.Open("db", 0o600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	// 参数true表示创建一个写事务,false读事务
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// 使用事务对象创建key bucket
	b, err := tx.CreateBucketIfNotExists([]byte("key"))
	if err != nil {
		return err
	}
	// 使用bucket对象更新一个key
	if err := b.Put([]byte("r94"), []byte("world")); err != nil {
		return err
	}
	// 提交事务
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}
