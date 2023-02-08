## etcd

通过etcd实现锁     


```

txn := client.Txn(ctx).If(v3.Compare(v3.CreateRevision(k), "=", 0))
txn = txn.Then(v3.OpPut(k, val, v3.WithLease(s.Lease())))
txn = txn.Else(v3.OpGet(k))
resp, err := txn.Commit()
if err != nil {
    return err
}
# 租约保活,可以拿到锁的修订版本
# --------------other---------------------

var wr v3.WatchResponse
wch := client.Watch(cctx, key, v3.WithRev(rev))
for wr = range wch {
   for _, ev := range wr.Events {
      if ev.Type == mvccpb.DELETE { // 等待其他client下线
         return nil
      }
   }
}


```

```
实现分布式锁的方案有多种,比如你可以通过 client 是否成功创建一个固定的 key,来判断此 client 是否获得锁,
你也可以通过多个 client 创建 prefix 相同,名称不一样的 key,哪个 key 的 revision 最小,最终就是它获得锁.
1. 按照文中介绍concurrency包中用的是prefix
2. 如果使用相同的key,我能够想到存在的问题,在释放锁后,会导致获取锁的事务同时发生,事务数量变得很大

嗯,最主要是惊群效应,所有client都会收到相应key被删除消息,都会尝试发起事务,写入key,性能会比较差

etcd如何避免惊群效应:mutex,通过 Watch 机制各自监听 prefix 相同,revision 比自己小的 key,
                 因为只有 revision 比自己小的 key 释放锁,才能有机会,获得锁


```