```
在一个任期内,一个Raft 节点最多只能为一个候选人投票,按照先到先得的原则,投给最早来拉选票的候选人(注意:下文的"安全性"针对投票添加了一个额外的限制)
```

#### append

1) 客户端向Leader 发送写请求.
2) Leader 将写请求解析成操作指令追加到本地日志文件中.
3) Leader 为每个Follower 广播AppendEntries RPC .
4) Follower 通过一致性检查,选择从哪个位置开始追加Leader 的日志条目.
5) 一旦日志项commit成功, Leader 就将该日志条目对应的指令应用(apply) 到本地状态机,并向客户端返回操作结果.
6) Leader后续通过AppendEntries RPC 将已经成功(在大多数节点上)提交的日志项告知Follower .
7) Follower 收到提交的日志项之后,将其应用至本地状态机.

``` Goland
type Storage interface {
	InitialState() (pb.HardState, pb.ConfState, error)
	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)
	Term(i uint64) (uint64, error)
	LastIndex() (uint64, error)     // 返回最后一条数据的索引
	FirstIndex() (uint64, error)    // 返回第一条数据的索引
	Snapshot() (pb.Snapshot, error) // 反回最近的快照数据
}
var _ Storage = &MemoryStorage{}


type raftLog struct {
	storage Storage   // stable 
	unstable unstable
	committed uint64
	applied uint64
	logger Logger
	maxNextEntsSize uint64
}

```
