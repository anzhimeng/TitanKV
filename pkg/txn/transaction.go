package txn

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	
	"titankv/pd/api/pdpb"
	"titankv/pkg/client" // Week 8 实现的 Client
)

// Mutation 表示一次修改
type Mutation struct {
	Key   []byte
	Value []byte // 如果是 Delete，Value 为 nil
	Op    OpType
}

type OpType int
const (
	OpPut    OpType = 0
	OpDelete OpType = 1
)

type Transaction struct {
	StartTS uint64
	client  *client.Client // 持有 KV Client 和 PD Client
	
	// 读写缓冲
	// key -> value (nil 表示 delete)
	buffer map[string][]byte
	// 记录修改顺序 (虽然 Percolator 不依赖顺序，但为了 Debug 方便)
	mutations []Mutation 
	
	// 事务状态
	committed bool
}

// 开启新事务
func NewTransaction(ctx context.Context, c *client.Client) (*Transaction, error) {
	// 1. 从 PD 获取 StartTS
	// 假设 Client 暴露了 GetTS 接口 (Week 8 Day 2)
	// 如果没有，需要去补一个
	ts, err := c.GetTS(ctx)
	if err != nil {
		return nil, err
	}

	return &Transaction{
		StartTS:   ts,
		client:    c,
		buffer:    make(map[string][]byte),
		mutations: make([]Mutation, 0),
	}, nil
}

func (txn *Transaction) Set(key []byte, value []byte) {
	k := string(key)
	txn.buffer[k] = value
	
	// 记录 Mutation (为了 Commit 阶段使用)
	// 优化：如果 key 已存在 mutations 中，更新它？
	// 简单起见，直接 append，Commit 时再 deduplicate 或者以 buffer 为准
	txn.mutations = append(txn.mutations, Mutation{
		Key:   key,
		Value: value,
		Op:    OpPut,
	})
}

func (txn *Transaction) Delete(key []byte) {
	k := string(key)
	txn.buffer[k] = nil // nil 表示删除标记
	
	txn.mutations = append(txn.mutations, Mutation{
		Key: key,
		Op:  OpDelete,
	})
}

func (txn *Transaction) Get(ctx context.Context, key []byte) ([]byte, error) {
	k := string(key)
	
	// 1. 查 Buffer (Read-Your-Writes)
	if val, ok := txn.buffer[k]; ok {
		if val == nil {
			return nil, errors.New("key not found (deleted in txn)")
		}
		return val, nil
	}

	// 2. 查 Storage (Snapshot Read)
	// 我们需要调用 Server 的 SnapshotRead 接口
	// (Server 端接口将在 Week 14 实现，这里先定义 Client 逻辑)
	
	// 假设 Client 有一个 SnapshotGet 方法，传入 Key 和 StartTS
	val, err := txn.client.SnapshotGet(ctx, key, txn.StartTS)
	if err != nil {
		return nil, err
	}
	
	// 3. 将读到的数据缓存到 Buffer 吗？
	// 通常不需要，除非是为了 Repeatable Read 的校验。
	// Percolator 模型在 Prewrite 阶段会检查 Write Conflict，所以这里不需要缓存读集。
	
	return val, nil
}