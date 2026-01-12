package id

import (
	"context"
	"encoding/binary"
	"sync"
	"log"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// 每次预分配的步长
	allocStep = uint64(1000)
	// Etcd 中存储 ID 的 Key
	idPath = "/pd/alloc_id"
)

// Allocator 负责生成全局唯一的 ID
type Allocator struct {
	client *clientv3.Client
	mu     sync.Mutex
	base   uint64 // 当前内存中可用的起始 ID
	end    uint64 // 当前内存中可用的结束 ID (不包含)
}

func NewAllocator(client *clientv3.Client) *Allocator {
	a := &Allocator{
		client: client,
	}
    // 【新增】尝试读取一次当前 Base (仅用于调试打印)
    // 注意：这里没有 Context，可以用 Background
    resp, err := client.Get(context.Background(), idPath)
    var currentBase uint64 = 0
    if err == nil && len(resp.Kvs) > 0 {
        currentBase = binary.BigEndian.Uint64(resp.Kvs[0].Value)
    }
    log.Printf("[IDAllocator] Initialized. Current Etcd Base: %d", currentBase)
    
	return a
}

// Alloc 分配 ID (支持 CAS 乐观锁重试)
func (a *Allocator) Alloc(ctx context.Context) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// 1. 内存够用，直接返回
	if a.base < a.end {
		id := a.base
		a.base++
		return id, nil
	}

	// 2. 内存不够，CAS 循环申请
	for {
		// A. 获取当前 Etcd 值和版本号 (Revision)
		resp, err := a.client.Get(ctx, idPath)
		if err != nil {
			return 0, err
		}

		var currentMax uint64 = 0
		var currentRev int64 = 0

		if len(resp.Kvs) > 0 {
			currentMax = binary.BigEndian.Uint64(resp.Kvs[0].Value)
			currentRev = resp.Kvs[0].ModRevision // 获取版本号
		}

		// B. 计算新值
		nextEnd := currentMax + allocStep
		valVal := make([]byte, 8)
		binary.BigEndian.PutUint64(valVal, nextEnd)

		// C. 构造 CAS 事务
		// If (ModRevision == currentRev) Then (Put nextEnd) Else (Fail)
		cmp := clientv3.Compare(clientv3.ModRevision(idPath), "=", currentRev)
		put := clientv3.OpPut(idPath, string(valVal))

		txnResp, err := a.client.Txn(ctx).If(cmp).Then(put).Commit()
		if err != nil {
			return 0, err
		}

		// D. 检查结果
		if txnResp.Succeeded {
			// 成功抢到了锁！更新内存
			a.base = currentMax + 1
			a.end = nextEnd
			
			id := a.base
			a.base++
			return id, nil
		}
		
		// E. 失败了 (txnResp.Succeeded == false)
		// 说明在 Get 和 Txn 之间，有另一个 PD 修改了该 Key。
		// 循环继续，重新 Get，重新尝试...
	}
}