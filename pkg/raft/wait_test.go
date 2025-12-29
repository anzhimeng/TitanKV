package raft

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 模拟 TitanRaft 的部分功能进行测试
func TestWaitApplied(t *testing.T) {
	// 1. 构造一个模拟的 Raft 对象
	tr := &TitanRaft{}
	tr.applyCond = sync.NewCond(&tr.applyMu)
	atomic.StoreUint64(&tr.lastApplied, 100) // 初始 Index = 100

	// 用于覆盖原方法的 getApplied (这就需要我们把 getApplied 稍微改一下或者直接测试逻辑)
	// 这里我们直接测试 WaitApplied 的逻辑，因为它内部调用了 tr.getApplied
	// 确保 node.go 里的 getApplied 是: atomic.LoadUint64(&tr.lastApplied)

	var wg sync.WaitGroup

	// Case 1: 目标 Index 小于当前，应该立即返回
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := tr.WaitApplied(ctx, 90); err != nil {
			t.Errorf("WaitApplied(90) failed: %v", err)
		}
	}()

	// Case 2: 目标 Index 大于当前，应该阻塞直到更新
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		
		start := time.Now()
		// 等待 Index 105
		if err := tr.WaitApplied(ctx, 105); err != nil {
			t.Errorf("WaitApplied(105) failed: %v", err)
		}
		
		// 验证是否真的发生了等待
		if time.Since(start) < 100*time.Millisecond {
			t.Error("WaitApplied returned too fast, expected blocking")
		}
	}()

	// 模拟 Raft Apply 推进
	go func() {
		time.Sleep(500 * time.Millisecond) // 模拟处理耗时
		
		tr.applyMu.Lock() // 获取锁（配合 Cond）
		atomic.StoreUint64(&tr.lastApplied, 105) // 更新 Index
		tr.applyCond.Broadcast() // 广播唤醒
		tr.applyMu.Unlock()
	}()

	// Case 3: 超时测试
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 设置一个极短的超时
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		
		// 等待一个永远不到达的 Index
		err := tr.WaitApplied(ctx, 99999)
		if err != context.DeadlineExceeded {
			t.Errorf("Expected DeadlineExceeded, got %v", err)
		}
	}()

	wg.Wait()
}