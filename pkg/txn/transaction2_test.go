package txn

import (
	"context"
	"fmt"
	"testing"
	"time"

	"titankv/api/titankvpb"
	"titankv/pkg/client"
)

// 辅助：创建 Client
func newTestClient(t *testing.T) *client.Client {
	t.Helper()
	c, err := client.NewClient("127.0.0.1:9000")
	if err != nil {
		t.Skipf("PD unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.GetTS(ctx); err != nil {
		t.Skipf("PD unavailable: %v", err)
	}
	leaderCtx, leaderCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer leaderCancel()
	if _, err := c.LocateLeader(leaderCtx, []byte("health")); err != nil {
		t.Skipf("Cluster unavailable: %v", err)
	}
	return c
}

// TestTxnAtomicity 验证：
// 1. Prewrite 成功后，锁存在。
// 2. 此时另一个事务读，应该被阻塞（或报错 KeyLocked）。
// 3. 原事务不 Commit（模拟 Crash），数据对外界不可见。
func TestTxnAtomicity(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	key := []byte(fmt.Sprintf("atomicity-key-%d", time.Now().UnixNano()))
	val := []byte("atomicity-val")

	// --- 阶段 1: 模拟一个只做了一半的事务 (Prewrite Only) ---

	// 1. 获取 StartTS
	t.Log("Getting TS...")
	ts1, err := c.GetTS(ctx)
	t.Logf("Got TS: %d", ts1)
	if err != nil {
		t.Fatalf("GetTS failed: %v", err)
	}

	// 2. 手动构造并发送 Prewrite (Primary Key = key)
	// 这里我们需要调用 Client 的 SendPrewrite
	// 这是一个“作弊”操作，绕过了 Transaction.Commit 的后续步骤
	req := &titankvpb.PrewriteRequest{
		Context: &titankvpb.RegionContext{RegionId: 1, RegionEpoch: &titankvpb.RegionEpoch{ConfVer: 1, Version: 1}}, // 简化 Context
		Mutations: []*titankvpb.Mutation{
			{Op: titankvpb.Mutation_Put, Key: key, Value: val},
		},
		PrimaryKey: key,
		StartTs:    ts1,
		LockTtl:    2000, // 2秒 TTL，方便测试超时
	}

	// 调用 Client 的底层发送接口
	// 注意：SendPrewrite 是在 pkg/client 定义的，如果它是公开的 (首字母大写)，我们可以直接调。
	// 如果它是小写 sendPrewrite，我们需要在 client 包里加个 export_test.go，或者在这里测试只能在 pkg/client 下。
	// 假设你 Week 14 Day 1 把它定义为了 SendPrewrite (大写)。
	resp, err := c.SendPrewrite(ctx, req)
	if err != nil {
		t.Fatalf("Prewrite failed: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("Prewrite error: %s", resp.Error)
	}
	t.Logf("Txn 1 Prewrite success. Key is locked.")

	// --- 阶段 2: 另一个事务尝试读取 ---

	t2, _ := NewTransaction(ctx, c) // 获取了更新的 StartTS (ts2 > ts1)

	// 设置一个短一点的超时，因为我们预期它会 Backoff 直到超时
	readCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	_, err = t2.Get(readCtx, key)

	// 验证结果
	if err == nil {
		t.Fatal("Txn 2 should NOT read uncommitted data!")
	} else {
		// 期望错误是 Context Deadline Exceeded (因为一直在 Backoff 重试)
		// 或者 KeyLocked (如果没做 Backoff)
		t.Logf("Txn 2 blocked as expected: %v", err)
	}

	// --- 阶段 3: 等待 TTL 过期后 (Week 15 内容) ---
	// 此时如果再读，Week 15 的 ResolveLock 应该能清理它。
	// Week 14 阶段，锁过期后 Get 依然会报错 KeyLocked (因为锁还在，只是过期了，需要有人去清)。
	// 所以到此为止，测试通过。
}
