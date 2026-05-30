package txn

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"titankv/pkg/client"

	"github.com/stretchr/testify/assert"
)

// 辅助：获取客户端
func getClient(t *testing.T) *client.Client {
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

var keyCounter int64

func uniqueKey(prefix string) []byte {
	id := atomic.AddInt64(&keyCounter, 1)
	return []byte(fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), id))
}

// Test 1: 基础事务流程 (ACID)
// 1. 开启事务 -> Set -> Get (读己之写)
// 2. Commit
// 3. 开启新事务 -> Get (读已提交)
func TestBasicACID(t *testing.T) {
	c := getClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := uniqueKey("acid")
	val := []byte("value-1")
	ts1, _ := c.GetTS(ctx)
	t.Logf("Txn 1 StartTS: %d", ts1)
	// 1. Txn 1
	txn1, err := NewTransaction(ctx, c)
	if err != nil {
		t.Fatalf("NewTransaction failed: %v", err)
	}

	txn1.Set(key, val)

	// Read Your Writes
	v, err := txn1.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, val, v, "Should read my own write")

	// Commit
	err = txn1.Commit(ctx)
	assert.NoError(t, err, "Commit failed")

	// 2. Txn 2 (Read Committed)
	txn2, err := NewTransaction(ctx, c)
	if err != nil {
		t.Fatalf("NewTransaction failed: %v", err)
	}
	ts2, _ := c.GetTS(ctx) // NewTransaction 内部获取
	t.Logf("Txn 2 StartTS: %d", ts2)
	v2, err := txn2.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, val, v2, "Should read committed data")
}

// Test 2: 原子性验证 (Prewrite 可见性)
// 1. Txn 1 Prewrite 但不 Commit
// 2. Txn 2 读取 -> 应该读不到数据 (或者 Block)
// 3. Txn 1 Commit
// 4. Txn 2 读取 -> 读到数据
// 注意：由于我们还没有手动控制 Prewrite 的接口暴露给 Test，
// 我们这里通过 Hack 方式或者逻辑推演验证。
// 我们可以利用 "多行事务"，如果 Secondary 还没 Commit，读取者应该能通过 Primary 状态查到。
func TestAtomicity_SecondaryCommit(t *testing.T) {
	c := getClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	k1 := uniqueKey("atom-1")
	k2 := uniqueKey("atom-2")
	val := []byte("val-atom")

	txn1, _ := NewTransaction(ctx, c)
	txn1.Set(k1, val)
	txn1.Set(k2, val)

	// Commit 内部会先 Prewrite 全部，然后 Commit Primary，最后异步 Commit Secondary
	// 如果 Commit 返回成功，说明 Primary 已经 Commit。
	err := txn1.Commit(ctx)
	assert.NoError(t, err)

	// 立即开启 Txn 2 读取
	// 此时 Secondary 可能还没 Commit (Async)，但 Get 逻辑应该能处理这种情况 (Roll-forward)
	// 只要能读到，就说明原子性机制 (Primary Check) 工作正常。
	txn2, _ := NewTransaction(ctx, c)
	v1, _ := txn2.Get(ctx, k1)
	v2, _ := txn2.Get(ctx, k2)

	assert.Equal(t, val, v1)
	assert.Equal(t, val, v2)
}

// Test 3: 并发写冲突 (Concurrency)
// 两个事务同时修改同一个 Key
// 预期：一个成功，一个失败（或重试后成功）
func TestConcurrentWriteWithBackoff(t *testing.T) {
	c := getClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := uniqueKey("concurrent")
	defer func() {
		// 强制删除 Lock CF 和 Default CF (需要暴露 DeleteCF 接口给 Client，或者简单 Delete)
		// 简单 Delete 只删 Default，不删 Lock。
		// 如果我们没有 DeleteCF 接口，这里其实很难清理干净。
		// 既然如此，我们依靠 uniqueKey 避免下一次测试冲突。
		// 所以这里的清理更多是形式上的。
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cleanupCancel()
		txn, _ := NewTransaction(cleanupCtx, c)
		txn.Delete(key)
		txn.Commit(cleanupCtx)
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	// Txn A: Set "A"
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if ctx.Err() != nil {
				return
			}
			txnCtx, txnCancel := context.WithTimeout(ctx, 2*time.Second)
			txn, err := NewTransaction(txnCtx, c)
			if err != nil {
				txnCancel()
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Duration(rand.Intn(200)+50) * time.Millisecond):
					continue
				}
			}
			txn.Set(key, []byte("A"))
			if err := txn.Commit(txnCtx); err == nil {
				txnCancel()
				t.Log("Txn A success")
				return
			}
			txnCancel()
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()

	// Txn B: Set "B"
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
		for i := 0; i < 50; i++ {
			if ctx.Err() != nil {
				return
			}
			txnCtx, txnCancel := context.WithTimeout(ctx, 2*time.Second)
			txn, err := NewTransaction(txnCtx, c)
			if err != nil {
				txnCancel()
				select {
				case <-ctx.Done():
					return
				case <-time.After(10 * time.Millisecond):
					continue
				}
			}
			txn.Set(key, []byte("B"))
			if err := txn.Commit(txnCtx); err == nil {
				txnCancel()
				t.Log("Txn B success")
				return
			}
			txnCancel()
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()

	wg.Wait()
	// 验证最终结果
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer verifyCancel()
	verifyTxn, _ := NewTransaction(verifyCtx, c)
	val, err := verifyTxn.Get(verifyCtx, key)
	assert.NoError(t, err)

	finalVal := string(val)
	t.Logf("Final value: %s", finalVal)

	// 只要是 A 或 B 之一就算通过
	if finalVal != "A" && finalVal != "B" {
		t.Fatalf("Unexpected value: %s", finalVal)
	} else {
		t.Log("Concurrent write test PASSED!")
	}
}
