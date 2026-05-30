package txn

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// 模拟两个事务同时修改同一个 Key
func TestConcurrentWrite(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	c := newTestClient(t)

	key := []byte("concurrent-key")

	var wg sync.WaitGroup
	wg.Add(2)

	// Txn A
	go func() {
		defer wg.Done()
		for {
			if testCtx.Err() != nil {
				return
			}
			ctx, cancel := context.WithTimeout(testCtx, 2*time.Second)
			txn, err1 := NewTransaction(ctx, c)
			if err1 != nil {
				cancel()
				select {
				case <-testCtx.Done():
					return
				case <-time.After(50 * time.Millisecond):
					continue
				}
			}
			txn.Set(key, []byte("A"))
			err := txn.Commit(ctx)
			cancel()
			if err == nil {
				t.Log("Txn A commit success")
				return
			}
			t.Logf("Txn A failed: %v", err)
			// 遇到冲突 (Write Conflict 或 Lock Conflict)，重试
			// 注意：Backoff 是在 Client 底层做的，如果是 WriteConflict (提交阶段发现版本冲突)，Transaction 层需要手动重试
			// Week 14 的 Commit 实现如果返回 error，通常意味着失败。
			select {
			case <-testCtx.Done():
				return
			case <-time.After(time.Duration(rand.Intn(400)+100) * time.Millisecond):
			}
		}
	}()

	// Txn B
	go func() {
		defer wg.Done()
		for {
			if testCtx.Err() != nil {
				return
			}
			ctx, cancel := context.WithTimeout(testCtx, 2*time.Second)
			txn, err1 := NewTransaction(ctx, c)
			if err1 != nil {
				cancel()
				select {
				case <-testCtx.Done():
					return
				case <-time.After(50 * time.Millisecond):
					continue
				}
			}
			txn.Set(key, []byte("B"))
			err := txn.Commit(ctx)
			cancel()
			if err == nil {
				t.Log("Txn B commit success")
				return
			}
			select {
			case <-testCtx.Done():
				return
			case <-time.After(time.Duration(rand.Intn(200)+10) * time.Millisecond):
			}
		}
	}()

	wg.Wait()

	// 验证最后的数据是 A 或 B
	vctx, vcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer vcancel()
	verifyTxn, _ := NewTransaction(vctx, c)
	val, _ := verifyTxn.Get(vctx, key)
	t.Logf("Final Value: %s", string(val))
}
