package txn

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"titankv/pkg/client"
)

func newPessimisticTestClient(t *testing.T) *client.Client {
	t.Helper()
	c, err := client.NewClient("127.0.0.1:2379") // Standard PD port
	if err != nil {
		// Try another port if standard fails, or skip
		c, err = client.NewClient("127.0.0.1:9000")
		if err != nil {
			t.Skipf("Cluster unavailable: %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.GetTS(ctx); err != nil {
		t.Skipf("PD unavailable: %v", err)
	}
	return c
}

func TestPessimisticLockConflict(t *testing.T) {
	c := newPessimisticTestClient(t)
	ctx := context.Background()

	key := []byte(fmt.Sprintf("pessimistic-%d", time.Now().UnixNano()))

	// Txn 1: Acquire Lock
	txn1, err := NewTransaction(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := txn1.EnablePessimistic(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("Txn1 StartTS: %d", txn1.StartTS)

	if err := txn1.LockKeys(ctx, [][]byte{key}); err != nil {
		t.Fatalf("Txn1 LockKeys failed: %v", err)
	}
	t.Log("Txn1 acquired lock")

	// Txn 2: Try to Acquire Lock (Should Fail)
	txn2, err := NewTransaction(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := txn2.EnablePessimistic(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("Txn2 StartTS: %d", txn2.StartTS)

	// Use a short timeout for Txn2 because it might retry internally
	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	err = txn2.LockKeys(ctx2, [][]byte{key})
	if err == nil {
		t.Fatal("Txn2 should have failed to acquire lock, but succeeded")
	} else {
		t.Logf("Txn2 failed as expected: %v", err)
		// Expected error should contain "KeyLocked" or "acquire pessimistic lock failed"
		if !strings.Contains(err.Error(), "KeyLocked") && !strings.Contains(err.Error(), "key locked") && !strings.Contains(err.Error(), "acquire pessimistic lock failed") && !strings.Contains(err.Error(), "Key is locked by other txn") {
			t.Errorf("Unexpected error message: %v", err)
		}
	}

	// Txn 1: Commit
	txn1.Set(key, []byte("val1"))
	if err := txn1.Commit(ctx); err != nil {
		t.Fatalf("Txn1 Commit failed: %v", err)
	}
	t.Log("Txn1 Committed")

	// Txn 3: Acquire Lock (Should Succeed now that Txn1 is committed)
	// Wait a bit for async apply?
	time.Sleep(100 * time.Millisecond)

	txn3, err := NewTransaction(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := txn3.EnablePessimistic(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("Txn3 StartTS: %d", txn3.StartTS)

	if err := txn3.LockKeys(ctx, [][]byte{key}); err != nil {
		// Note: Since Txn1 committed, Txn3 should see the new value.
		// But Locking should succeed because the lock was released (committed).
		// Unless there is a write conflict (Txn3.StartTS < Txn1.CommitTS)?
		// Txn3 is created AFTER Txn1 Commit. So Txn3.StartTS > Txn1.CommitTS.
		// So it should succeed.
		t.Fatalf("Txn3 LockKeys failed: %v", err)
	}
	t.Log("Txn3 acquired lock")
}
