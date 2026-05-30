package txn

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestPessimisticLifecycle(t *testing.T) {
	c := newPessimisticTestClient(t)
	ctx := context.Background()

	key := []byte(fmt.Sprintf("pessimistic-lifecycle-%d", time.Now().UnixNano()))
	val1 := []byte("val1")
	val2 := []byte("val2")

	// Txn 1: Acquire Lock, Write, Commit
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

	txn1.Set(key, val1)
	if err := txn1.Commit(ctx); err != nil {
		t.Fatalf("Txn1 Commit failed: %v", err)
	}
	t.Log("Txn1 Committed")

	// Txn 2: Read (Should see val1)
	txn2, err := NewTransaction(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	// Note: txn2 is optimistic read
	val, err := txn2.Get(ctx, key)
	if err != nil {
		t.Fatalf("Txn2 Get failed: %v", err)
	}
	if string(val) != string(val1) {
		t.Fatalf("Txn2 expected %s, got %s", val1, val)
	}
	t.Log("Txn2 Read val1 correctly")

	// Txn 3: Pessimistic Update (Read-Modify-Write)
	txn3, err := NewTransaction(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := txn3.EnablePessimistic(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("Txn3 StartTS: %d", txn3.StartTS)

	// Lock first
	if err := txn3.LockKeys(ctx, [][]byte{key}); err != nil {
		t.Fatalf("Txn3 LockKeys failed: %v", err)
	}
	t.Log("Txn3 acquired lock")

	// Read (should be snapshot read, seeing val1)
	val, err = txn3.Get(ctx, key)
	if err != nil {
		t.Fatalf("Txn3 Get failed: %v", err)
	}
	if string(val) != string(val1) {
		t.Fatalf("Txn3 expected %s, got %s", val1, val)
	}

	// Write val2
	txn3.Set(key, val2)
	if err := txn3.Commit(ctx); err != nil {
		t.Fatalf("Txn3 Commit failed: %v", err)
	}
	t.Log("Txn3 Committed")

	// Txn 4: Read Final Value
	txn4, err := NewTransaction(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	val, err = txn4.Get(ctx, key)
	if err != nil {
		t.Fatalf("Txn4 Get failed: %v", err)
	}
	if string(val) != string(val2) {
		t.Fatalf("Txn4 expected %s, got %s", val2, val)
	}
	t.Log("Txn4 Read val2 correctly")
}
