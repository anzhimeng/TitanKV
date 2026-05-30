package txn

import (
	"context"
	"testing"
)

// Mock Client
type mockClient struct {
    // ...
}

// (为了测试 Transaction 逻辑，我们其实不需要真实的 Client，只需要测试 buffer 行为)

func TestTxnBuffer(t *testing.T) {
    // 1. 创建事务 (手动构造，绕过 NewTransaction 的网络请求)
    txn := &Transaction{
        StartTS: 100,
        buffer:  make(map[string][]byte),
    }

    key := []byte("key1")
    val := []byte("val1")

    // 2. Set
    txn.Set(key, val)
    
    // 3. Get (Read-Your-Writes)
    res, err := txn.Get(context.Background(), key)
    if err != nil {
        t.Fatalf("Get failed: %v", err)
    }
    if string(res) != string(val) {
        t.Errorf("Want %s, got %s", val, res)
    }

    // 4. Delete
    txn.Delete(key)
    
    // 5. Get (Should be nil/not found)
    // 根据我们的实现，Get 返回 nil, nil 表示删除，或者返回 error
    // check implementation: if val==nil -> error("key not found")
	val, err = txn.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Expected nil after delete, got error: %v", err)
	}
	if val != nil {
		t.Fatalf("Expected nil after delete, got %v", val)
	}
}
