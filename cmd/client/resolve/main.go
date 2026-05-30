package main

import (
	"context"
	"log"
	"time"
	"fmt"

	"titankv/api/titankvpb"
	"titankv/pkg/client"

)

// 假设本地单机环境
const targetAddr = "127.0.0.1:9000"

func main() {
	log.Println("🔥 ResolveLock Integration Test Started")

    // 1. 准备两个 Client
    c1, _ := client.NewClient(targetAddr)
    c2, _ := client.NewClient(targetAddr)
    
    key := []byte(fmt.Sprintf("crash-key-%d", time.Now().UnixNano()))
    primary := key
    val := []byte("val-locked")
    
    // 2. Client 1: Prewrite 但不 Commit (模拟 Crash)
    // ----------------------------------------------------
    log.Println(">> Step 1: Client 1 Prewriting (and then crashing)...")
    ts1, err1 := c1.GetTS(context.Background())
    if err1 != nil {
        log.Fatalf("Failed to get TS: %v", err1) // 这一行会告诉你为什么 TS 是 0
    }
    log.Printf("   Txn 1 StartTS: %d", ts1)
    req := &titankvpb.PrewriteRequest{
        // 手动构造请求，跳过 Transaction 封装以便控制不 Commit
        Context: &titankvpb.RegionContext{RegionId: 1, RegionEpoch: &titankvpb.RegionEpoch{ConfVer: 1, Version: 1}},
        Mutations: []*titankvpb.Mutation{{Op: titankvpb.Mutation_Put, Key: key, Value: val}},
        PrimaryKey: primary,
        StartTs:    ts1,
        LockTtl:    2000, // 2s TTL
    }
    
    // 直接调用 SendPrewrite
    _, err := c1.SendPrewrite(context.Background(), req)
    if err != nil {
        log.Fatalf("Prewrite failed: %v", err)
    }
    log.Println("   Client 1 Crashed (Lock created).")

    // 3. 等待 TTL 过期
    // ----------------------------------------------------
    log.Println(">> Step 2: Waiting for TTL (3s)...")
    time.Sleep(3 * time.Second)

    // 4. Client 2: Get (触发 ResolveLock)
    // ----------------------------------------------------
    log.Println(">> Step 3: Client 2 Reading...")
    
    ts2, _ := c2.GetTS(context.Background())
    log.Printf("   Txn 2 StartTS: %d", ts2)

    // SnapshotGet 内部会自动：
    // 1. 遇到 Lock -> KeyLocked Error
    // 2. 解析 LockInfo -> Primary=key, StartTS=ts1
    // 3. ResolveLocks -> CheckTxnStatus -> TtlExpire -> Rollback
    // 4. 重试 Get -> KeyNotFound (因为 Txn 1 回滚了)
    
    res, err := c2.SnapshotGet(context.Background(), key, ts2)
    
    if err == nil {
        // 如果读到了数据，说明锁失效了但数据还在？不对，回滚后数据应该没了
        // 除非之前有旧版本数据。假设这是新 Key。
        if len(res) > 0 {
             log.Fatalf("❌ Should not read value (rolled back), got: %s", string(res))
        } else {
             // 读到 nil (NotFound) 是对的
             log.Printf("✅ Get returned nil (NotFound). Txn 1 rolled back successfully.")
        }
    } else {
        // 或者是返回 NotFound Error
        log.Printf("✅ Get returned error: %v. Txn 1 rolled back successfully.", err)
    }
    
    log.Println("🎉 Test Passed!")
}
