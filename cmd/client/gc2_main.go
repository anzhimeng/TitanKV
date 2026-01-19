package main

import (
	"context"
	"log"
	"time"

     "titankv/pd/api/pdpb"
	"titankv/api/titankvpb"
	"titankv/pkg/client"

	
	"google.golang.org/grpc/codes"
     "google.golang.org/grpc/status"
)

const targetAddr = "127.0.0.1:9000"

func main() {
	log.Println("🔥 MVCC GC Test Started")

	c, _ := client.NewClient(targetAddr)
	ctx := context.Background()
     c.SnapshotGet(ctx, []byte("warmup"), 1)
	key := []byte("gc-test-key")

	// 1. 写入 3 个版本
	// 简单起见，我们手动指定 TS，而不是去 PD 拿（为了精确控制 SafePoint）
	// 注意：Server 并不校验 TS 是否来自 PD，只要递增就行
	ts1 := uint64(100)
	ts2 := uint64(200)
	ts3 := uint64(300)

	log.Println(">> Writing Version 1 (TS=100)...")
	mustCommit(c, key, []byte("v1"), ts1, ts1+5)
	
	log.Println(">> Writing Version 2 (TS=200)...")
	mustCommit(c, key, []byte("v2"), ts2, ts2+5)
	
	log.Println(">> Writing Version 3 (TS=300)...")
	mustCommit(c, key, []byte("v3"), ts3, ts3+5)

	// 2. 验证写入成功
	assertGet(c, key, ts1+10, "v1")
	assertGet(c, key, ts2+10, "v2")
	assertGet(c, key, ts3+10, "v3")
	log.Println(">> Initial read check passed.")

	// 3. 触发 GC
	// SafePoint = 250
	// 期望：
	// TS=100 被删除 (因为被 TS=200 覆盖，且 200 <= 250)
	// TS=200 保留 (SafePoint 前的最新版)
	// TS=300 保留
	log.Println(">> Triggering GC (SafePoint=250)...")
	
	// 我们需要给 Client 加一个 CallGC 的后门接口，或者直接用 RPC
	// 这里假设我们在 titankv.proto 加了一个 AdminGC 接口，或者复用 Batcher 的机制
	// 为了简单，我们 Hack 一下：在 Client 端直接调用 Server 的 Store.GC (如果这是单机测试)
	// 或者，我们在 server.go 加一个 debug 接口。
	
	// 假设我们通过某种方式触发了 Server 的 GC(250)
	// 如果你还没实现 RPC 触发，可以在 server main.go 里写死一个 timer 触发。
	// 这里我们模拟等待 Server 的定时任务 (假设你把 main.go 的 ticker 改成了 5s，且 safePoint 计算逻辑被 hack 为固定值)
	
	// 【方案】为了这个测试脚本能跑，建议在 titankv.proto 加一个 Compact 接口
    // 但为了不改 proto，我们假设 Server 已经自动 GC 了（通过修改 main.go 的 ticker 和 safePoint）
    // 这里我们只是打印提示，你需要手动去改 server main.go
    log.Println("!!! Please manually trigger GC on server with SafePoint=250 !!!")
    log.Println("!!! Or modify server/main.go to run GC(250) every 5s !!!")
    time.Sleep(10 * time.Second) // 等待 GC 执行

	// 4. 验证 GC 结果
	log.Println(">> Verifying GC Result...")
	
	// TS=150 读取：应该读不到 v1 了 (因为 v1 的 Write 记录被删了)
	// 这里的行为取决于 MvccGet 的实现。
	// 如果 Seek(150) 找不到 <= 150 的版本，返回 NotFound。
	val, err := c.SnapshotGet(ctx, key, 150)
	if err != nil || val == nil {
		log.Println("✅ v1 is GCed (NotFound).")
	} else {
		log.Fatalf("❌ v1 should be GCed! Got: %s", string(val))
	}

	// TS=250 读取：应该读到 v2
	assertGet(c, key, 250, "v2")
	
	// TS=350 读取：应该读到 v3
	assertGet(c, key, 350, "v3")

	log.Println("🎉 GC Test Passed!")
}

func mustCommit(c *client.Client, key, val []byte, start, commit uint64) {
    ctx := context.Background()
    
    for i := 0; i < 5; i++ {
        // 1. 每次循环都重新获取最新的路由信息 (Region Context)
        // 这样如果发生了 Epoch 变更，我们能拿到新的
        region, _ := c.GetRegionCache().Search(key)
        var regionCtx *titankvpb.RegionContext
        if region != nil {
            regionCtx = &titankvpb.RegionContext{
                RegionId:    region.Id,
                RegionEpoch: toTitanEpoch(region.RegionEpoch),
            }
        } else {
             regionCtx = &titankvpb.RegionContext{RegionId: 1} // Fallback
        }

        // 2. 构造请求
        req := &titankvpb.PrewriteRequest{
            Context:    regionCtx, // 使用最新的 Context
            Mutations:  []*titankvpb.Mutation{{Op: titankvpb.Mutation_Put, Key: key, Value: val}},
            PrimaryKey: key,
            StartTs:    start,
            LockTtl:    2000,
        }
        
        _, err := c.SendPrewrite(ctx, req)
        
        if err != nil {
             // 检查 Epoch，刷新缓存
             st, _ := status.FromError(err)
             if st.Code() == codes.Aborted && st.Message() == "EpochNotMatch" {
                 c.GetRegionCache().Invalidate(key)
                 // 强制重新 Locate (去 PD 拿)
                 c.LocateLeader(ctx, key) 
                 time.Sleep(100 * time.Millisecond)
                 continue
             }
             // ...
        }
        
        // 3. Commit
        creq := &titankvpb.CommitRequest{
            Context:  regionCtx, // 使用最新的 Context
            StartTs:  start,
            CommitTs: commit,
            Keys:     [][]byte{key},
        }
        
        _, err = c.SendCommit(ctx, creq)
        if err != nil {
             log.Printf("Commit failed: %v", err)
             st, _ := status.FromError(err)
             if st.Code() == codes.Aborted && st.Message() == "EpochNotMatch" {
                 c.GetRegionCache().Invalidate(key)
                 c.LocateLeader(ctx, key)
                 time.Sleep(100 * time.Millisecond)
                 continue
             }
             // 其他错误，重试
             continue 
        }
        
        return // 成功
    }
    log.Fatalf("mustCommit retries exceeded")
}

func assertGet(c *client.Client, key []byte, ts uint64, expect string) {
	val, err := c.SnapshotGet(context.Background(), key, ts)
	if err != nil {
		log.Fatalf("Get(%d) failed: %v", ts, err)
	}
	if string(val) != expect {
		log.Fatalf("Get(%d) mismatch. Want %s, Got %s", ts, expect, string(val))
	}
}

// 辅助函数：将 pdpb.RegionEpoch 转换为 titankvpb.RegionEpoch
func toTitanEpoch(e *pdpb.RegionEpoch) *titankvpb.RegionEpoch {
	if e == nil {
		return nil
	}
	return &titankvpb.RegionEpoch{
		ConfVer: e.ConfVer,
		Version: e.Version,
	}
}