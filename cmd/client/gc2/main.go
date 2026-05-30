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

	baseTS, err := c.GetTS(ctx)
	if err != nil {
		log.Fatalf("GetTS failed: %v", err)
	}
	basePhysical := int64(baseTS >> 18)
	past1 := int64((30 * time.Minute).Milliseconds())
	past2 := int64((20 * time.Minute).Milliseconds())
	future := int64((5 * time.Minute).Milliseconds())
	ts1 := uint64(basePhysical-past1) << 18
	ts2 := uint64(basePhysical-past2) << 18
	ts3 := uint64(basePhysical+future) << 18

	log.Printf(">> Writing Version 1 (TS=%d)...", ts1)
	mustCommit(c, key, []byte("v1"), ts1, ts1+5)
	
	log.Printf(">> Writing Version 2 (TS=%d)...", ts2)
	mustCommit(c, key, []byte("v2"), ts2, ts2+5)
	
	log.Printf(">> Writing Version 3 (TS=%d)...", ts3)
	mustCommit(c, key, []byte("v3"), ts3, ts3+5)

	// 2. 验证写入成功
	assertGet(c, key, ts1+10, "v1")
	assertGet(c, key, ts2+10, "v2")
	assertGet(c, key, ts3+10, "v3")
	log.Println(">> Initial read check passed.")

	log.Println(">> Waiting for PD-triggered GC...")
	deadline := time.Now().Add(2 * time.Minute)
	for {
		val, err := c.SnapshotGet(ctx, key, ts1+10)
		if err != nil || val == nil {
			break
		}
		if time.Now().After(deadline) {
			log.Fatalf("GC did not run in time")
		}
		time.Sleep(2 * time.Second)
	}

	log.Println(">> Verifying GC Result...")
	assertGet(c, key, ts2+10, "v2")
	assertGet(c, key, ts3+10, "v3")

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
