package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"titankv/api/titankvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	targetAddr = "127.0.0.1:9091"
	itemCount  = 3000 // 写入 3MB，触发多次 Split
	itemSize   = 1024
)

// 模拟路由表
type RegionRoute struct {
	RegionID uint64
	Version  uint64
	// 简单的 Range 判断：这里不存 Start/End，直接遇到错误就换 Region 试
}

var routes = []*RegionRoute{
	{RegionID: 1, Version: 1}, // 初始只有 Region 1
}

func main() {
	conn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { log.Fatalf("Connect: %v", err) }
	defer conn.Close()
	c := titankvpb.NewTitanKVClient(conn)

	fmt.Println("🚀 开始 Split 压力测试 (Smart Client Mode)...")

	successCount := 0
	
	for i := 0; i < itemCount; i++ {
		key := fmt.Sprintf("key-%05d", i)
		val := make([]byte, itemSize)

		// 简单的重试与路由寻找逻辑
		// 实际上应该查 PD，这里我们暴力轮询已知的所有 Region
		wrote := false
		
		for retry := 0; retry < 5; retry++ {
			// 尝试所有已知的 Region (包括可能分裂出来的新 Region)
			// 注意：随着 Split，RegionID 会增加。我们假设最大到 10。
			// 每次重试，我们尝试探测新的 RegionID。
			
			// 动态扩展路由表 (Mock PD)
			if len(routes) < 10 {
			    routes = append(routes, &RegionRoute{RegionID: uint64(len(routes)+1), Version: 1})
			}

			for _, r := range routes {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				_, err := c.Put(ctx, &titankvpb.PutRequest{
					Context: &titankvpb.RegionContext{
						RegionId: r.RegionID,
						RegionEpoch: &titankvpb.RegionEpoch{
							ConfVer: 1, 
							Version: r.Version, // 使用缓存的 Version
						},
					},
					Key: []byte(key),
					Value: val,
				})
				cancel()

				if err == nil {
					successCount++
					wrote = true
					break 
				}

				// 处理错误
				st, _ := status.FromError(err)
				if st.Code() == codes.Aborted && st.Message() == "EpochNotMatch" {
					// 版本过期了！更新 Version 并重试
					// log.Printf("Region %d Version outdated, update to %d", r.RegionID, r.Version+1)
					r.Version++
				}
				// 如果是 KeyNotInRegion (Range错)，则继续循环尝试下一个 Region
			}
			
			if wrote { break }
			time.Sleep(50 * time.Millisecond)
		}
		
		if i % 100 == 0 {
		    fmt.Printf("   Wrote %d keys... (Active Regions: %d)\r", i, countActiveRegions(routes))
		}
	}
	fmt.Printf("\n写入完成。成功: %d/%d\n", successCount, itemCount)

    // ... 验证逻辑 ...
}

func countActiveRegions(routes []*RegionRoute) int {
    return len(routes) // 简化统计
}
