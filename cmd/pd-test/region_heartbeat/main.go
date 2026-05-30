package main

import (
	"context"
	"log"

	"titankv/pd/api/pdpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 1. 【修复关键】建立 gRPC 连接
	// 假设 PD 监听在 9000 端口 (根据 Week 8 Day 2 的设置)
	targetAddr := "127.0.0.1:9000"
	conn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	// 2. 【修复关键】初始化 Client 和 Context
	client := pdpb.NewPDClient(conn)
	ctx := context.Background()

	// 3. 构造测试数据
	// 模拟一个 Region (ID=100, Range=["a", "z"])
	region := &pdpb.Region{
		Id:       100,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		RegionEpoch: &pdpb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers: []*pdpb.Peer{
			{Id: 1, StoreId: 1},
			{Id: 2, StoreId: 2},
			{Id: 3, StoreId: 3},
		},
	}

	leader := &pdpb.Peer{Id: 1, StoreId: 1} // Node 1 是 Leader

	// 4. 发送 Region 心跳
	log.Println("Sending Region Heartbeat...")
	_, err = client.RegionHeartbeat(ctx, &pdpb.RegionHeartbeatRequest{
		Region:          region,
		Leader:          leader,
		ApproximateSize: 64, // 64MB
	})
	if err != nil {
		log.Fatalf("Heartbeat failed: %v", err)
	}

	// 5. 验证 GetRegion (路由查询)
	log.Println("Querying Region for key 'b'...")
	resp, err := client.GetRegion(ctx, &pdpb.GetRegionRequest{Key: []byte("b")})
	if err != nil {
		log.Fatalf("GetRegion failed: %v", err)
	}

	if resp.Region == nil {
		log.Fatalf("Region not found!")
	}

	log.Printf("Found Region ID: %d", resp.Region.Id)
	
	// 检查 Leader 是否存在
	if resp.Leader != nil {
		log.Printf("Leader Store ID: %d", resp.Leader.StoreId)
		if resp.Leader.StoreId != 1 {
			log.Fatalf("Leader mismatch, want Store 1, got %d", resp.Leader.StoreId)
		}
	} else {
		log.Println("Warning: Leader info is nil (PD might not have updated cache yet)")
	}

	if resp.Region.Id != 100 {
		log.Fatalf("Region ID mismatch, want 100, got %d", resp.Region.Id)
	}

	log.Println("✅ Region Heartbeat & Routing Test Passed!")
}
