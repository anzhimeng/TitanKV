package main

import (
	"context"
	"flag"
	"log"
	"time"

	"titankv/pd/api/pdpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	pdAddr = flag.String("pd", "127.0.0.1:9000", "PD gRPC address")
)

func main() {
	flag.Parse()
	
	// 1. 连接 PD
	log.Printf("Connecting to PD at %s...", *pdAddr)
	conn, err := grpc.Dial(*pdAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()
	client := pdpb.NewPDClient(conn)

	ctx := context.Background()

	// 2. 申请 Store ID
	log.Println("Step 1: Allocating Store ID...")
	allocResp, err := client.AllocID(ctx, &pdpb.AllocIDRequest{})
	if err != nil {
		log.Fatalf("AllocID failed: %v", err)
	}
	storeID := allocResp.Id
	log.Printf("-> Allocated Store ID: %d", storeID)

	// 3. 注册 Store (PutStore)
	log.Println("Step 2: Registering Store...")
	storeMeta := &pdpb.MetaStore{
		Id:      storeID,
		Address: "127.0.0.1:20160",
		State:   pdpb.StoreState_UP,
		Version: "v1.0.0",
		Labels:  []*pdpb.StoreLabel{{Key: "zone", Value: "us-west"}},
	}
	_, err = client.PutStore(ctx, &pdpb.PutStoreRequest{Store: storeMeta})
	if err != nil {
		log.Fatalf("PutStore failed: %v", err)
	}
	log.Println("-> Store registered successfully.")

	// 4. 发送心跳 (模拟正常运行)
	log.Println("Step 3: Sending Heartbeats (5 times)...")
	for i := 0; i < 5; i++ {
		req := &pdpb.StoreHeartbeatRequest{
			StoreId: storeID,
			Stats: &pdpb.StoreStats{
				Capacity:  100 * 1024 * 1024 * 1024, // 100GB
				Available: 80 * 1024 * 1024 * 1024,  // 80GB
				RegionCount: 10,
			},
		}
		_, err := client.StoreHeartbeat(ctx, req)
		if err != nil {
			log.Printf("Heartbeat failed: %v", err)
		} else {
			log.Printf("-> Heartbeat %d sent.", i+1)
		}
		time.Sleep(1 * time.Second)
	}

	// 5. 停止心跳 (模拟宕机)
	log.Println("Step 4: Stopping heartbeats to simulate failure...")
	log.Println("   (Please observe PD Server logs for 'Disconnected' warning in ~20s)")
	
	// 为了观察方便，我们可以一直挂着不退出
	select {}
}
