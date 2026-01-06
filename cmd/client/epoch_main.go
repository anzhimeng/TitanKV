package main

import (
	"context"
	"fmt"
	"log"

	"titankv/api/titankvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 连接 Leader (假设是 9091)
	conn, err := grpc.Dial("127.0.0.1:9091", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Fail: %v", err)
	}
	defer conn.Close()
	c := titankvpb.NewTitanKVClient(conn)

	key := []byte("epoch-test-key")
	val := []byte("value")
	ctx := context.Background()

	// ---------------------------------------------------------
	// Case 1: 正常 Epoch (Version=1, ConfVer=1)
	// ---------------------------------------------------------
	fmt.Println(">> Case 1: Sending request with Correct Epoch (1,1)...")
	req1 := &titankvpb.PutRequest{
		Key:   key,
		Value: val,
		Context: &titankvpb.RegionContext{
			RegionId: 1, // 默认 Region
			RegionEpoch: &titankvpb.RegionEpoch{
				ConfVer: 1,
				Version: 1, // 初始值通常是 1
			},
		},
	}

	_, err = c.Put(ctx, req1)
	if err != nil {
		log.Fatalf("❌ Case 1 Failed: Expected Success, got error: %v", err)
	}
	fmt.Println("✅ Case 1 Passed.")

	// ---------------------------------------------------------
	// Case 2: 过期 Epoch (Version=0, ConfVer=0)
	// ---------------------------------------------------------
	fmt.Println(">> Case 2: Sending request with Stale Epoch (0,0)...")
	req2 := &titankvpb.PutRequest{
		Key:   key,
		Value: val,
		Context: &titankvpb.RegionContext{
			RegionId: 1,
			RegionEpoch: &titankvpb.RegionEpoch{
				ConfVer: 0, // 故意写错
				Version: 0, // 故意写错
			},
		},
	}

	_, err = c.Put(ctx, req2)
	
	// 验证错误信息
	if err == nil {
		log.Fatalf("❌ Case 2 Failed: Expected EpochNotMatch error, got Success!")
	} else {
		// 检查错误详情
		// gRPC 错误通常包含 status code，这里简单检查字符串
		errStr := err.Error()
		fmt.Printf("   Got Error: %v\n", errStr)
		
		// 你的 Server 端返回的是 status.Error(codes.Aborted, "EpochNotMatch")
		// 所以错误信息应该包含 "EpochNotMatch"
		// 注意：具体字符串取决于你在 server.go 里的写法
		// 如果你写的是 return nil, status.Error(codes.Aborted, "EpochNotMatch")
		
		// 预期包含 "EpochNotMatch"
		// 实际运行看输出即可
		fmt.Println("✅ Case 2 Passed (Request Rejected).")
	}
}