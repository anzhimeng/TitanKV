package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"titankv/api/titankvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	targetAddr = "127.0.0.1:9091"
	keyCount   = 500  // 写入 Key 的数量
	blobSize   = 5000 // 5KB，确保大于 4KB 阈值，进入 BlobStore
)

func main() {
	// 1. 连接 Server
	conn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Connect failed: %v", err)
	}
	defer conn.Close()
	c := titankvpb.NewTitanKVClient(conn)
	ctx := context.Background()

	// 2. 第一轮写入 (此时 Valid Ratio = 1.0)
	fmt.Println(">> Phase 1: Initial Write (Creating Blob Files)...")
	val1 := strings.Repeat("A", blobSize)
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("gc-key-%d", i)
		if _, err := c.Put(ctx, &titankvpb.PutRequest{Key: []byte(key), Value: []byte(val1)}); err != nil {
			log.Fatalf("Put failed: %v", err)
		}
	}
	fmt.Println("   Phase 1 Done. Data is live.")

	// 3. 第二轮覆盖写 (制造垃圾)
	// Key 相同，Value 不同。此时旧的 Blob 数据变成垃圾。
	// 理论上此时 Valid Ratio 降至 0.5 (如果都在一个文件里)
	fmt.Println(">> Phase 2: Overwrite (Generating Garbage)...")
	val2 := strings.Repeat("B", blobSize)
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("gc-key-%d", i)
		if _, err := c.Put(ctx, &titankvpb.PutRequest{Key: []byte(key), Value: []byte(val2)}); err != nil {
			log.Fatalf("Overwrite failed: %v", err)
		}
	}
	fmt.Println("   Phase 2 Done. Garbage generated.")

	// 4. 等待后台 GC 触发
	// 我们在 C++ 设置了 10秒 轮询一次。这里等待 15秒 确保覆盖一次周期。
	fmt.Println(">> Phase 3: Waiting for Background GC (15s)...")
	fmt.Println("   请观察 Server 端日志，期待看到 [BlobGC] Picked file...")
	time.Sleep(15 * time.Second)

	// 5. 验证数据正确性
	// GC 不应该把有效数据删了，也不应该回滚到旧数据
	fmt.Println(">> Phase 4: Verifying Data Integrity...")
	    var errorCount int // 增加计数
		for i := 0; i < keyCount; i++ {
			key := fmt.Sprintf("gc-key-%d", i)
			resp, err := c.Get(ctx, &titankvpb.GetRequest{Key: []byte(key)})
			if err != nil {
				log.Printf("❌ Get failed for %s: %v", key, err)
	            errorCount++
				continue
			}
			
			if string(resp.Value) != val2 {
				log.Printf("❌ Data Mismatch! Key: %s...", key)
	            errorCount++
			}
		}
	
	    if errorCount > 0 {
	        log.Fatalf("Test Failed with %d errors", errorCount)
	    }
	fmt.Println("✅ All Data Verified! GC Logic is Safe.")

}