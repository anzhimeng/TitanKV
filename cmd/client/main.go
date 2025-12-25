package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"titankv/api/titankvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var clusterMap = map[uint64]string{
	1: "127.0.0.1:9091",
	2: "127.0.0.1:9092",
	3: "127.0.0.1:9093",
}

func main() {
	// 循环写入 20 条数据，足以触发 snapshotCount=10 的阈值
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		put(key, val)
		// 稍微停顿一下，让日志好看点
		time.Sleep(200 * time.Millisecond)
	}
	log.Println("Done sending 20 requests.")
}

func put(key, val string) {
	leaderID := uint64(1) // 假设先连 Node 1
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		addr := clusterMap[leaderID]
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Connect failed: %v", err)
			return
		}
		c := titankvpb.NewTitanKVClient(conn)

		_, err = c.Put(ctx, &titankvpb.PutRequest{Key: []byte(key), Value: []byte(val)})
		conn.Close()

		if err != nil {
			// 网络错误重试
			time.Sleep(100 * time.Millisecond)
			continue
		}
		
		// 实际上 Server 可能返回 err=nil 但 ErrCode=1 (NotLeader)
		// 但由于我们在 Day 5 Step 3 中，如果不是 Leader 会返回 nil error + ErrCode=1
		// 这里简化：假设我们直接连对了，或者你可以复用之前的重定向逻辑。
		// 为了验证 Log Compaction，只要能写进去就行。
		// 如果控制台报错 "Not leader"，请手动修改上面的 leaderID := uint64(X)
		
		log.Printf("Put %s success", key)
		break
	}
}