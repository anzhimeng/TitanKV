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
	for i := 0; i < 30; i++ {
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
         addr, ok := clusterMap[leaderID]
         if !ok {
             log.Printf("Unknown leaderID: %d", leaderID)
             return
         }

         conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
         if err != nil {
             log.Printf("Connect failed: %v", err)
             return
         }
         // 【修改 2】defer Close 要放在 Dial 成功之后
         // 注意：在 for 循环里用 defer 会导致连接直到函数退出才释放
         // 为了简单起见，我们在 break 或者 continue 前手动 close，或者用闭包
         // 这里为了演示简单，先不优化连接池
        
         c := titankvpb.NewTitanKVClient(conn)

         req := &titankvpb.PutRequest{
             Context: &titankvpb.RegionContext{RegionId: 1},
             Key:   []byte(key),
             Value: []byte(val),
         }
 
         // 【关键修复】你原来漏掉了这一行！真正调用 RPC
         resp, err := c.Put(ctx, req)
        
         conn.Close() // 用完立即关闭连接

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
		if resp.ErrCode != 0 {
            log.Printf("Put failed: %d", resp.ErrCode)
            // 简单的 Leader 切换逻辑：轮询下一个
            // 实际项目中应该读取 resp.Err.NotLeader.Leader.StoreId
            leaderID++
            if leaderID > 3 { leaderID = 1 }
            time.Sleep(100 * time.Millisecond)
            continue
          }
		log.Printf("Put %s success", key)
		break
	}
}
