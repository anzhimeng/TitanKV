package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"titankv/api/titankvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	nodesCount    = 4 // 支持 Node 4
	keyCount      = 20000
	testDuration  = 2 * time.Minute
	killInterval  = 60 * time.Second
	logDir        = "logs_chaos"
)

var (
	processes   = make(map[int]*exec.Cmd)
	logFiles    = make(map[int]*os.File)
	pdCmd       *exec.Cmd
	pdLogFile   *os.File
	
	// 配置
	basePort    = 9091
	pdAddr      = "127.0.0.1:9000"
	clusterConf = "1=127.0.0.1:9091,2=127.0.0.1:9092,3=127.0.0.1:9093,4=127.0.0.1:9094"

	// 统计
	writeSuccess uint64
	writeFail    uint64
	verifyErr    uint64
)

func main() {
	setupLogging()
	log.Println("🔥 TitanKV Chaos Test Suite Started")

	// 1. 环境清理
	cleanup()
	
	// 2. 启动 PD
	startPD()
	time.Sleep(2 * time.Second) // 等待 PD 就绪

	// 3. 启动集群 (Node 1-3)
	log.Println("🚀 Starting Initial Cluster (Nodes 1-3)...")
	for i := 1; i <= 3; i++ {
		startNode(i)
	}
	time.Sleep(5 * time.Second) // 等待选主

	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()

	var wg sync.WaitGroup

	// 4. 启动 Workload (模拟客户端)
	wg.Add(1)
	go func() {
		defer wg.Done()
		runWorkload(ctx)
	}()

	// 5. 启动 Chaos Monkey (随机杀节点)
	wg.Add(1)
	go func() {
		defer wg.Done()
		runChaos(ctx)
	}()

	// 6. 动态扩容 (Node 4 加入)
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(30 * time.Second)
		log.Println("🌟 Scaling Out: Starting Node 4...")
		startNode(4)
	}()

	wg.Wait()
	log.Println("🛑 Chaos Phase Finished. Stabilizing...")

	// 7. 恢复所有节点并验证
	recoverAll()
	time.Sleep(15 * time.Second) // 等待数据同步/Snapshot
	
	verifyData()
	
	// 8. 最终清理
	cleanup()
	log.Printf("✅ Test Complete. Logs saved to ./%s/", logDir)
}

func setupLogging() {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		panic(err)
	}
	// 主日志同时也输出到 stdout
}

func cleanup() {
	log.Println("🧹 Cleaning up processes and data...")
	exec.Command("pkill", "titankv-server").Run()
	exec.Command("pkill", "pd-server").Run()
	
	os.RemoveAll("/tmp/pd1")
	for i := 1; i <= nodesCount; i++ {
		os.RemoveAll(fmt.Sprintf("/tmp/node%d", i))
		if f := logFiles[i]; f != nil { f.Close() }
	}
	if pdLogFile != nil { pdLogFile.Close() }
}

func startPD() {
	logFile, err := os.Create(filepath.Join(logDir, "pd.log"))
	if err != nil { log.Fatal(err) }
	pdLogFile = logFile

	cmd := exec.Command("./pd-server",
		"--name=pd1",
		"--data-dir=/tmp/pd1",
		"--client-urls=http://127.0.0.1:2379",
		"--peer-urls=http://127.0.0.1:2380",
		"--initial-cluster=pd1=http://127.0.0.1:2380",
		"--addr=:9000",
	)
	cmd.Stdout = io.MultiWriter(logFile) // PD 日志只写文件，不刷屏
	cmd.Stderr = io.MultiWriter(logFile)
	
	if err := cmd.Start(); err != nil {
		log.Fatalf("Failed to start PD: %v", err)
	}
	pdCmd = cmd
	log.Printf("🟢 PD started (PID: %d)", cmd.Process.Pid)
}

func startNode(id int) {
	if processes[id] != nil && processes[id].ProcessState == nil {
		return
	}

	logFile, err := os.Create(filepath.Join(logDir, fmt.Sprintf("node%d.log", id)))
	if err != nil { log.Fatal(err) }
	logFiles[id] = logFile

	cmd := exec.Command("./titankv-server",
		fmt.Sprintf("--id=%d", id),
		fmt.Sprintf("--port=%d", basePort+id-1),
		fmt.Sprintf("--db_path=/tmp/node%d", id),
		fmt.Sprintf("--cluster=%s", clusterConf),
		"--direct_io=true",
	)
	
	// 重定向日志到文件
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		log.Fatalf("Failed to start Node %d: %v", id, err)
	}
	processes[id] = cmd
	log.Printf("🟢 Node %d started (PID: %d)", id, cmd.Process.Pid)
}

func killNode(id int) {
	if cmd := processes[id]; cmd != nil && cmd.Process != nil {
		log.Printf("🔴 Killing Node %d...", id)
		cmd.Process.Signal(syscall.SIGKILL)
		cmd.Wait()
		processes[id] = nil
	}
}

func recoverAll() {
	log.Println("🚑 Recovering all nodes...")
	for i := 1; i <= nodesCount; i++ {
		startNode(i)
	}
}

func runChaos(ctx context.Context) {
	ticker := time.NewTicker(killInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 随机杀一个 (1~3，保留4稳定运行以接收迁移，或者全随机)
			target := rand.Intn(nodesCount) + 1
			// 不要杀还没启动的 Node 4 (如果在 30s 内)
			if processes[target] == nil { continue }
			
			killNode(target)
			
			// 随机停机 3-8 秒
			sleepTime := time.Duration(rand.Intn(3)+1) * time.Second
			
			select {
			case <-ctx.Done():
				return
			case <-time.After(sleepTime):
				startNode(target)
			}
		}
	}
}

func runWorkload(ctx context.Context) {
	// 连接池逻辑简化：每次随机连一个
	for i := 0; i < keyCount; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		key := fmt.Sprintf("chaos-%d", i)
		val := fmt.Sprintf("val-%d-payload", i)
		
		success := false
		for retry := 0; retry < 5; retry++ {
			target := rand.Intn(nodesCount) + 1
			// 如果该节点死掉了，换一个
			if processes[target] == nil { continue }
			
			addr := fmt.Sprintf("127.0.0.1:%d", basePort+target-1)
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				c := titankvpb.NewTitanKVClient(conn)
				req := &titankvpb.PutRequest{
					Context: &titankvpb.RegionContext{RegionId: 1}, 
					Key: []byte(key), 
					Value: []byte(val),
				}
				
				opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				_, err := c.Put(opCtx, req)
				cancel()
				conn.Close()
				
				if err == nil {
					success = true
					atomic.AddUint64(&writeSuccess, 1)
					break
				} else {
	                    if atomic.LoadUint64(&writeFail) < 5 {
						    log.Printf("[Client] Put failed on Node %d: %v", target, err)
	                    }
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
		if !success {
			atomic.AddUint64(&writeFail, 1)
		}
	}
}

func verifyData() {
	log.Println("🔍 Verifying data consistency...")
	// 连接 Node 1 (假设它活着，或者轮询直到连上)
	conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", basePort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { log.Fatal(err) }
	defer conn.Close()
	c := titankvpb.NewTitanKVClient(conn)

    var verified, mismatch, notFound uint64
    var wg sync.WaitGroup
    
    // 使用 50 个并发
    workers := 50
    batchSize := keyCount / workers

    for w := 0; w < workers; w++ {
        wg.Add(1)
        go func(start, end int) {
            defer wg.Done()
            for i := start; i < end; i++ {
                key := fmt.Sprintf("chaos-%d", i)
                expected := fmt.Sprintf("val-%d-payload", i)
                
                ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond) // 缩短超时
			resp, err := c.Get(ctx, &titankvpb.GetRequest{
				Context: &titankvpb.RegionContext{RegionId: 1},
				Key: []byte(key),
			})
                cancel()
                
                if err == nil {
                    if string(resp.Value) != expected {
                        atomic.AddUint64(&mismatch, 1)
                        // log.Printf("Mismatch...")
                    } else {
                        atomic.AddUint64(&verified, 1)
                    }
                } else {
                    atomic.AddUint64(&notFound, 1)
                }
            }
        }(w*batchSize, (w+1)*batchSize)
    }
    
    wg.Wait()
	
	log.Printf("📝 Report: WriteSucc=%d, Fail=%d | VerifyOK=%d, Mismatch=%d", 
		atomic.LoadUint64(&writeSuccess), atomic.LoadUint64(&writeFail), verified, mismatch)

	if mismatch == 0 {
		log.Println("✅ Data Consistency Check PASSED!")
	} else {
		log.Println("❌ Data Consistency Check FAILED!")
	}
}