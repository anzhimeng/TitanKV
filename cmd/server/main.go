package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
	"context"

	_ "net/http/pprof"

	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb" // 暂时不用
	"titankv/pkg/raftstore"
	"titankv/pkg/service"
	"titankv/pkg/store"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var (
	port     = flag.Int("port", 9090, "The server port")
	dbPath   = flag.String("db_path", "/tmp/titankv_data", "Path to DB data")
	nodeID   = flag.Uint64("id", 1, "Raft Node ID")
	cluster  = flag.String("cluster", "1=127.0.0.1:9090", "Cluster configuration")
	directIO = flag.Bool("direct_io", false, "Enable Direct IO (io_uring)")
)

// 定义 Metrics
var (
	gcRunCounter = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "titankv_gc_run_count",
		Help: "Total number of GC runs",
	})
	gcKeysMoved = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "titankv_gc_keys_moved",
		Help: "Total keys moved by GC",
	})
)

func init() {
	prometheus.MustRegister(gcRunCounter)
	prometheus.MustRegister(gcKeysMoved)
}

func main() {
	flag.Parse()

	// 1. 解析集群配置
	peers := make(map[uint64]string)
	parts := strings.Split(*cluster, ",")
	for _, p := range parts {
		kv := strings.Split(p, "=")
		if len(kv) != 2 {
			log.Fatalf("Invalid cluster config: %s", p)
		}
		id, err := strconv.ParseUint(kv[0], 10, 64)
		if err != nil {
			log.Fatalf("Invalid node ID: %v", err)
		}
		peers[id] = kv[1]
	}
	// 【关键调试】打印解析结果
	log.Printf("----------------------------------------------------------------")
	log.Printf("My Node ID: %d", *nodeID)
	log.Printf("Cluster Config String: %s", *cluster)
	log.Printf("Parsed Peers Map: %v", peers)
	log.Printf("----------------------------------------------------------------")


	// 2. 初始化 C++ 存储引擎
	log.Printf("Opening storage at %s (DirectIO: %v)...", *dbPath, *directIO)
	db, err := store.Open(*dbPath, *directIO)
	if err != nil {
		log.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// 3. 启动 Metrics 采集
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			stats := db.GetStatistics()
			gcRunCounter.Set(float64(stats.GCRunCount))
			gcKeysMoved.Set(float64(stats.GCKeysMoved))
		}
	}()

	// 4. 连接 PD 并注册 Store
	pdAddr := "127.0.0.1:9000"
	conn, err := grpc.Dial(pdAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect pd: %v", err)
	}
	// 不关闭 conn，因为它需要一直存活
	pdClient := pdpb.NewPDClient(conn)
	defer conn.Close()

	// 【新增】注册 Store
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	meta := &pdpb.MetaStore{
		Id:      *nodeID,
		Address: peers[*nodeID],
		State:   pdpb.StoreState_UP,
		Version: "v1.0.0",
	}
	_, err = pdClient.PutStore(ctx, &pdpb.PutStoreRequest{
		Store: meta,
	})
	cancel()
	if err != nil {
		// 如果 PD 没启动，这里会报错，但我们可以选择只打印日志继续运行 (软依赖)
		log.Printf("WARNING: Failed to register store to PD: %v", err)
	} else {
		log.Printf("Successfully registered store %d to PD", *nodeID)
	}
	
	// 【可选】启动 Store 心跳 (Week 9 的内容，这里加上更完整)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			// 简单的 stats
			used := uint64(0)
			if *nodeID <= 3 {
			    used = 50 * 1024 * 1024 * 1024 // 50GB Used
			}
			_, err := pdClient.StoreHeartbeat(ctx, &pdpb.StoreHeartbeatRequest{
				StoreId: *nodeID,
				Stats: &pdpb.StoreStats{
					Capacity:  100 * 1024 * 1024 * 1024,
	                    Available: 100*1024*1024*1024 - used,
	                    RegionCount: uint32(used / (96 * 1024 * 1024)),
				},
			})
			cancel()
			if err != nil {
				if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
					retryCtx, retryCancel := context.WithTimeout(context.Background(), 3*time.Second)
					_, _ = pdClient.PutStore(retryCtx, &pdpb.PutStoreRequest{Store: meta})
					retryCancel()
				}
			}
		}
	}()

	// 5. 初始化 RaftStore (Multi-Raft 核心)
	log.Printf("Starting RaftStore (Node %d)...", *nodeID)
	
	router := raftstore.NewRouter()

	// 初始化 Transport
	trans := raftstore.NewTransport(peers, pdClient)
	
	// 传入 Transport 和 PDClient
	storeWorker := raftstore.NewStoreWorker(router, trans, db, pdClient)
	// 启动 Worker 线程
	go storeWorker.Run()
	router.RegisterStore(storeWorker.Receiver())
     if *nodeID <= 3 { // 假设 ID 1-3 是初始节点
     log.Printf("Bootstrapping initial region for node %d", *nodeID)
	// 初始化默认 Region (ID=1)
	initialRegion := &titankvpb.Region{
		Id:       1, 
		StartKey: nil, 
		EndKey:   nil,
		RegionEpoch: &titankvpb.RegionEpoch{ConfVer: 1, Version: 1},
	}
        for id := range peers {
             // 只有 1, 2, 3 才是初始成员
             if id <= 3 {
                 initialRegion.Peers = append(initialRegion.Peers, &titankvpb.Peer{Id: id, StoreId: id})
             }
        }

	// 创建 Peer 并注册
	peer, err := raftstore.NewPeer(*nodeID, initialRegion, db) 
	if err != nil {
		log.Fatalf("Failed to create peer: %v", err)
	}
	
	router.Register(1, storeWorker.Receiver(), peer)
	storeWorker.AddPeer(peer) 
    } else {
        log.Printf("Node %d is a new node, waiting for scheduling...", *nodeID)
    }


	// 6. 监听端口
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// 7. 启动 HTTP 服务 (Pprof + Metrics)
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		pprofAddr := fmt.Sprintf("0.0.0.0:%d", 6060+*nodeID)
		log.Printf("Pprof & Metrics listening on %s", pprofAddr)
		log.Println(http.ListenAndServe(pprofAddr, nil))
	}()

	// 8. 创建 gRPC 服务器
	grpcServer := grpc.NewServer()
	
	titanServer := service.NewServer(router, db)
	
	titankvpb.RegisterTitanKVServer(grpcServer, titanServer)

	// 9. 优雅关闭
	done := make(chan struct{})
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		
		log.Println("Received shutdown signal...")
		
		// 1. 先停止业务层 (Worker & Transport)
		// 这会切断 Client 端的 Stream，理应触发 Server 端的 Recv 错误
		log.Println("Stopping StoreWorker...")
		storeWorker.Stop()
		
		log.Println("Closing Transport...")
		trans.Close()
		
		// 2. 关闭 PD 连接
		// conn.Close() // 如果有的话

		// 3. 尝试优雅关闭 gRPC，带超时
		log.Println("Stopping gRPC server (Graceful)...")
		
		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		
		select {
		case <-stopped:
			log.Println("gRPC server stopped gracefully.")
		case <-time.After(5 * time.Second):
			log.Println("Graceful stop timed out, forcing Stop.")
			grpcServer.Stop()
		}
		
		close(done)
	}()

	log.Printf("Server listening on port %d", *port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

	<-done
	log.Println("TitanKV Server exit.")
}
