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

	_ "net/http/pprof"

	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb" // 暂时不用
	"titankv/pkg/raftstore"
	"titankv/pkg/service"
	"titankv/pkg/store"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	log.Printf("Parsed peers: %v", peers)
	
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

		// 4. 连接 PD
	pdAddr := "127.0.0.1:9000"
    
     // 【修改】正确定义 conn
	conn, err := grpc.Dial(pdAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
 	if err != nil {
		log.Fatalf("Failed to init pd: %v", err)
	}
     // 注意：conn 不要在 main 结束前关闭，或者你可以 defer conn.Close()
     // 但因为 server 是常驻进程，不关也没事，直到进程退出。
    
     pdClient := pdpb.NewPDClient(conn)


	// 5. 初始化 RaftStore (Multi-Raft 核心)
	log.Printf("Starting RaftStore (Node %d)...", *nodeID)
	
	router := raftstore.NewRouter()
	
	// 【新增】初始化 Transport
	trans := raftstore.NewTransport(peers)
	
	// 【修改】传入 Transport
	storeWorker := raftstore.NewStoreWorker(router, trans, db, pdClient)
	
	// 启动 Worker 线程
	go storeWorker.Run()

	// 初始化默认 Region (ID=1)
	// 在生产环境中，这里应该从 DB 加载所有 Region
	// 为了跑通 Day 3，我们手动创建一个
	initialRegion := &titankvpb.Region{
		Id:       1, 
		StartKey: nil, 
		EndKey:   nil,
		// Peers 需要包含当前集群的所有节点
		RegionEpoch: &titankvpb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	for id := range peers {
		initialRegion.Peers = append(initialRegion.Peers, &titankvpb.Peer{Id: id, StoreId: id})
	}

	// 创建 Peer 并注册
	// 注意：这里需要根据当前节点的 ID 来创建 Peer
	// 如果 initialRegion 包含 *nodeID，则创建
	// (在我们的测试配置中，peers 包含所有节点，所以肯定会创建)
	peer, err := raftstore.NewPeer(*nodeID, initialRegion, db) 
	if err != nil {
		log.Fatalf("Failed to create peer: %v", err)
	}
	
	// 注册到 Router 和 Worker
	// 注意：Receiver 是一个 Channel，Register 需要的是 chan Msg
	router.Register(1, storeWorker.Receiver())
	storeWorker.AddPeer(peer) 

	// 5. 监听端口
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// 6. 启动 HTTP 服务 (Pprof + Metrics)
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		pprofAddr := fmt.Sprintf("0.0.0.0:%d", 6060+*nodeID)
		log.Printf("Pprof & Metrics listening on %s", pprofAddr)
		log.Println(http.ListenAndServe(pprofAddr, nil))
	}()

	// 7. 创建 gRPC 服务器
	grpcServer := grpc.NewServer()
	
	// 【修改】NewServer 只需 router 和 db
	titanServer := service.NewServer(router, db)
	
	titankvpb.RegisterTitanKVServer(grpcServer, titanServer)

	// 8. 优雅关闭
	done := make(chan struct{})
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		
		log.Println("Shutting down gRPC server...")
		grpcServer.GracefulStop()
		
		// 停止 Transport
		trans.Close()
		
		close(done)
	}()

	log.Printf("Server listening on port %d", *port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

	<-done
	log.Println("TitanKV Server exit.")
}