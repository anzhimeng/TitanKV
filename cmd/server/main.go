package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time" // ✅ 删除：不再需要，Batcher 配置已移至 Raft 内部
	"net/http"
     _ "net/http/pprof" // 注册 pprof 路由

	"titankv/api/titankvpb"
	"titankv/pkg/raft"
	"titankv/pkg/service"
	"titankv/pkg/store"

	"google.golang.org/grpc"

	
	"github.com/prometheus/client_golang/prometheus"
     "github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	port    = flag.Int("port", 9090, "The server port")
	dbPath  = flag.String("db_path", "/tmp/titankv_data", "Path to DB data")
	nodeID  = flag.Uint64("id", 1, "Raft Node ID")
	cluster = flag.String("cluster", "1=127.0.0.1:9090", "Cluster configuration")
	directIO = flag.Bool("direct_io", false, "Enable Direct IO (io_uring)")
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
	// 建议：为了防止同一台机器开多个节点冲突，可以在路径后追加 ID
	// 但为了保持和你原有逻辑一致，这里先不做修改
    log.Printf("Opening storage at %s (DirectIO: %v)...", *dbPath, *directIO)
    db, err := store.Open(*dbPath, *directIO)
	if err != nil {
		log.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// 【新增】启动 Metrics 采集循环
    go func() {
        ticker := time.NewTicker(5 * time.Second)
        for range ticker.C {
            stats := db.GetStatistics()
            gcRunCounter.Set(float64(stats.GCRunCount))
            gcKeysMoved.Set(float64(stats.GCKeysMoved))
            // log.Printf("GC Stats: Run=%d, Moved=%d", stats.GCRunCount, stats.GCKeysMoved)
        }
    }()

	// 3. 初始化 Raft 节点
	// 注意：NewTitanRaft 内部现在会自动初始化 Batcher，不需要外部手动创建了
	log.Printf("Starting Raft Node %d...", *nodeID)
	raftNode := raft.NewTitanRaft(*nodeID, peers, db, *dbPath)
	batcher := raft.NewBatcher(raftNode, 500, 10*time.Millisecond)
	// 4. 监听端口
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

    go func() {
    	   http.Handle("/metrics", promhttp.Handler())
        pprofAddr := fmt.Sprintf("0.0.0.0:%d", 6060+*nodeID) // 避免端口冲突: 6061, 6062...
        log.Printf("Pprof listening on %s", pprofAddr)
        log.Println(http.ListenAndServe(pprofAddr, nil))
    }()

	// 5. 创建 gRPC 服务器
	grpcServer := grpc.NewServer()

	titanServer := service.NewServer(raftNode, batcher, db)

	titankvpb.RegisterTitanKVServer(grpcServer, titanServer)

	// 6. 优雅关闭
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down gRPC server...")
		grpcServer.GracefulStop()
	}()

	log.Printf("Server listening on port %d", *port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}