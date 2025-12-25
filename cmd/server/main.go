package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"strings"
     "strconv"

	"titankv/api/titankvpb"
	"titankv/pkg/raft"
	"titankv/pkg/service"
	"titankv/pkg/store"

	"google.golang.org/grpc"
)

var (
	port   = flag.Int("port", 9090, "The server port")
	dbPath = flag.String("db_path", "/tmp/titankv_data", "Path to DB data")
	// 节点 ID
	nodeID = flag.Uint64("id", 1, "Raft Node ID")
	cluster = flag.String("cluster", "1=127.0.0.1:9090", "Cluster configuration")
)

func main() {
	flag.Parse()
	   
	// 解析集群配置
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

	

	// 1. 初始化 C++ 存储引擎
	log.Printf("Opening storage at %s...", *dbPath)
	db, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// 2. 【Day 3 新增】初始化 Raft 节点
	// 目前是单节点集群，Peers 只有自己 (ID=1)
     // 初始化 Raft (传入 peers map)
     log.Printf("Starting Raft Node %d with peers %v", *nodeID, peers)
     raftNode := raft.NewTitanRaft(*nodeID, peers, db)

	// 3. 监听端口
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// 4. 创建 gRPC 服务器
	grpcServer := grpc.NewServer()
	
	// 【关键修复】这里必须传入 raftNode 和 db
	titanServer := service.NewServer(raftNode, db)
	
	titankvpb.RegisterTitanKVServer(grpcServer, titanServer)

	// 5. 优雅关闭
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down gRPC server...")
		grpcServer.GracefulStop()
		// 这里也可以显式停止 raftNode
	}()

	log.Printf("Server listening on port %d", *port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}