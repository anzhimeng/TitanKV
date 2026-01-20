package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"titankv/pd"
	"titankv/pd/api/pdpb"

	"google.golang.org/grpc"
)

var (
	name       = flag.String("name", "pd-1", "Human-readable name for this member")
	dataDir    = flag.String("data-dir", "/tmp/pd-1", "Path to the data directory")
	clientUrls = flag.String("client-urls", "http://127.0.0.1:2379", "List of URLs to listen on for client traffic")
	peerUrls   = flag.String("peer-urls", "http://127.0.0.1:2380", "List of URLs to listen on for peer traffic")
	cluster    = flag.String("initial-cluster", "pd-1=http://127.0.0.1:2380", "Initial cluster configuration")
	
	addr = flag.String("addr", ":9000", "gRPC server address")
	gcInterval = flag.Duration("gc-interval", 1*time.Minute, "GC interval")
	gcSafePointLag = flag.Duration("gc-safe-point-lag", 10*time.Minute, "GC safe point lag")
)

func main() {
	flag.Parse()

	cfg := &pd.Config{
		Name:           *name,
		DataDir:        *dataDir,
		ClientUrls:     strings.Split(*clientUrls, ","),
		PeerUrls:       strings.Split(*peerUrls, ","),
		InitialCluster: *cluster,
		GCInterval:     *gcInterval,
		GCSafePointLag: *gcSafePointLag,
	}

	server := pd.NewServer(cfg)
	
	// 1. 启动 PD 内部逻辑 (Etcd + TSO Loop)
	if err := server.Run(); err != nil {
		log.Fatalf("Failed to run PD server: %v", err)
	}

	// 2. 启动 gRPC 服务监听 (用于 Client 获取 TSO)
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", *addr, err)
	}
	
	grpcServer := grpc.NewServer()
	pdpb.RegisterPDServer(grpcServer, server)
	
	log.Printf("PD gRPC Server listening on %s", *addr)
	
	// 在后台启动 gRPC 服务
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	// 3. 优雅退出
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM)
	<-sc

	// 停止服务
	grpcServer.GracefulStop()
	server.Close()
	log.Println("PD Server stopped")
}
