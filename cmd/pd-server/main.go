package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"titankv/pd"
)

var (
	name       = flag.String("name", "pd-1", "Human-readable name for this member")
	dataDir    = flag.String("data-dir", "/tmp/pd-1", "Path to the data directory")
	clientUrls = flag.String("client-urls", "http://127.0.0.1:2379", "List of URLs to listen on for client traffic")
	peerUrls   = flag.String("peer-urls", "http://127.0.0.1:2380", "List of URLs to listen on for peer traffic")
	cluster    = flag.String("initial-cluster", "pd-1=http://127.0.0.1:2380", "Initial cluster configuration")
)

func main() {
	flag.Parse()

	cfg := &pd.Config{
		Name:           *name,
		DataDir:        *dataDir,
		ClientUrls:     strings.Split(*clientUrls, ","),
		PeerUrls:       strings.Split(*peerUrls, ","),
		InitialCluster: *cluster,
	}

	server := pd.NewServer(cfg)
	if err := server.Run(); err != nil {
		log.Fatalf("Failed to run PD server: %v", err)
	}

	// 优雅退出
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM)
	<-sc

	server.Close()
	log.Println("PD Server stopped")
}