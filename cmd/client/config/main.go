package main

import (
	"context"
	"log"

	"titankv/api/titankvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)


func main() {
    conn, _ := grpc.Dial("127.0.0.1:9091", grpc.WithTransportCredentials(insecure.NewCredentials()))
    c := titankvpb.NewTitanKVClient(conn)
    
    // 将 GC 阈值改为 0.1 (非常保守，几乎不 GC)
    _, err := c.UpdateConfig(context.Background(), &titankvpb.UpdateConfigRequest{
        GcThreshold: 0.1,
    })
    if err != nil {
        log.Fatal(err)
    }
    log.Println("Config updated!")
}
