package raft

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
	"titankv/api/titankvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Transport 管理与其他节点的连接
type Transport struct {
	peers map[uint64]string // ID -> Address
	conns map[uint64]*grpc.ClientConn
	mu    sync.Mutex
}

func NewTransport(peers map[uint64]string) *Transport {
	return &Transport{
		peers: peers,
		conns: make(map[uint64]*grpc.ClientConn),
	}
}

// 获取或创建到某个 Peer 的连接
func (t *Transport) GetPeerClient(id uint64) (titankvpb.TitanKVClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 1. 如果已有连接，直接返回
	if conn, ok := t.conns[id]; ok {
		return titankvpb.NewTitanKVClient(conn), nil
	}

	// 2. 建立新连接
	addr, ok := t.peers[id]
	if !ok {
		return nil, fmt.Errorf("unknown peer %d", id)
	}

	// 生产环境应配置重试策略和 KeepAlive
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	t.conns[id] = conn
	return titankvpb.NewTitanKVClient(conn), nil
}

// 发送消息 (异步发送，不阻塞 Raft 主循环)
func (t *Transport) Send(id uint64, msg *titankvpb.RaftMessage) {
	go func() {
		client, err := t.GetPeerClient(id)
		if err != nil {
			log.Printf("Failed to get client for %d: %v", id, err)
			return
		}

		// 设置超时，防止网络卡死
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err = client.Raft(ctx, msg)
		if err != nil {
			// 在真正的 Raft 中，发送失败是正常的（比如对方宕机），
			// etcd/raft 会在下次 Tick 时重试或进行选举。
			// 这里打印日志方便调试。
			// log.Printf("Failed to send message to %d: %v", id, err)
		}
	}()
}

func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, conn := range t.conns {
		conn.Close()
	}
}