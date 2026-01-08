package raftstore

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

type Transport struct {
	// StoreID -> Address (集群拓扑)
	storeAddrs map[uint64]string
	// StoreID -> Connection
	conns map[uint64]*grpc.ClientConn
	mu    sync.Mutex
}

func NewTransport(storeAddrs map[uint64]string) *Transport {
	return &Transport{
		storeAddrs: storeAddrs,
		conns:      make(map[uint64]*grpc.ClientConn),
	}
}

// 批量发送消息
// msgs: 已经封装好 RegionID 和 ToPeerID 的消息列表
func (t *Transport) Send(msgs []*titankvpb.RaftMessage) {
	// 1. 按目标 Store 分组 (减少锁竞争和连接切换)
	batches := make(map[uint64][]*titankvpb.RaftMessage)
	
	for _, msg := range msgs {
		// 我们需要知道 PeerID 属于哪个 StoreID
		// 这需要一个全局的 Peer -> Store 映射，或者 msg 里带上 ToStoreID
		// 简化：假设 msg.ToPeerId 就是 StoreID (或者通过某种规则映射)
		// 实际工业级：RaftMessage 应该带 ToStoreId
		toStoreId := msg.ToPeerId // Hack: 假设 1:1
		batches[toStoreId] = append(batches[toStoreId], msg)
	}

	// 2. 并发发送
	for storeID, batch := range batches {
		go t.sendToStore(storeID, batch)
	}
}

func (t *Transport) sendToStore(storeID uint64, msgs []*titankvpb.RaftMessage) {
	client, err := t.getClient(storeID)
	if err != nil {
		log.Printf("Get client for store %d failed: %v", storeID, err)
		return
	}

	// 这里可以进一步优化：调用 gRPC 的 Streaming 接口或者 Batch RPC
	// Day 4 简化：循环调用 Unary RPC (虽然还是多，但已经在 Goroutine 里了)
	for _, msg := range msgs {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := client.Raft(ctx, msg)
		cancel()
		if err != nil {
			// log.Printf("Send raft msg fail: %v", err)
		}
	}
}

func (t *Transport) getClient(storeID uint64) (titankvpb.TitanKVClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if conn, ok := t.conns[storeID]; ok {
		return titankvpb.NewTitanKVClient(conn), nil
	}

	addr, ok := t.storeAddrs[storeID]
	if !ok {
		return nil, fmt.Errorf("unknown store %d", storeID)
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.conns[storeID] = conn
	return titankvpb.NewTitanKVClient(conn), nil
}

func (t *Transport) Close() {
    t.mu.Lock()
    defer t.mu.Unlock()
    for _, conn := range t.conns {
        conn.Close()
    }
}

func (t *Transport) SendSnapshot(msg *titankvpb.RaftMessage) error {
    // 1. 获取文件路径 (从 msg.Data 反序列化出 Snapshot，再从 Data 获取路径)
    var snap raftpb.Snapshot
    proto.Unmarshal(msg.Data, &snap)
    filePath := string(snap.Data) // Day 2 我们把路径存这里了
    
    // 2. 打开文件
    file, err := os.Open(filePath)
    if err != nil { return err }
    defer file.Close()
    
    info, _ := file.Stat()
    
    // 3. 建立 Stream
    client, err := t.getClient(msg.ToPeerId)
    stream, err := client.StreamSnapshot(context.Background())
    
    // 4. 发送 Chunk
    buf := make([]byte, 1024*1024) // 1MB Chunk
    for {
        n, err := file.Read(buf)
        if err == io.EOF { break }
        
        chunk := &titankvpb.SnapshotChunk{
            RegionId: msg.RegionId,
            FileSize: uint64(info.Size()),
            Data:     buf[:n],
            IsLast:   false,
        }
        stream.Send(chunk)
    }
    
    // 发送最后一块
    // 序列化 Snapshot 元数据 (注意：snap.Data 此时是路径，接收端不需要路径，只需要 Metadata)
    // 我们可以清空 Data 字段只传 Metadata
    snapToSend := snap
    snapToSend.Data = nil 
    snapData, _ := snapToSend.Marshal()

    stream.Send(&titankvpb.SnapshotChunk{
        RegionId: msg.RegionId,
        IsLast:   true,
        RaftSnapshotData: snapData, // 【新增】
    })
    
    
    _, err = stream.CloseAndRecv()
    return err
}