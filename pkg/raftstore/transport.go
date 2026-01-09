package raftstore

import (
	"context"
	"fmt"
	"sync"
	"time"
	"io"
	"log"
     "os"


	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
	
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Transport struct {
	storeAddrs map[uint64]string
	pdClient   pdpb.PDClient
	
	mu sync.RWMutex
	// StoreID -> Stream
	streams map[uint64]titankvpb.TitanKV_BatchRaftClient
	conns   map[uint64]*grpc.ClientConn
	failedStores map[uint64]time.Time
	streamCancels map[uint64]context.CancelFunc
}

func NewTransport(storeAddrs map[uint64]string, pdClient pdpb.PDClient) *Transport {
	return &Transport{
		storeAddrs: storeAddrs,
		conns:      make(map[uint64]*grpc.ClientConn),
		streams:    make(map[uint64]titankvpb.TitanKV_BatchRaftClient),
       	pdClient:   pdClient,
       	failedStores: make(map[uint64]time.Time),
       	streamCancels: make(map[uint64]context.CancelFunc),
	}
}
// 批量发送消息
func (t *Transport) Send(msgs []*titankvpb.RaftMessage) {
	// 1. 按 Store 分组
	batches := make(map[uint64]*titankvpb.BatchRaftMessage)
	for _, msg := range msgs {
		sid := msg.ToStoreId // 现在直接用这个
		if _, ok := batches[sid]; !ok {
			batches[sid] = &titankvpb.BatchRaftMessage{}
		}
		batches[sid].Msgs = append(batches[sid].Msgs, msg)
	}

	// 2. 并发发送
	for storeID, batch := range batches {
		go t.sendBatch(storeID, batch)
	}
}

func (t *Transport) sendBatch(storeID uint64, batch *titankvpb.BatchRaftMessage) {
	stream, err := t.getStream(storeID)
	if err != nil {
		// 连接失败，尝试重连或丢弃
		if err.Error() != "silent_cooldown" {

		}
		log.Printf("Get stream for store %d failed: %v", storeID, err)
		t.closeStream(storeID) // 清理坏连接
		return
	}

	if err := stream.Send(batch); err != nil {
		log.Printf("Send batch to store %d failed: %v", storeID, err)
		t.closeStream(storeID) // 发生错误，关闭流，下次重连
	}
}


// 获取或创建流
func (t *Transport) getStream(storeID uint64) (titankvpb.TitanKV_BatchRaftClient, error) {
	// 1. 检查是否在冷却期 (无锁快查)
	// 注意：这里访问 map 是非线程安全的，如果并发读写会 panic。
    // 为了安全，我们把 failedStores 的访问也放在 mu 锁内，或者使用 sync.Map。
    // 鉴于这里是 Transport 的热点，我们把 failedStores 的检查放在 RLock 里面。
    
	t.mu.RLock()
    if lastFail, ok := t.failedStores[storeID]; ok {
        if time.Since(lastFail) < 3*time.Second {
            t.mu.RUnlock()
            return nil, fmt.Errorf("store %d is unreachable (cooldown)", storeID)
        }
    }
	stream, ok := t.streams[storeID]
	t.mu.RUnlock()
	
	if ok {
		return stream, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Double check
	if stream, ok = t.streams[storeID]; ok {
		return stream, nil
	}

	// 2. 获取连接
	conn, ok := t.conns[storeID]
	if !ok {
		addr, ok := t.storeAddrs[storeID]
		if !ok {
			// 去 PD 查
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			resp, err := t.pdClient.GetStore(ctx, &pdpb.GetStoreRequest{StoreId: storeID})
			cancel()
			if err != nil {
                // 记录失败，触发冷却
                t.failedStores[storeID] = time.Now()
				return nil, err
			}
			addr = resp.Store.Address
			t.storeAddrs[storeID] = addr
            log.Printf("[Transport] Resolved Store %d -> %s", storeID, addr)
		}
		
		var err error
		conn, err = grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
            t.failedStores[storeID] = time.Now()
			return nil, err
		}
		t.conns[storeID] = conn
	}

	// 3. 创建流
	client := titankvpb.NewTitanKVClient(conn)
   	 // 【修改】创建带 Cancel 的 Context
  	  ctx, cancel := context.WithCancel(context.Background())
    	newStream, err := client.BatchRaft(ctx)
   	 if err != nil {
       	 cancel()
       	 return nil, err
    	}
    // 成功建立连接，清除失败记录
    delete(t.failedStores, storeID)

	go func() {
		for {
			_, err := newStream.Recv()
			if err != nil {
				return
			}
		}
	}()

	t.streams[storeID] = newStream
	t.streamCancels[storeID] = cancel
	return newStream, nil
}

func (t *Transport) closeStream(storeID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.streams, storeID)
	if conn, ok := t.conns[storeID]; ok {
		conn.Close()
		delete(t.conns, storeID)
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

        // Hack 方式：假设 ID 4 -> 127.0.0.1:9094
        // addr = fmt.Sprintf("127.0.0.1:%d", 9090+storeID)
        // log.Printf("[Transport] Resolving Store %d -> %s", storeID, addr)
        
        // 【新增】从 PD 查询 Store 信息
        // 需要在 pdpb.proto 中定义 GetStore 接口 (Week 8 应该有，如果没有需补上)
        // 假设 proto 里有 GetStore
        ctx, cancel := context.WithTimeout(context.Background(), time.Second)
        req := &pdpb.GetStoreRequest{StoreId: storeID}
        resp, err := t.pdClient.GetStore(ctx, req)
        cancel()
        
        if err != nil {
            return nil, fmt.Errorf("resolve store %d failed: %v", storeID, err)
        }
        
        addr = resp.Store.Address
       
	}
	
	t.storeAddrs[storeID] = addr
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
    
    // 1. 先取消所有 Stream
    for id, cancel := range t.streamCancels {
        cancel()
        delete(t.streamCancels, id)
    }
    
    // 2. 再关闭连接
    for id, conn := range t.conns {
        conn.Close()
        delete(t.conns, id)
    }
    t.streams = make(map[uint64]titankvpb.TitanKV_BatchRaftClient)
}

func (t *Transport) SendSnapshot(msg *titankvpb.RaftMessage) error {
    // 1. 获取文件路径 (从 msg.Data 反序列化出 Snapshot，再从 Data 获取路径)
    var snap raftpb.Snapshot
    if err := snap.Unmarshal(msg.Data); err != nil {
	   return err
    }
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