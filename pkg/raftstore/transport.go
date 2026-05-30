package raftstore

import (
	"context"
	"fmt"
	"sync"
	"time"
	"io"
	"log"
	"os"
	"hash/crc32"
	"encoding/json"

	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
	
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

var batchRaftMsgPool = sync.Pool{
	New: func() interface{} {
		return &titankvpb.BatchRaftMessage{}
	},
}

func acquireBatchRaftMessage() *titankvpb.BatchRaftMessage {
	return batchRaftMsgPool.Get().(*titankvpb.BatchRaftMessage)
}

func releaseBatchRaftMessage(msg *titankvpb.BatchRaftMessage) {
	msg.Reset()
	batchRaftMsgPool.Put(msg)
}

type transportWorker struct {
	storeID   uint64
	transport *Transport
	ch        chan *titankvpb.RaftMessage
	prioCh    chan *titankvpb.RaftMessage
	ctx       context.Context
	cancel    context.CancelFunc
}

func (w *transportWorker) run() {
	const maxBatchSize = 256
	batch := make([]*titankvpb.RaftMessage, 0, maxBatchSize)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		// 1. Draining Priority Channel Aggressively
		// Check for priority messages and flush them immediately after draining
		forceFlush := false
		for {
			select {
			case msg := <-w.prioCh:
				batch = append(batch, msg)
				forceFlush = true
				if len(batch) >= maxBatchSize {
					w.flush(batch)
					batch = batch[:0]
					forceFlush = false // Batch flushed
				}
				continue
			default:
			}
			break
		}

		if forceFlush && len(batch) > 0 {
			w.flush(batch)
			batch = batch[:0]
		}

		select {
		case <-w.ctx.Done():
			for _, msg := range batch {
				ReleaseRaftMessage(msg)
			}
			return
		case msg := <-w.prioCh:
			batch = append(batch, msg)
			// Trigger strict priority handling
			// Loop back to top to drain potential burst and flush
			continue
		case msg := <-w.ch:
			batch = append(batch, msg)
			if len(batch) >= maxBatchSize {
				w.flush(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				w.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

func (w *transportWorker) flush(msgs []*titankvpb.RaftMessage) {
	// 1. Get Batch from Pool
	batchMsg := acquireBatchRaftMessage()
	// 注意：这里我们直接引用 msgs slice，但是 Proto Marshal 会处理。
	// 不过 BatchRaftMessage.Msgs 是 []*RaftMessage。
	// 为了避免 slice 引用问题，我们可以 append。
	batchMsg.Msgs = append(batchMsg.Msgs, msgs...)

	// 2. Get Stream
	stream, err := w.transport.getStream(w.storeID)
	if err != nil {
		log.Printf("Get stream for store %d failed: %v", w.storeID, err)
		w.transport.closeStream(w.storeID)
	} else {
		// 3. Send
		if err := stream.Send(batchMsg); err != nil {
			log.Printf("Send batch to store %d failed: %v", w.storeID, err)
			w.transport.closeStream(w.storeID)
		}
	}

	// 4. Release Batch
	releaseBatchRaftMessage(batchMsg)

	// 5. Release Messages
	for _, msg := range msgs {
		ReleaseRaftMessage(msg)
	}
}

type Transport struct {
	storeAddrs map[uint64]string
	pdClient   pdpb.PDClient
	
	mu sync.RWMutex
	// StoreID -> Stream
	streams map[uint64]titankvpb.TitanKV_BatchRaftClient
	conns   map[uint64]*grpc.ClientConn
	failedStores map[uint64]time.Time
	streamCancels map[uint64]context.CancelFunc
	
	// 【新增】Workers
	workers sync.Map // map[uint64]*transportWorker
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
	for _, msg := range msgs {
		storeID := msg.ToStoreId
		if storeID == 0 {
			ReleaseRaftMessage(msg)
			continue
		}
		worker := t.getWorker(storeID)
		select {
		case worker.ch <- msg:
		default:
			log.Printf("Worker channel full for store %d, dropping message", storeID)
			ReleaseRaftMessage(msg)
		}
	}
}

// SendPrioritized sends messages to the priority channel (e.g., heartbeats)
func (t *Transport) SendPrioritized(msgs []*titankvpb.RaftMessage) {
	for _, msg := range msgs {
		storeID := msg.ToStoreId
		if storeID == 0 {
			ReleaseRaftMessage(msg)
			continue
		}
		// Skip getWorker if not found? No, getWorker creates it.
		// Note: getWorker is thread-safe.
		worker := t.getWorker(storeID)
		select {
		case worker.prioCh <- msg:
		default:
			log.Printf("Priority Worker channel full for store %d, dropping message", storeID)
			ReleaseRaftMessage(msg)
		}
	}
}

func (t *Transport) getWorker(storeID uint64) *transportWorker {
	if v, ok := t.workers.Load(storeID); ok {
		return v.(*transportWorker)
	}

	ctx, cancel := context.WithCancel(context.Background())
	w := &transportWorker{
		storeID:   storeID,
		transport: t,
		ch:        make(chan *titankvpb.RaftMessage, 4096),
		prioCh:    make(chan *titankvpb.RaftMessage, 4096), // Priority channel
		ctx:       ctx,
		cancel:    cancel,
	}

	actual, loaded := t.workers.LoadOrStore(storeID, w)
	if loaded {
		cancel()
		return actual.(*transportWorker)
	}

	go w.run()
	return w
}

// sendBatch is replaced by worker.flush, removing it.


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
	// 0. Stop workers
	t.workers.Range(func(key, value interface{}) bool {
		w := value.(*transportWorker)
		w.cancel()
		return true
	})

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
	
	var filePath string
	var snapData SnapshotData
	// 尝试解析为 SnapshotData
	if err := json.Unmarshal(snap.Data, &snapData); err == nil && snapData.FilePath != "" {
		filePath = snapData.FilePath
	} else {
		// 兼容旧格式（直接存路径）
		filePath = string(snap.Data)
	}
	
	// 2. 打开文件
    file, err := os.Open(filePath)
    if err != nil { return err }
    defer file.Close()
    
    info, _ := file.Stat()
    fileSize := uint64(info.Size())
    
    // 3. 建立 Stream
    client, err := t.getClient(msg.ToPeerId)
    stream, err := client.StreamSnapshot(context.Background())
    
    // 4. 发送 Chunk
    buf := make([]byte, 1024*1024) // 1MB Chunk
    hasher := crc32.NewIEEE()
    var offset uint64
    for {
        n, err := file.Read(buf)
        if err == io.EOF { break }
        
        chunk := &titankvpb.SnapshotChunk{
            RegionId: msg.RegionId,
            FileSize: fileSize,
            Offset:   offset,
            Data:     buf[:n],
            IsLast:   false,
        }
        stream.Send(chunk)
        hasher.Write(buf[:n])
        offset += uint64(n)
    }
    
    // 发送最后一块
	// 序列化 Snapshot 元数据
	// 注意：我们需要保留 Region 信息（在 snap.Data 中），但接收端不需要 Sender 的 FilePath。
	// 不过为了简单，我们直接把 snap.Data 传过去，接收端会覆盖 FilePath。
	snapToSend := snap
	// snapToSend.Data = nil // 不要清空 Data，因为里面包含 Region 信息！
	
	// 如果是 SnapshotData JSON，我们可以选择清空 FilePath 以减少传输量，但不是必须的。
	
	snapDataBytes, _ := snapToSend.Marshal()

	stream.Send(&titankvpb.SnapshotChunk{
		RegionId: msg.RegionId,
		FileSize: fileSize,
		Offset:   offset,
		Checksum: uint64(hasher.Sum32()),
		IsLast:   true,
		RaftSnapshotData: snapDataBytes, // 【修改】包含完整元数据和 Region 信息
	})
    
    
    _, err = stream.CloseAndRecv()
    return err
}
