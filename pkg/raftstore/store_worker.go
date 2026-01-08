package raftstore

import (
	"log"
	"time"
	"context"
	
	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
	"titankv/pkg/store" // C++ 引擎

	"google.golang.org/protobuf/proto"
)

const (
	batchSize = 128
	// Region 最大阈值 (96MB)
     MaxRegionSize = /*96*/10 * 1024 * 1024 
     // 检查间隔 (每隔多少次 Tick 检查一次，避免频繁调用 CGO)
     SplitCheckInterval = 10 
)

type MsgAddPeer struct {
	Peer *Peer
}

type StoreWorker struct {
	peers        map[uint64]*Peer
	receiver     PeerSender
	router       *Router
	pendingPeers map[uint64]*Peer
	store        *store.TitanStore
	transport    *Transport // 【新增】持有 Transport
	// 【新增】PD Client
	pdClient pdpb.PDClient
	
	tickCount uint64
}

func NewStoreWorker(router *Router, trans *Transport, s *store.TitanStore, client pdpb.PDClient) *StoreWorker {
	return &StoreWorker{
		peers:        make(map[uint64]*Peer),
		receiver:     make(PeerSender, 4096),
		router:       router,
		pendingPeers: make(map[uint64]*Peer),
		store:        s,
		transport:    trans, // 赋值
		pdClient:     client, // 赋值
	}
}

func (w *StoreWorker) Receiver() PeerSender {
	return w.receiver
}

func (w *StoreWorker) Run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	msgs := make([]Msg, 0, batchSize)

	for {
		select {
		case msg := <-w.receiver:
			msgs = append(msgs, msg)
		case <-ticker.C:
			w.onTick()
		}

		pending := len(w.receiver)
		if pending > batchSize {
			pending = batchSize
		}
		for i := 0; i < pending; i++ {
			msgs = append(msgs, <-w.receiver)
		}

		for _, msg := range msgs {
			w.processMsg(msg)
		}
		msgs = msgs[:0]

		w.handleReady()
	}
}

func (w *StoreWorker) processMsg(msg Msg) {
	// 【修改】优先处理不需要 Peer 上下文的消息 (如创建 Peer)
	// 如果以后有 MsgCreatePeer，放在这里
	
	// 获取 Peer
	peer, ok := w.peers[msg.RegionID]
	if !ok {
		// Region 不存在，可能是已经被 Split 移除，或者发错了
		// log.Printf("[Worker] Msg %v for non-existent region %d", msg.Type, msg.RegionID)
		return
	}

	// 【Day 2 新增】处理 SplitCheck
	if msg.Type == MsgTypeSplitCheck {
		w.onSplitCheck(msg.RegionID)
		return
	}

	// 处理其他消息 (RaftMessage, RaftCmd, Tick)
	peer.step(msg)
	
	// 标记为活跃，以便后续 handleReady 处理 IO
	w.pendingPeers[msg.RegionID] = peer
}
func (w *StoreWorker) onTick() {
    w.tickCount++

    // 1. Raft Tick
    for _, peer := range w.peers {
        peer.step(NewMsgTick())
        w.pendingPeers[peer.regionID] = peer
    }
    
    // 2. Split Check (低频执行)
    if w.tickCount % SplitCheckInterval == 0 {
        w.checkSplit()
    }
}


func (w *StoreWorker) handleReady() {
	var messages []*titankvpb.RaftMessage
	var readyPeers []*Peer
	
	// CGO Batch Write 需要的切片
	var batchKeys [][]byte
	var batchValues [][]byte

	for _, peer := range w.pendingPeers {
		if !peer.hasReady() {
			continue
		}
		
		rd := peer.raftGroup.Ready()
		
		// A. 收集日志和状态 (WAL)
		// peer.storage.Append 返回 []kvPair
		kvPairs, err := peer.storage.Append(rd.Entries, &rd.HardState)
		if err != nil {
			log.Fatalf("Append failed: %v", err)
		}
		
		// 拆解 kvPair 到两个切片
		for _, kv := range kvPairs {
			batchKeys = append(batchKeys, kv.key)
			batchValues = append(batchValues, kv.value)
		}
		
		// B. 收集网络消息
		for _, msg := range rd.Messages {
			data, _ := msg.Marshal()
			tm := &titankvpb.RaftMessage{
				RegionId:   peer.regionID,
				FromPeerId: msg.From,
				ToPeerId:   msg.To,
				Data:       data,
			}
			messages = append(messages, tm)
		}
		
		readyPeers = append(readyPeers, peer)
	}

	// 2. 执行阶段：批量写盘 (Atomic & Batch)
	if len(batchKeys) > 0 {
		// 调用 CGO BatchPut
		err := w.store.BatchPut(batchKeys, batchValues)
		if err != nil {
			 log.Fatalf("BatchPut failed: %v", err)
		}
	}

	// 3. 执行阶段：批量发送
	if len(messages) > 0 {
		w.transport.Send(messages)
	}

	// 4. 后处理阶段：Apply & Advance
	for _, peer := range readyPeers {
		rd := peer.raftGroup.Ready()
		for _, entry := range rd.CommittedEntries {
			peer.processEntry(entry)
		}
		peer.raftGroup.Advance(rd)
	}

	w.pendingPeers = make(map[uint64]*Peer)
}

func (w *StoreWorker) AddPeer(p *Peer) {
	w.peers[p.regionID] = p
	w.pendingPeers[p.regionID] = p 
}

func (w *StoreWorker) checkSplit() {
    for _, peer := range w.peers {
        // 只有 Leader 才有资格发起 Split
        // 且 Region 正在运行中
        if peer.raftGroup.Status().Lead != peer.peerID {
            continue
        }
        
        // 调用底层引擎估算大小
        // DataKey 是加了 z 前缀的
        start := DataKey(peer.regionID, peer.region.StartKey)
        end := DataKey(peer.regionID, peer.region.EndKey) // 注意 EndKey 为空需处理
        
        // 如果 EndKey 为空，说明是无穷大。C++ 估算时需要一个具体的 Key。
        // 简单处理：如果是无穷大，构造成 z{RegionID+1} 作为边界
        if len(peer.region.EndKey) == 0 {
             end = DataKey(peer.regionID + 1, nil)
        }

        sizes := w.store.GetApproximateSizes([][]byte{start}, [][]byte{end})
        size := sizes[0]
        
        if size >= MaxRegionSize {
            log.Printf("Region %d size %d exceeds threshold, triggering split...", peer.regionID, size)
            
            // 3. 计算 Split Key
            // 我们需要找到中间的 Key。C++ 需要提供一个 Scan 接口或者 GetMiddleKey 接口。
            // 简化版：这里我们生成一个 MsgSplit 消息给自己，
            // 真正的 SplitKey 计算逻辑在处理该消息时做（Day 2 内容）。
            
            // 发送内部消息触发
            w.router.Send(peer.regionID, NewMsgSplitCheck(peer.regionID))
        }
        // 【测试专用】强制触发
        if w.tickCount == 50 {
             log.Println("DEBUG: Triggering Split Check for Region", peer.regionID)
             w.processMsg(NewMsgSplitCheck(peer.regionID)) // 直接处理，不用发 Channel 也行
        }
    }
}

func (w *StoreWorker) onSplitCheck(regionID uint64) {
    peer := w.peers[regionID]
    
    // 1. 扫描数据找到 SplitKey
    // 生产环境：调用 C++ Scan 扫描一半大小的数据，取那个 Key。
    // Day 2 简化：假设我们知道大概中间是 "key-50000" (结合之前的 bench_main)
    // 或者我们直接把 StartKey 和 EndKey 取字典序中间值。
    
    // 假设我们有一个 FindMiddleKey 函数
    splitKey := []byte("bench-key-50000") // Mock
    
    // 2. 向 PD 申请新 ID
    // 新 Region 的 ID
    newRegionID, err := w.askPDAllocID()
    if err != nil { return }
    
    // 新 Region 对应的 Peers 的 ID (每个副本都需要一个新 ID)
    var newPeerIDs []uint64
    for range peer.region.Peers {
        pid, _ := w.askPDAllocID()
        newPeerIDs = append(newPeerIDs, pid)
    }
    
    // 3. 构造 Admin Request
    adminReq := &titankvpb.AdminRequest{
        CmdType: titankvpb.AdminRequest_SPLIT,
        Split: &titankvpb.SplitRequest{
            SplitKey:    splitKey,
            NewRegionId: newRegionID,
            NewPeerIds:  newPeerIDs,
        },
    }
    
    cmd := &titankvpb.RaftCommand{
        Type:         titankvpb.RaftCommand_ADMIN,
        AdminRequest: adminReq,
    }
    
    // 4. Propose
    // Admin Command 和普通 Put/Delete 一样走 Raft 流程
    data, _ := proto.Marshal(cmd)
    peer.raftGroup.Propose(data)
    
    log.Printf("[Split] Proposing split at key %s for Region %d", string(splitKey), regionID)
}

func (w *StoreWorker) askPDAllocID() (uint64, error) {
    // 调用 PD RPC
    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()
    resp, err := w.pdClient.AllocID(ctx, &pdpb.AllocIDRequest{})
    if err != nil {
        return 0, err
    }
    return resp.Id, nil
}