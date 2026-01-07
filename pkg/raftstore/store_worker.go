package raftstore

import (
	"time"
)

const (
    batchSize = 128 // 一次最多处理多少消息
)

type StoreWorker struct {
	peers    map[uint64]*Peer // 本 Worker 管理的所有 Peer
	receiver PeerSender       // 接收消息的 Channel
	router   *Router
    
    // 缓存待处理的 Peers，避免每次遍历所有 map
    pendingPeers map[uint64]*Peer 
}

func NewStoreWorker(router *Router) *StoreWorker {
	return &StoreWorker{
		peers:        make(map[uint64]*Peer),
		receiver:     make(PeerSender, 4096),
		router:       router,
        pendingPeers: make(map[uint64]*Peer),
	}
}

func (w *StoreWorker) Run() {
	// 全局 Ticker，驱动所有 Peer 的心跳
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// 批处理缓冲区
	msgs := make([]Msg, 0, batchSize)

	for {
        // 1. 获取第一条消息 (阻塞)
		select {
		case msg := <-w.receiver:
			msgs = append(msgs, msg)
		case <-ticker.C:
			w.onTick()
            // Tick 之后也需要检查 Ready，继续往下走
		}

		// 2. 尝试获取更多消息 (非阻塞，Batching)
        // 尽可能多地从 channel 拿数据，直到拿满 batchSize 或者 channel 空了
		pending := len(w.receiver)
		if pending > batchSize {
			pending = batchSize
		}
		for i := 0; i < pending; i++ {
			msgs = append(msgs, <-w.receiver)
		}

		// 3. 处理消息批次
		for _, msg := range msgs {
			w.processMsg(msg)
		}
        
        // 清空 buffer
        msgs = msgs[:0]

		// 4. 处理 Ready (IO 聚合)
		w.handleReady()
	}
}

func (w *StoreWorker) processMsg(msg Msg) {
    peer, ok := w.peers[msg.RegionID]
    if !ok {
        // 可能是新 Region，或者已移除
        // 处理 CreateRegion 逻辑...
        return
    }
    
    // 执行 Raft Step
    peer.step(msg)
    
    // 标记该 Peer 有变动
    w.pendingPeers[msg.RegionID] = peer
}

func (w *StoreWorker) onTick() {
    // 对所有 Peer 进行 Tick
    // 优化点：不要一次性 Tick 10000 个 Peer，会卡顿
    // 应该分批 Tick。这里简化处理。
    for _, peer := range w.peers {
        peer.step(NewMsgTick())
        w.pendingPeers[peer.regionID] = peer
    }
}

// 核心优化：批量处理 IO
func (w *StoreWorker) handleReady() {
    // 1. 遍历所有活跃的 Peer
    for id, peer := range w.pendingPeers {
        if !peer.hasReady() {
            continue
        }
        
        rd := peer.raftGroup.Ready()
        
        // 2. 【TODO Week 10 Day 4】收集所有 rd.Entries -> WriteBatch
        // 3. 【TODO Week 10 Day 4】收集所有 rd.Messages -> Transport
        
        // 4. 推进状态
        peer.raftGroup.Advance(rd)
    }
    
    // 清空活跃列表
    w.pendingPeers = make(map[uint64]*Peer)
}