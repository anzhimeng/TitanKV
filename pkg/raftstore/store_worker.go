package raftstore

import (
	"log"
	"time"
	"titankv/api/titankvpb"
	"titankv/pkg/store" // C++ 引擎
)

const (
	batchSize = 128
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
}

func NewStoreWorker(router *Router, trans *Transport, s *store.TitanStore) *StoreWorker {
	return &StoreWorker{
		peers:        make(map[uint64]*Peer),
		receiver:     make(PeerSender, 4096),
		router:       router,
		pendingPeers: make(map[uint64]*Peer),
		store:        s,
		transport:    trans, // 赋值
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
	peer, ok := w.peers[msg.RegionID]
	if !ok {
		return
	}
	peer.step(msg)
	w.pendingPeers[msg.RegionID] = peer
}

func (w *StoreWorker) onTick() {
	for _, peer := range w.peers {
		peer.step(NewMsgTick())
		w.pendingPeers[peer.regionID] = peer
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