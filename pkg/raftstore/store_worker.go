package raftstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"titankv/api/raft_serverpb"
	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
	"titankv/pkg/store" // C++ 引擎

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	batchSize = 1024
	// Region 最大阈值 (128MB)
	MaxRegionSize = 128 * 1024 * 1024
	// 检查间隔 (每隔多少次 Tick 检查一次，避免频繁调用 CGO)
	SplitCheckInterval      = 100
	PDHeartbeatTickInterval = 20
	TickBucketCount         = 8
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
	pdClient         pdpb.PDClient
	tickBuckets      []map[uint64]*Peer
	heartbeatCounter map[uint64]uint64
	peerStoreCache   map[uint64]uint64

	// Reuse buffers
	batchKeys   [][]byte
	batchValues [][]byte
	messages    []*titankvpb.RaftMessage
	prioMessages []*titankvpb.RaftMessage // Buffer for high priority messages
	tickCount   uint64
	ctx       context.Context
	cancel    context.CancelFunc

	// 【新增】Async Apply Channel
	applyCh chan *ApplyTask
	// 【新增】Pending Apply Tasks (Unbounded Buffer)
	pendingApplyTasks []*ApplyTask
}

type ApplyTask struct {
	RegionID         uint64
	Peer             *Peer
	CommittedEntries []raftpb.Entry
	Snapshot         raftpb.Snapshot
}

type ApplyResult struct {
	RegionID uint64
	NewPeer  *Peer // For Split
	Removed  bool  // For RemovePeer
}

func NewStoreWorker(router *Router, trans *Transport, s *store.TitanStore, client pdpb.PDClient) *StoreWorker {
	ctx, cancel := context.WithCancel(context.Background())
	tickBuckets := make([]map[uint64]*Peer, TickBucketCount)
	for i := range tickBuckets {
		tickBuckets[i] = make(map[uint64]*Peer)
	}
	w := &StoreWorker{
		peers:            make(map[uint64]*Peer),
		receiver:         make(PeerSender, batchSize),
		router:           router,
		pendingPeers:     make(map[uint64]*Peer),
		store:            s,
		transport:        trans,
		pdClient:         client,
		tickBuckets:      tickBuckets,
		heartbeatCounter: make(map[uint64]uint64),
		peerStoreCache:   make(map[uint64]uint64),
		batchKeys:        make([][]byte, 0, 4096),
		batchValues:      make([][]byte, 0, 4096),
		messages:         make([]*titankvpb.RaftMessage, 0, 1024),
		prioMessages:     make([]*titankvpb.RaftMessage, 0, 1024),
		tickCount:        0,
		ctx:              ctx,
		cancel:           cancel,
		applyCh:          make(chan *ApplyTask, 1024),
		pendingApplyTasks: make([]*ApplyTask, 0, 1024),
	}
	go w.runApplyWorker()
	return w
}

func (w *StoreWorker) Stop() {
	w.cancel()
}

func (w *StoreWorker) Receiver() PeerSender {
	return w.receiver
}

func (w *StoreWorker) refreshRegionFromPD(peer *Peer) *titankvpb.Region {
	if peer == nil || peer.region == nil {
		return nil
	}
	return w.fetchRegionFromPD(peer.region.StartKey)
}

func (w *StoreWorker) fetchRegionFromPD(key []byte) *titankvpb.Region {
	if w.pdClient == nil {
		return nil
	}
	if key == nil {
		key = []byte{}
	}
	ctx, cancel := context.WithTimeout(w.ctx, time.Second)
	defer cancel()
	resp, err := w.pdClient.GetRegion(ctx, &pdpb.GetRegionRequest{Key: key})
	if err != nil || resp == nil || resp.Region == nil {
		if err != nil {
			log.Printf("GetRegion failed: %v", err)
		} else {
			log.Printf("GetRegion returned nil region")
		}
		return nil
	}
	region := &titankvpb.Region{
		Id:       resp.Region.Id,
		StartKey: resp.Region.StartKey,
		EndKey:   resp.Region.EndKey,
	}
	if resp.Region.RegionEpoch != nil {
		region.RegionEpoch = &titankvpb.RegionEpoch{
			ConfVer: resp.Region.RegionEpoch.ConfVer,
			Version: resp.Region.RegionEpoch.Version,
		}
	}
	for _, p := range resp.Region.Peers {
		region.Peers = append(region.Peers, &titankvpb.Peer{
			Id:      p.Id,
			StoreId: p.StoreId,
		})
	}
	return region
}

func (w *StoreWorker) cacheRegionPeers(region *titankvpb.Region) {
	if region == nil {
		return
	}
	for _, p := range region.Peers {
		if p == nil {
			continue
		}
		if p.StoreId != 0 {
			w.peerStoreCache[p.Id] = p.StoreId
		}
	}
}

func (w *StoreWorker) runApplyWorker() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case task := <-w.applyCh:
			w.handleApplyTask(task)
		}
	}
}

func (w *StoreWorker) handleApplyTask(task *ApplyTask) {
	if task == nil {
		return
	}
	peer := task.Peer

	peer.mu.Lock()
	defer peer.mu.Unlock()

	// 使用 Batch Apply 优化写入性能
	newPeers := peer.ApplyCommittedEntries(task.CommittedEntries)

	// 检查 Peer 状态
	if peer.stopped {
		// 如果 Peer 停止了，不需要做更多处理
		// 【新增】检查是否有 Pending Merge (Source Region Merge 后停止)
		if peer.pendingMerge != nil {
			// 通知 Target Region 执行 Merge
			req := peer.pendingMerge
			log.Printf("[Worker] Region %d merged into %d, notifying Target...", peer.regionID, req.TargetRegionId)
			
			// 构造发给 Target 的 Admin Request
			// 注意：这里我们复用 MergeRequest，Target 收到后会发现自己是 TargetRegionId
			cmd := &titankvpb.RaftCommand{
				Header: &titankvpb.RaftRequestHeader{
					RegionId:    req.TargetRegionId,
					RegionEpoch: req.TargetRegionEpoch,
				},
				AdminRequest: &titankvpb.AdminRequest{
					CmdType: titankvpb.AdminRequest_MERGE,
					Merge:   req,
				},
			}
			
			// 发送给 StoreWorker 路由到 Target Peer
			// 注意：这只是 Propose，需要 Target Leader 处理
			w.receiver <- Msg{
				Type:     MsgTypeRaftCmd,
				RegionID: req.TargetRegionId,
				RaftCmd:  cmd,
			}
			// 清除状态
			peer.pendingMerge = nil
		}

		// Notify StoreWorker to remove this peer (Asynchronously to avoid deadlock)
		go func(p *Peer) {
			w.receiver <- NewMsgPeerStopped(p)
		}(peer)

		return
	}

	for _, newPeer := range newPeers {
		if newPeer != nil {
			// 通知 StoreWorker 注册新 Peer
			// 注意：这里是在 ApplyWorker goroutine，不能直接写 w.peers
			// 通过 channel 发送 MsgAddPeer
			// 为了防止死锁 (w.receiver 可能满)，起一个 goroutine
			go func(np *Peer) {
				w.receiver <- Msg{Type: MsgTypeAddPeer, Peer: np}
			}(newPeer)
		}
	}
}

func (w *StoreWorker) Run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	msgs := make([]Msg, 0, batchSize)

	for {
		var applyTask *ApplyTask
		var applyCh chan *ApplyTask
		if len(w.pendingApplyTasks) > 0 {
			applyTask = w.pendingApplyTasks[0]
			applyCh = w.applyCh
		}

		select {
		case msg := <-w.receiver:
			msgs = append(msgs, msg)
		case <-ticker.C:
			w.onTick()
			w.handleReady()
		case applyCh <- applyTask:
			w.pendingApplyTasks = w.pendingApplyTasks[1:]
		case <-w.ctx.Done(): // 【新增】退出信号
			return
		}

		pending := len(w.receiver)
		if pending > 0 {
			if pending > batchSize {
				pending = batchSize
			}
			for i := 0; i < pending; i++ {
				msgs = append(msgs, <-w.receiver)
			}
		}

		if len(msgs) > 0 {
			w.processMsgs(msgs)
			msgs = msgs[:0]
			w.handleReady()
		}
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
		if msg.Type == MsgTypeRaftMessage {
			w.maybeCreatePeer(msg)
		}
		return
	}

	// 【Day 2 新增】处理 SplitCheck
	if msg.Type == MsgTypeSplitCheck {
		w.onSplitCheck(msg.RegionID)
		return
	}
	// 【Day 5 新增】处理 MergeCheck
	if msg.Type == MsgTypeMergeCheck {
		w.onMergeCheck(msg.RegionID)
		return
	}
	if msg.Type == MsgTypeRaftCmdBatch {
		peer.lastActiveTick = w.tickCount
		peer.stepBatchCmds(msg.RaftCmds, msg.Callbacks)
		w.pendingPeers[msg.RegionID] = peer
		return
	}

	// 处理其他消息 (RaftMessage, RaftCmd, Tick)
	peer.step(msg)

	// 标记为活跃，以便后续 handleReady 处理 IO
	w.pendingPeers[msg.RegionID] = peer
}

func (w *StoreWorker) processMsgs(msgs []Msg) {
	batched := make(map[uint64][]Msg)

	flush := func(regionID uint64) {
		cmds := batched[regionID]
		if len(cmds) == 0 {
			return
		}
		peer, ok := w.peers[regionID]
		if !ok || peer == nil {
			return
		}

		// Update activity timestamp for Merge idle check
		peer.lastActiveTick = w.tickCount

		if len(cmds) == 1 {
			peer.step(cmds[0])
		} else {
			peer.stepBatch(cmds)
		}
		w.pendingPeers[regionID] = peer
		batched[regionID] = batched[regionID][:0]
	}

	for _, msg := range msgs {
		if msg.Type == MsgTypeTick {
			w.onTick()
			continue
		}
		if msg.Type == MsgTypeSplitCheck {
			w.onSplitCheck(msg.RegionID)
			continue
		}
		if msg.Type == MsgTypeAddPeer {
			if msg.Peer != nil {
				w.registerPeer(msg.Peer)
			}
			continue
		}
		if msg.Type == MsgTypePeerStopped {
			if msg.Peer != nil {
				w.removePeer(msg.Peer)
			}
			continue
		}

		if msg.Type == MsgTypeRaftCmd {
			batched[msg.RegionID] = append(batched[msg.RegionID], msg)
			continue
		}
		// 【新增】ReadIndex Batching
		if msg.Type == MsgTypeReadIndex {
			batched[msg.RegionID] = append(batched[msg.RegionID], msg)
			continue
		}
		flush(msg.RegionID)
		w.processMsg(msg)
	}

	for regionID := range batched {
		flush(regionID)
	}
}

func (w *StoreWorker) onTick() {
	w.tickCount++
	bucket := int(w.tickCount % TickBucketCount)
	for _, peer := range w.tickBuckets[bucket] {
		if peer.stopped {
			continue
		}
		// 1. Raft Tick
		for i := 0; i < TickBucketCount; i++ {
			peer.step(NewMsgTick())
		}
		w.pendingPeers[peer.regionID] = peer

		// 2. 【新增】PD Heartbeat
		w.heartbeatCounter[peer.regionID]++
		if w.heartbeatCounter[peer.regionID]%PDHeartbeatTickInterval == 0 {
			w.sendRegionHeartbeat(peer)
		}
	}
	// 3. Split Check (低频执行)
	if w.tickCount%SplitCheckInterval == 0 {
		w.checkSplit()
		w.checkMerge() // 【新增】检查 Merge
	}
}

func (w *StoreWorker) handleReady() {
	// Reuse messages buffer
	w.messages = w.messages[:0]
	w.prioMessages = w.prioMessages[:0]
	type readyData struct {
		peer *Peer
		rd   raft.Ready
	}
	var readies []readyData

	// Clear buffers
	w.batchKeys = w.batchKeys[:0]
	w.batchValues = w.batchValues[:0]

	// 1. 预处理阶段：收集 Ready，处理 Snapshot/WAL
	for _, peer := range w.pendingPeers {
		if peer == nil {
			continue
		}
		if !peer.hasReady() {
			continue
		}

		rd := peer.raftGroup.Ready()
		readies = append(readies, readyData{peer: peer, rd: rd})

		if len(rd.ReadStates) > 0 {
			peer.handleReadStates(rd.ReadStates)
		}

		// A. 收集日志和状态 (WAL)
		// peer.storage.Append 返回 []kvPair
		// 【新增】处理 Snapshot
		if !raft.IsEmptySnap(rd.Snapshot) {
			filePath := ""
			var snapData SnapshotData
			if err := json.Unmarshal(rd.Snapshot.Data, &snapData); err == nil {
				if snapData.FilePath != "" {
					filePath = snapData.FilePath
				}
				if snapData.Region != nil {
					peer.region = snapData.Region
					peer.storage.region = snapData.Region
				}
			}
			if filePath == "" {
				filePath = string(rd.Snapshot.Data)
			}
			if filePath != "" {
				peer.applySnapshot(filePath)
			}
			peer.storage.ApplySnapshot(rd.Snapshot)
			if peer.region != nil {
				log.Printf("[Worker] Persisting RegionState for Peer %d Region %d", peer.peerID, peer.region.Id)
				writeRegionStateAtomic(peer.storage.engine, peer.region, &peer.storage.raftState)
			} else {
				log.Printf("[Worker] Peer %d region is nil, skipping persistence", peer.peerID)
			}
		}

		// 使用 AppendBatch 直接写入 buffer
		if err := peer.storage.AppendBatch(&w.batchKeys, &w.batchValues, rd.Entries, &rd.HardState); err != nil {
			log.Fatalf("AppendBatch failed: %v", err)
		}

		// 注意：这里不再收集网络消息，推迟到 Apply 之后
	}

	// 2. 执行阶段：批量写盘 (Atomic & Batch)
	if len(w.batchKeys) > 0 {
		//start := time.Now()
		// 调用 CGO BatchPut
		err := w.store.BatchPut(w.batchKeys, w.batchValues)
		if err != nil {
			log.Fatalf("BatchPut failed: %v", err)
		}
		//log.Printf("[Worker] Batch Write done. Items=%d, Cost=%v", len(batchKeys), time.Since(start))
	}

	// 3. 发送消息阶段：在 Apply 之前发送，减少延迟
	for _, item := range readies {
		peer := item.peer
		rd := item.rd
		if peer == nil || peer.stopped {
			continue
		}

		// B. 收集网络消息
		for _, msg := range rd.Messages {
			// 查找目标 Peer 的 StoreID
			toStoreId := uint64(0)
			for _, p := range peer.region.Peers {
				if p.Id == msg.To {
					toStoreId = p.StoreId
					break
				}
			}

			if toStoreId == 0 {
				// 尝试刷新
				refreshed := w.refreshRegionFromPD(peer)
				if refreshed != nil {
					for _, p := range refreshed.Peers {
						if p.Id == msg.To {
							toStoreId = p.StoreId
							break
						}
					}
					if toStoreId != 0 {
						w.peerStoreCache[msg.To] = toStoreId
					}
				}
			}
			if toStoreId == 0 {
				if cached, ok := w.peerStoreCache[msg.To]; ok {
					toStoreId = cached
				}
			}
			if toStoreId == 0 {
				continue
			}

			if msg.Type == raftpb.MsgSnap {
				snapBytes, err := msg.Snapshot.Marshal()
				if err != nil {
					log.Printf("Failed to marshal snapshot: %v", err)
					continue
				}
				tm := AcquireRaftMessage()
				tm.RegionId = peer.regionID
				tm.FromPeerId = msg.From
				tm.ToPeerId = msg.To
				tm.ToStoreId = toStoreId
				tm.Data = snapBytes
				
				if err := w.transport.SendSnapshot(tm); err != nil {
					log.Printf("SendSnapshot failed: %v", err)
				}
				ReleaseRaftMessage(tm)
				continue
			}

			data, _ := msg.Marshal()
			tm := AcquireRaftMessage()
			tm.RegionId = peer.regionID
			tm.FromPeerId = msg.From
			tm.ToPeerId = msg.To
			tm.ToStoreId = toStoreId
			tm.Data = data

			if msg.Type == raftpb.MsgHeartbeat || msg.Type == raftpb.MsgHeartbeatResp ||
				msg.Type == raftpb.MsgVote || msg.Type == raftpb.MsgVoteResp ||
				msg.Type == raftpb.MsgPreVote || msg.Type == raftpb.MsgPreVoteResp {
				w.prioMessages = append(w.prioMessages, tm)
			} else {
				w.messages = append(w.messages, tm)
			}
		}
	}

	// 4. 执行阶段：批量发送
	if len(w.messages) > 0 {
		w.transport.Send(w.messages)
	}
	if len(w.prioMessages) > 0 {
		w.transport.SendPrioritized(w.prioMessages)
	}

	// 5. 后处理阶段：Apply & Advance
	for _, item := range readies {
		peer := item.peer
		rd := item.rd

		if peer == nil {
			continue
		}
		w.cacheRegionPeers(peer.GetRegion())

		// Async Apply
		if len(rd.CommittedEntries) > 0 {
			// 将 CommittedEntries 拷贝一份，因为 rd.CommittedEntries 可能被复用
			entries := make([]raftpb.Entry, len(rd.CommittedEntries))
			copy(entries, rd.CommittedEntries)
			
			task := &ApplyTask{
				RegionID:         peer.regionID,
				Peer:             peer,
				CommittedEntries: entries,
				Snapshot:         rd.Snapshot,
			}
			// 发送到 Apply Buffer
			w.pendingApplyTasks = append(w.pendingApplyTasks, task)
		}

		// 【新增】处理 ReadStates (ReadIndex)
		if len(rd.ReadStates) > 0 {
			peer.handleReadStates(rd.ReadStates)
		}

		// Check Stopped (RemovePeer 在 handleReady 前可能已经标记 stopped?)
		// 这里主要检查 snapshot 可能导致的 stopped
		if peer.stopped {
			w.removePeer(peer)
			continue
		}

		// Advance
		// 注意：Async Apply 模式下，我们立即 Advance，允许 Raft 继续
		// 前提是 Log 已经持久化 (step 2 done)
		peer.raftGroup.Advance(rd)
	}

	w.pendingPeers = make(map[uint64]*Peer)
}

func (w *StoreWorker) removePeer(p *Peer) {
	delete(w.peers, p.regionID)
	delete(w.pendingPeers, p.regionID)
	// 【修复】tickBuckets 删除逻辑可能有点问题，因为 bucket 是 slice
	// 正确做法：delete map in bucket
	bucketID := w.bucketForRegion(p.regionID)
	if bucketID < len(w.tickBuckets) {
		delete(w.tickBuckets[bucketID], p.regionID)
	}
	
	w.router.Unregister(p.regionID)

	// 【新增】处理 Merge 数据迁移
	if p.pendingMerge != nil {
		targetID := p.pendingMerge.TargetRegionId
		// 检查 Target Region 是否在本地
		if targetPeer, ok := w.peers[targetID]; ok {
			log.Printf("[Merge] Migrating data from Source %d to Local Target %d...", p.regionID, targetID)
			
			// 1. 迁移数据 (通过 MergeRequest 发送给 Target)
			data, err := w.collectData(p)
			if err != nil {
				log.Printf("[Merge] Failed to collect data: %v", err)
			}
			
			// 2. 向 Target Region 发起 "Commit Merge" (扩展 Range + 写入数据)
			// 注意：这里需要 Target Leader 处理。如果本地 Target Peer 不是 Leader，
			// Propose 会转发给 Leader。
			// 构造 MergeRequest (Target Side)
			// 我们复用 MergeRequest，Target 收到后会发现 TargetRegionId == SelfID
			req := p.pendingMerge // 已经是 MergeRequest
			req.Data = data       // 附带数据
			
			cmd := &titankvpb.RaftCommand{
				Type: titankvpb.RaftCommand_ADMIN,
				AdminRequest: &titankvpb.AdminRequest{
					CmdType: titankvpb.AdminRequest_MERGE,
					Merge:   req,
				},
			}
			
			cmdBytes, _ := proto.Marshal(cmd)
			// 必须通过 Raft Propose，不能直接修改状态
			targetPeer.raftGroup.Propose(cmdBytes)
			log.Printf("[Merge] Proposed CommitMerge to Target Region %d", targetID)
		} else {
			log.Printf("[Merge] Target Region %d not found locally. Data for Source %d will be deleted (Assumed replicated elsewhere).", targetID, p.regionID)
			// 如果 Target 不在本地，说明此 Store 不需要保留该范围的数据
			// (或者需要发送 Snapshot 给 Target，这里简化为直接删除)
		}
	}

	tombstone := &raft_serverpb.RegionLocalState{
		State:  raft_serverpb.PeerState_Tombstone,
		Region: p.region,
	}
	regionVal, _ := proto.Marshal(tombstone)
	raftVal, _ := proto.Marshal(&p.storage.raftState)
	if err := w.store.BatchPut(
		[][]byte{RegionStateKey(p.regionID), RaftStateKey(p.regionID)},
		[][]byte{regionVal, raftVal},
	); err != nil {
		log.Printf("Failed to persist tombstone state for region %d: %v", p.regionID, err)
	}

	// 3. 异步执行物理清理 (避免阻塞 Worker 主循环)
	// 我们需要清理两部分数据：
	// A. Data (z{RegionID}...)
	// B. Raft Log (r{RegionID}...)
	// 注意：保留 RegionState (Tombstone) 以防旧消息导致 Peer 重启

	// 捕获需要的变量
	store := w.store
	regionID := p.regionID
	
	// 启动后台清理
	go w.deletePeerData(store, regionID)

	//log.Printf("Peer %d removed from Region %d (scheduled for GC)", p.peerID, p.regionID)
}

func (w *StoreWorker) deletePeerData(store *store.TitanStore, regionID uint64) {
	//log.Printf("[GC] Clearing data for removed Region %d...", regionID)

	// --- 清理 Data (z{RegionID}...) ---
	// 构造物理范围
	// DataKey 前缀: 'z' + RegionID
	dataStart := DataKey(regionID, nil)
	// 下一个 Region 的 Start
	dataEnd := DataKey(regionID+1, nil)

	if err := store.DeleteRange(dataStart, dataEnd); err != nil {
		log.Printf("[GC] Failed to delete data range for region %d: %v", regionID, err)
	}

	// --- 清理 Raft Log (r{RegionID}...) ---
	// RaftLogKey 格式: r{RegionID}{0x01}{Index}
	// RaftStateKey 格式: r{RegionID}{0x02}
	// 我们删除 [LogStart, RaftStateKey) 范围，即删除所有 Log，保留 State (State 会被后续 BatchPut 更新为 Tombstone 相关，或者被删除)
	// 等等，我们在 removePeer 中已经更新了 RaftStateKey (持久化 Tombstone 时也写了 RaftState)。
	// 如果这里删除 RaftStateKey，可能会丢数据。
	// 但 Raft Log 是可以删的。
	
	logStart := RaftLogKey(regionID, 0)
	// Log 结束位置可以用 RaftStateKey 作为边界 (因为 0x01 < 0x02)
	logEnd := RaftStateKey(regionID)
	
	if err := store.DeleteRange(logStart, logEnd); err != nil {
		log.Printf("[GC] Failed to delete raft logs for region %d: %v", regionID, err)
	}
	
	// ApplyStateKey (r{RegionID}{0x03}) 也可以删
	if err := store.Delete(ApplyStateKey(regionID)); err != nil {
		// Ignore not found
		if err.Error() != "key not found" {
			log.Printf("[GC] Failed to delete ApplyState for region %d: %v", regionID, err)
		}
	}

	//log.Printf("[GC] Region %d cleanup finished.", regionID)
}

func (w *StoreWorker) AddPeer(p *Peer) {
	w.peers[p.regionID] = p
	w.pendingPeers[p.regionID] = p
	w.tickBuckets[w.bucketForRegion(p.regionID)][p.regionID] = p
	w.sendRegionHeartbeat(p)
}

func (w *StoreWorker) registerPeer(p *Peer) {
	w.peers[p.regionID] = p
	w.router.Register(p.regionID, w.receiver, p) // 注册路由
	w.tickBuckets[w.bucketForRegion(p.regionID)][p.regionID] = p
	w.sendRegionHeartbeat(p)

	// 如果新 Peer 也是本 Worker 管理，需要启动心跳吗？
	// onTick 会遍历 w.peers，所以自动生效
	log.Printf("Registered new peer for Region %d", p.regionID)
}

func (w *StoreWorker) collectData(sourcePeer *Peer) ([]*titankvpb.KeyValue, error) {
	// 扫描 Source Region 的所有数据
	start := DataKey(sourcePeer.regionID, sourcePeer.region.StartKey)
	var end []byte
	if len(sourcePeer.region.EndKey) > 0 {
		end = DataKey(sourcePeer.regionID, sourcePeer.region.EndKey)
	} else {
		end = DataKey(sourcePeer.regionID+1, nil)
	}

	// 使用 Iterator 扫描
	iter := w.store.NewIterator(start, end)
	defer iter.Close()

	var data []*titankvpb.KeyValue
	count := 0
	
	// Max Raft Msg Size: 2MB (Safe limit for Raft log replication)
	// Larger regions should use Snapshot-based Merge (future optimization)
	maxSize := 2 * 1024 * 1024
	currentSize := 0

	for iter.Seek(start); iter.Valid(); iter.Next() {
		// 1. 获取原始 Key (z{SourceID}{UserKey})
		srcKey := iter.Key()
		// Debug logging for key issue
		// log.Printf("[Merge] srcKey: %x (len: %d)", srcKey, len(srcKey))

		// Check bounds (NewIterator doesn't enforce end yet)
		if len(end) > 0 && bytes.Compare(srcKey, end) >= 0 {
			break
		}

		val := iter.Value()

		// 2. 解码出 UserKey
		// DataKey 格式: 'z' + RegionID(8B) + UserKey
		// 跳过前缀 'z' (1B) + RegionID (8B) = 9B
		if len(srcKey) < 9 {
			continue
		}
		userKey := srcKey[9:]

		// 3. 构造 KeyValue (UserKey)
		// 需要 Copy，因为 iter.Key/Value 在 Next 后失效
		k := make([]byte, len(userKey))
		copy(k, userKey)
		v := make([]byte, len(val))
		copy(v, val)
		
		kv := &titankvpb.KeyValue{
			Key:   k,
			Value: v,
		}

		data = append(data, kv)
		count++
		
		currentSize += len(k) + len(v) + 16 // Approximate overhead
		if currentSize >= maxSize {
			return nil, fmt.Errorf("source region %d data size %d exceeds limit %d, abort merge", sourcePeer.regionID, currentSize, maxSize)
		}
	}

	log.Printf("[Merge] Collected %d keys (%d bytes) from Region %d", count, currentSize, sourcePeer.regionID)
	return data, nil
}

func (w *StoreWorker) bucketForRegion(regionID uint64) int {
	return int(regionID % uint64(TickBucketCount))
}

func (w *StoreWorker) checkSplit() {
	for _, peer := range w.peers {
		if peer.raftGroup.Status().Lead != peer.peerID {
			continue
		}

		// 【修复】去掉 raftstore. 前缀，直接调用 DataKey
		start := DataKey(peer.regionID, peer.region.StartKey)

		var end []byte
		if len(peer.region.EndKey) > 0 {
			end = DataKey(peer.regionID, peer.region.EndKey)
		} else {
			end = DataKey(peer.regionID+1, nil)
		}

		// 【修复】构造成切片 [][]byte
		sizes := w.store.GetApproximateSizes([][]byte{start}, [][]byte{end})
		if len(sizes) == 0 {
			continue
		}

		size := sizes[0]

		if size >= MaxRegionSize {
			log.Printf("Region %d size %d exceeds threshold, triggering split...", peer.regionID, size)
			// 【修复】去掉 raftstore. 前缀
			w.processMsg(NewMsgSplitCheck(peer.regionID))
		}
	}
}

func (w *StoreWorker) onSplitCheck(regionID uint64) {
	peer, ok := w.peers[regionID]
	if !ok {
		return
	}

	// 【修改】动态计算 Split Key (字典序中间值)
	// 这是一个简化实现，假设 Key 是 "key-00000" 格式
	// 生产环境应调用 C++ Engine 的 ApproximateMiddle

	// 简单粗暴：直接取 start 往后偏移一点，或者硬编码一个更合理的中间值
	// 在本次压测中 (2000 个 Key)，中间大概是 key-01000
	// 为了确保 SplitKey 落在 Range 内，我们取 "key-01000"
	splitKey := []byte("key-01000")

	// 校验一下是否在范围内，如果不在，可能不需要分裂或者逻辑有误
	if !peer.isKeyInRange(splitKey) {
		log.Printf("Calculated split key %s out of range, skip", splitKey)
		return
	}

	// 2. 向 PD 申请新 ID
	// 新 Region 的 ID
	newRegionID, err := w.askPDAllocID()
	if err != nil {
		return
	}

	// 新 Region 对应的 Peers 的 ID (每个副本都需要一个新 ID)
	var newPeers []*titankvpb.Peer
	for _, p := range peer.region.Peers {
		pid, _ := w.askPDAllocID()
		newPeers = append(newPeers, &titankvpb.Peer{
			Id:      pid,
			StoreId: p.StoreId, // 绑定 StoreID
		})
	}

	// 3. 构造 Admin Request
	adminReq := &titankvpb.AdminRequest{
		CmdType: titankvpb.AdminRequest_SPLIT,
		Split: &titankvpb.SplitRequest{
			SplitKey:    splitKey,
			NewRegionId: newRegionID,
			NewPeers:    newPeers,
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

func (w *StoreWorker) checkMerge() {
	for _, peer := range w.peers {
		if peer.raftGroup.Status().Lead != peer.peerID {
			continue
		}

		// Check Idle (e.g. 600 ticks = 1 minute if tick is 100ms)
		// We only merge idle regions to avoid impacting active traffic
		if w.tickCount > peer.lastActiveTick && (w.tickCount-peer.lastActiveTick) < 600 {
			continue
		}

		// 检查 Region 大小
		start := DataKey(peer.regionID, peer.region.StartKey)
		var end []byte
		if len(peer.region.EndKey) > 0 {
			end = DataKey(peer.regionID, peer.region.EndKey)
		} else {
			end = DataKey(peer.regionID+1, nil)
		}

		sizes := w.store.GetApproximateSizes([][]byte{start}, [][]byte{end})
		if len(sizes) == 0 {
			continue
		}
		size := sizes[0]

		// 阈值：假设小于 1MB 且 key 数量少 (这里只看 size)
		if size < 1*1024*1024 { // 1MB
			// 避免频繁检查，可以使用随机或计数器
			w.processMsg(NewMsgMergeCheck(peer.regionID))
		}
	}
}

func (w *StoreWorker) onMergeCheck(regionID uint64) {
	peer, ok := w.peers[regionID]
	if !ok {
		return
	}

	// 1. 寻找相邻 Region (Target)
	// 策略：优先合并到后一个 Region
	if len(peer.region.EndKey) == 0 {
		// 最后一个 Region，尝试合并到前一个 (暂不实现)
		return
	}

	// 从 PD 获取后一个 Region
	// EndKey 就是下一个 Region 的 StartKey
	targetRegion := w.fetchRegionFromPD(peer.region.EndKey)
	if targetRegion == nil {
		return
	}

	// 校验连续性
	if bytes.Compare(targetRegion.StartKey, peer.region.EndKey) != 0 {
		return
	}

	// 2. 发起 Merge 请求 (Propose to Source)
	req := &titankvpb.MergeRequest{
		TargetRegionId:    targetRegion.Id,
		TargetRegionEpoch: targetRegion.RegionEpoch,
		SourceRegion:      peer.region,
	}

	cmd := &titankvpb.RaftCommand{
		Type: titankvpb.RaftCommand_ADMIN,
		AdminRequest: &titankvpb.AdminRequest{
			CmdType: titankvpb.AdminRequest_MERGE,
			Merge:   req,
		},
	}

	data, _ := proto.Marshal(cmd)
	peer.raftGroup.Propose(data)
	log.Printf("[Merge] Proposing merge Region %d into %d", regionID, targetRegion.Id)
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

func (w *StoreWorker) sendRegionHeartbeat(p *Peer) {
	// 1. 转换 Region (titankvpb -> pdpb)
	// 这一步必须深拷贝所有字段
	region := &pdpb.Region{
		Id:       p.region.Id,
		StartKey: p.region.StartKey,
		EndKey:   p.region.EndKey,
	}
	if p.region.RegionEpoch != nil {
		region.RegionEpoch = &pdpb.RegionEpoch{
			ConfVer: p.region.RegionEpoch.ConfVer,
			Version: p.region.RegionEpoch.Version,
		}
	}
	for _, peer := range p.region.Peers {
		region.Peers = append(region.Peers, &pdpb.Peer{
			Id:      peer.Id,
			StoreId: peer.StoreId,
		})
	}

	// 2. 转换 Leader Peer (titankvpb -> pdpb)
	var leaderPeer *pdpb.Peer
	leadID := p.raftGroup.Status().Lead
	if leadID != 0 {
		for _, peer := range region.Peers {
			if peer.Id == leadID {
				leaderPeer = &pdpb.Peer{
					Id:      peer.Id,
					StoreId: peer.StoreId,
				}
				break
			}
		}
	}

	approxSize := uint64(100)
	approxKeys := uint64(10000)

	go func() {
		req := &pdpb.RegionHeartbeatRequest{
			Region:          region,
			Leader:          leaderPeer,
			ApproximateSize: approxSize,
			ApproximateKeys: approxKeys,
		}

		ctx, cancel := context.WithTimeout(w.ctx, 3*time.Second)
		defer cancel()

		resp, err := w.pdClient.RegionHeartbeat(ctx, req)
		if err != nil {
			log.Printf("Failed to send region heartbeat: %v", err)
			return
		}
		// 【调试点】打印接收到的 Response
		// log.Printf("[DEBUG] Recv Heartbeat Resp: %v", resp)

		if resp.TransferLeader != nil {
			log.Printf("[Schedule] Received TransferLeader to peer %d", resp.TransferLeader.PeerId)
			// 简单实现：直接调用 RawNode
			p.raftGroup.TransferLeader(resp.TransferLeader.PeerId)
		} else if resp.ChangePeer != nil {
			cp := resp.ChangePeer

			// 3. 转换 ChangePeer (pdpb -> titankvpb)
			tkChangeType := titankvpb.ChangePeer_ADD_NODE
			if cp.ChangeType == pdpb.ChangePeer_REMOVE_NODE {
				tkChangeType = titankvpb.ChangePeer_REMOVE_NODE
			}

			tkPeer := &titankvpb.Peer{
				Id:      cp.Peer.Id,
				StoreId: cp.Peer.StoreId,
			}
			if tkChangeType == titankvpb.ChangePeer_REMOVE_NODE {
				delete(w.peerStoreCache, tkPeer.Id)
			} else if tkPeer.StoreId != 0 {
				w.peerStoreCache[tkPeer.Id] = tkPeer.StoreId
			}

			adminReq := &titankvpb.AdminRequest{
				CmdType: titankvpb.AdminRequest_CONF_CHANGE,
				ChangePeer: &titankvpb.ChangePeer{
					ChangeType: tkChangeType, // 使用转换后的类型
					Peer:       tkPeer,       // 使用转换后的 Peer
				},
			}

			cmd := &titankvpb.RaftCommand{
				Type:         titankvpb.RaftCommand_ADMIN,
				AdminRequest: adminReq,
			}

			// NewMsgRaftCmd 需要传入 regionID
			w.router.Send(p.regionID, NewMsgRaftCmd(p.regionID, cmd, nil))
		}
	}()
}

func (w *StoreWorker) maybeCreatePeer(msg Msg) {
	if msg.Type != MsgTypeRaftMessage {
		return
	}

	var rMsg raftpb.Message
	if err := rMsg.Unmarshal(msg.RaftMessage.Data); err != nil {
		return
	}

	// 只有 Snapshot 消息携带了足够的信息来创建 Peer
	log.Printf("[StoreWorker] maybeCreatePeer called for region %d from %d", msg.RegionID, msg.RaftMessage.FromPeerId)

	if rMsg.Type == raftpb.MsgSnap {
		log.Printf("[StoreWorker] Received Snapshot for unknown region %d. Creating Peer...", msg.RegionID)

		var snapData SnapshotData
		if err := json.Unmarshal(rMsg.Snapshot.Data, &snapData); err != nil {
			log.Printf("Failed to unmarshal snapshot data: %v", err)
			return
		}

		// 1. 获取当前 Store 对应的 PeerID
		// 假设 NewPeer 内部能处理，或者我们需要在这里找到 PeerID
		// NewPeer(storeID, region, engine)
		// StoreID 如何获取？我们在 NewStoreWorker 里没有存 StoreID。
		// 假设我们通过 msg.ToPeerId 反查？
		// 最好 StoreWorker 持有 StoreID。
		// Hack: 遍历 snapData.Region.Peers，找到 msg.ToPeerId 对应的 StoreId
		var myStoreID uint64
		for _, p := range snapData.Region.Peers {
			if p.Id == msg.RaftMessage.ToPeerId {
				myStoreID = p.StoreId
				break
			}
		}

		if myStoreID == 0 {
			log.Printf("Cannot find my store ID for peer %d", msg.RaftMessage.ToPeerId)
			return
		}

		// 2. 创建 Peer
		newPeer, err := NewPeer(myStoreID, snapData.Region, w.store)
		if err != nil {
			log.Printf("Failed to create peer: %v", err)
			return
		}

		// 3. 注册 Peer
		w.registerPeer(newPeer)

		newPeer.step(msg)
		w.pendingPeers[msg.RegionID] = newPeer

		// 4. 将消息转交给新 Peer 处理 (Peer 会应用 Snapshot)
		newPeer.step(msg)
	} else {
		if region := w.fetchRegionFromPD([]byte{}); region != nil {
			var myStoreID uint64
			for _, p := range region.Peers {
				if p.Id == msg.RaftMessage.ToPeerId {
					myStoreID = p.StoreId
					break
				}
			}
			if myStoreID != 0 {
				newPeer, err := NewPeer(myStoreID, region, w.store)
				if err == nil {
					w.registerPeer(newPeer)
					writeRegionStateAtomic(newPeer.storage.engine, newPeer.region, &newPeer.storage.raftState)
					newPeer.step(msg)
					w.pendingPeers[msg.RegionID] = newPeer
					return
				}
				log.Printf("Failed to create peer: %v", err)
			}
		}
		// 对于非 Snapshot 消息（如 Vote, Heartbeat），
		// 如果我们没有 Peer，说明我们落后了或者是被误发了。
		// 我们可以回复 GroupMissing 或者忽略。
		// 为了触发 Leader 发送 Snapshot，最好的办法是回复一个 "Index=0" 的 Reject。
		// 这里构造回复并确保正确的 ToStoreId 以便能发送出去。
		log.Printf("Ignored non-snapshot msg type %s for unknown region %d from %d. Term: %d, Index: %d", rMsg.Type, msg.RegionID, rMsg.From, rMsg.Term, rMsg.Index)

		if rMsg.Type == raftpb.MsgHeartbeat {
			// 回复心跳，促使 Leader 继续发送日志
			log.Printf("[StoreWorker] Replying HeartbeatResp to trigger Append")
			resp := raftpb.Message{
				Type: raftpb.MsgHeartbeatResp,
				To:   rMsg.From,
				From: rMsg.To,
				Term: rMsg.Term,
			}
			data, err := resp.Marshal()
			if err != nil {
				log.Printf("Failed to marshal heartbeat resp: %v", err)
				return
			}
			var toStoreID uint64
			if w.pdClient != nil {
				ctx, cancel := context.WithTimeout(w.ctx, time.Second)
				pdResp, err := w.pdClient.GetRegion(ctx, &pdpb.GetRegionRequest{Key: []byte{}})
				cancel()
				if err == nil && pdResp != nil && pdResp.Region != nil {
					for _, p := range pdResp.Region.Peers {
						if p.Id == rMsg.From {
							toStoreID = p.StoreId
							break
						}
					}
				}
			}
			if toStoreID == 0 {
				log.Printf("HeartbeatResp cannot resolve leader store for peer %d; skip sending", rMsg.From)
				return
			}
			raftMsg := &titankvpb.RaftMessage{
				RegionId:   msg.RegionID,
				FromPeerId: msg.RaftMessage.ToPeerId,
				ToPeerId:   msg.RaftMessage.FromPeerId,
				ToStoreId:  toStoreID,
				Data:       data,
			}
			w.transport.Send([]*titankvpb.RaftMessage{raftMsg})
		} else if rMsg.Type == raftpb.MsgApp {
			log.Printf("[StoreWorker] Replying with Reject to MsgApp to trigger Snapshot")
			// Construct Reject Response
			resp := raftpb.Message{
				Type:       raftpb.MsgAppResp,
				To:         rMsg.From,
				From:       rMsg.To,   // We are the recipient
				Term:       rMsg.Term, // Use leader's term
				Index:      0,         // Request from beginning
				Reject:     true,
				RejectHint: 0, // Hint to start from 0
			}

			data, err := resp.Marshal()
			if err != nil {
				log.Printf("Failed to marshal reject response: %v", err)
				return
			}

			// 解析 Leader 的 StoreId，通过 PD 获取 Region 并查找 FromPeerId
			var toStoreID uint64
			if w.pdClient != nil {
				ctx, cancel := context.WithTimeout(w.ctx, time.Second)
				pdResp, err := w.pdClient.GetRegion(ctx, &pdpb.GetRegionRequest{Key: []byte{}})
				cancel()
				if err != nil || pdResp == nil || pdResp.Region == nil {
					if err != nil {
						log.Printf("Resolve ToStoreId via PD failed: %v", err)
					} else {
						log.Printf("Resolve ToStoreId via PD failed: nil region")
					}
				} else {
					for _, p := range pdResp.Region.Peers {
						if p.Id == rMsg.From {
							toStoreID = p.StoreId
							break
						}
					}
				}
			}
			if toStoreID == 0 {
				log.Printf("Reject response cannot resolve leader store for peer %d; skip sending", rMsg.From)
				return
			}

			raftMsg := &titankvpb.RaftMessage{
				RegionId:   msg.RegionID,
				FromPeerId: msg.RaftMessage.ToPeerId, // We are "To" in the original message
				ToPeerId:   msg.RaftMessage.FromPeerId,
				ToStoreId:  toStoreID,
				Data:       data,
				// RegionEpoch: We don't have it, omit
			}

			w.transport.Send([]*titankvpb.RaftMessage{raftMsg})
		}
	}
}
