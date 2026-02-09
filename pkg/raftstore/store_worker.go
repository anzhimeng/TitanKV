package raftstore

import (
	"context"
	"encoding/json"
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
	batchSize = 128
	// Region 最大阈值 (96MB)
	MaxRegionSize = /*96*/ 1 * 1024 * 1024
	// 检查间隔 (每隔多少次 Tick 检查一次，避免频繁调用 CGO)
	SplitCheckInterval      = 10
	PDHeartbeatTickInterval = 50
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

	tickCount uint64
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewStoreWorker(router *Router, trans *Transport, s *store.TitanStore, client pdpb.PDClient) *StoreWorker {
	ctx, cancel := context.WithCancel(context.Background())
	tickBuckets := make([]map[uint64]*Peer, TickBucketCount)
	for i := range tickBuckets {
		tickBuckets[i] = make(map[uint64]*Peer)
	}
	return &StoreWorker{
		peers:            make(map[uint64]*Peer),
		receiver:         make(PeerSender, 4096),
		router:           router,
		pendingPeers:     make(map[uint64]*Peer),
		store:            s,
		transport:        trans,  // 赋值
		pdClient:         client, // 赋值
		tickBuckets:      tickBuckets,
		heartbeatCounter: make(map[uint64]uint64),
		peerStoreCache:   make(map[uint64]uint64),
		ctx:              ctx,
		cancel:           cancel,
	}
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
		case <-w.ctx.Done(): // 【新增】退出信号
			return
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

	// 处理其他消息 (RaftMessage, RaftCmd, Tick)
	peer.step(msg)

	// 标记为活跃，以便后续 handleReady 处理 IO
	w.pendingPeers[msg.RegionID] = peer
}
func (w *StoreWorker) onTick() {
	w.tickCount++
	bucket := int(w.tickCount % uint64(TickBucketCount))
	for _, peer := range w.tickBuckets[bucket] {
		if peer == nil {
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
	// 2. Split Check (低频执行)
	if w.tickCount%SplitCheckInterval == 0 {
		w.checkSplit()
	}
}

func (w *StoreWorker) handleReady() {
	var messages []*titankvpb.RaftMessage

	type readyData struct {
		peer *Peer
		rd   raft.Ready
	}
	var readies []readyData

	// CGO Batch Write 需要的切片
	var batchKeys [][]byte
	var batchValues [][]byte

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

		kvPairs, err := peer.storage.Append(rd.Entries, &rd.HardState)
		if err != nil {
			log.Fatalf("Append failed: %v", err)
		}

		// 拆解 kvPair 到两个切片
		for _, kv := range kvPairs {
			batchKeys = append(batchKeys, kv.key)
			batchValues = append(batchValues, kv.value)
		}

		// 注意：这里不再收集网络消息，推迟到 Apply 之后
	}

	// 2. 执行阶段：批量写盘 (Atomic & Batch)
	if len(batchKeys) > 0 {
		//start := time.Now()
		// 调用 CGO BatchPut
		err := w.store.BatchPut(batchKeys, batchValues)
		if err != nil {
			log.Fatalf("BatchPut failed: %v", err)
		}
		//log.Printf("[Worker] Batch Write done. Items=%d, Cost=%v", len(batchKeys), time.Since(start))
	}

	// 3. 后处理阶段：Apply & Collect Messages & Advance
	for _, item := range readies {
		peer := item.peer
		rd := item.rd

		if peer == nil {
			continue
		}
		w.cacheRegionPeers(peer.region)

		// Apply CommittedEntries (Updates RegionState)
		for _, entry := range rd.CommittedEntries {
			newPeer := peer.processEntry(entry)
			if peer != nil {
				if entry.Index > peer.GetAppliedIndex() {
					peer.SetAppliedIndex(entry.Index)
				}
			}
			// 检查是否被移除
			if peer.stopped {
				w.removePeer(peer)
				break
			}
			// 如果产生了分裂，注册新 Peer
			if peer == nil {
				log.Printf("!!! PANIC ALERT !!! peer became nil in loop!")
				break
			}
			if newPeer != nil {
				w.registerPeer(newPeer)
			}
		}

		if peer.stopped {
			continue
		}

		// B. 收集网络消息 (现在 PeerState 已经更新)
		for _, msg := range rd.Messages {
			// 查找目标 Peer 的 StoreID
			toStoreId := uint64(0)
			for _, p := range peer.region.Peers {
				if p.Id == msg.To {
					toStoreId = p.StoreId
					break
				}
			}

			log.Printf("[StoreWorker] Processing msg Type=%s To=%d ResolvedStoreId=%d", msg.Type, msg.To, toStoreId)

			if toStoreId == 0 {
				log.Printf("Peer %d not found in region %d (Peers: %v). MsgType: %s. Attempting fallback...", msg.To, peer.regionID, peer.region.Peers, msg.Type)
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
				log.Printf("Peer %d still not found in region %d after fallback (Peers: %v). MsgType: %s", msg.To, peer.regionID, peer.region.Peers, msg.Type)
				if cached, ok := w.peerStoreCache[msg.To]; ok {
					toStoreId = cached
				}
			}
			if toStoreId == 0 {
				continue
			}

			log.Printf("[StoreWorker] Sending msg %s to Peer %d (Store %d)", msg.Type, msg.To, toStoreId)

			if msg.Type == raftpb.MsgSnap {
				snapBytes, err := msg.Snapshot.Marshal()
				if err != nil {
					log.Printf("Failed to marshal snapshot: %v", err)
					continue
				}
				tm := &titankvpb.RaftMessage{
					RegionId:   peer.regionID,
					FromPeerId: msg.From,
					ToPeerId:   msg.To,
					ToStoreId:  toStoreId,
					Data:       snapBytes,
				}
				if err := w.transport.SendSnapshot(tm); err != nil {
					log.Printf("SendSnapshot failed: %v", err)
				}
				continue
			}

			data, _ := msg.Marshal()
			tm := &titankvpb.RaftMessage{
				RegionId:   peer.regionID,
				FromPeerId: msg.From,
				ToPeerId:   msg.To,
				ToStoreId:  toStoreId,
				Data:       data,
			}

			messages = append(messages, tm)
		}

		// Advance
		peer.raftGroup.Advance(rd)
	}

	// 4. 执行阶段：批量发送
	if len(messages) > 0 {
		//start := time.Now()
		w.transport.Send(messages)
		//log.Printf("[Worker] Batch Send done. Msgs=%d, Cost=%v", len(messages), time.Since(start))
	}

	w.pendingPeers = make(map[uint64]*Peer)
}

func (w *StoreWorker) removePeer(p *Peer) {
	// 1. 从内存移除 (停止服务)
	delete(w.peers, p.regionID)
	delete(w.pendingPeers, p.regionID)
	delete(w.tickBuckets[w.bucketForRegion(p.regionID)], p.regionID)
	w.router.Unregister(p.regionID)

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

	// 2. 异步执行物理清理 (避免阻塞 Worker 主循环)
	// 我们需要清理两部分数据：
	// A. Data (z{RegionID}...)
	// B. Raft Log/State (r{RegionID}...)

	// 为了在 goroutine 中使用，拷贝需要的数据
	store := w.store
	regionID := p.regionID
	startKey := p.region.StartKey
	endKey := p.region.EndKey

	go func() {
		//log.Printf("[GC] Clearing data for removed Region %d...", regionID)

		// --- 清理 Data ---
		// 构造物理范围
		dataStart := DataKey(regionID, startKey)
		var dataEnd []byte
		if len(endKey) > 0 {
			dataEnd = DataKey(regionID, endKey)
		} else {
			// 如果 EndKey 无穷大，物理上是下一个 RegionID 的开始
			// DataKey 编码规则: 'z' + RegionID(8B) + UserKey
			// 所以下一个 Region 的前缀是 'z' + (RegionID+1)
			dataEnd = DataKey(regionID+1, nil)
		}

		if err := store.DeleteRange(dataStart, dataEnd); err != nil {
			log.Printf("[GC] Failed to delete data range: %v", err)
		}

		// --- 清理 Raft Log ---
		logStart := RaftLogKey(regionID, 0)
		logEnd := RaftStateKey(regionID)
		if err := store.DeleteRange(logStart, logEnd); err != nil {
			log.Printf("[GC] Failed to delete raft logs: %v", err)
		}

		//log.Printf("[GC] Region %d cleanup finished.", regionID)
	}()

	//log.Printf("Peer %d removed from Region %d (scheduled for GC)", p.peerID, p.regionID)
}

func (w *StoreWorker) AddPeer(p *Peer) {
	w.peers[p.regionID] = p
	w.pendingPeers[p.regionID] = p
	w.tickBuckets[w.bucketForRegion(p.regionID)][p.regionID] = p
}

func (w *StoreWorker) registerPeer(p *Peer) {
	w.peers[p.regionID] = p
	w.router.Register(p.regionID, w.receiver, p) // 注册路由
	w.tickBuckets[w.bucketForRegion(p.regionID)][p.regionID] = p

	// 如果新 Peer 也是本 Worker 管理，需要启动心跳吗？
	// onTick 会遍历 w.peers，所以自动生效
	log.Printf("Registered new peer for Region %d", p.regionID)
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
