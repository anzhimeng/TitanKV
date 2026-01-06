package raft

import (
	"context"
	"encoding/binary"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"titankv/api/titankvpb"
	"titankv/pkg/store"
	"titankv/pd/api/pdpb"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	electionTicks = 20 // 2秒超时，容忍 IO 抖动
	tickMs        = 100
	
	// 时钟漂移保护时间
	ClockDriftBound = 5 * time.Millisecond
)

type TitanRaft struct {
	Node        raft.Node
	ID          uint64
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL
	snapshotter *snap.Snapshotter
	fsm         *store.TitanStore
	transport   *Transport
	batcher     *Batcher
	peers       []uint64
	walDir      string

	lastApplied uint64 // atomic
	reqIDGen    uint64 // atomic

	readWaitC map[string]chan uint64
	readMu    sync.Mutex

	applyMu   sync.Mutex
	applyCond *sync.Cond

	// Lease Read 相关
	peerLastActive  sync.Map
	leaseExpiration time.Time
	leaseMu         sync.Mutex 
	electionTimeout time.Duration
	pdClient pdpb.PDClient // 【新增】持有 PD 连接
}

func replayWAL(walDir string, snapshot *raftpb.Snapshot) (*wal.WAL, *raft.MemoryStorage) {
	if !fileutil.Exist(walDir) {
		if err := os.MkdirAll(walDir, 0750); err != nil {
			log.Fatalf("Cannot create dir for wal: %v", err)
		}

		w, err := wal.Create(zap.NewExample(), walDir, nil)
		if err != nil {
			log.Fatalf("Failed to create WAL: %v", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index = snapshot.Metadata.Index
		walsnap.Term = snapshot.Metadata.Term
	}

	w, err := wal.Open(zap.NewExample(), walDir, walsnap)
	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}

	_, state, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("Failed to read WAL: %v", err)
	}

	storage := raft.NewMemoryStorage()
	if snapshot != nil {
		storage.ApplySnapshot(*snapshot)
	}
	if state.Term != 0 {
		storage.SetHardState(state)
	}
	storage.Append(ents)

	return w, storage
}

func NewTitanRaft(id uint64, peers map[uint64]string, fsm *store.TitanStore, dbPath string, pdClient pdpb.PDClient) *TitanRaft {
	walDir := filepath.Join(dbPath, "raft-wal")
	snapDir := filepath.Join(dbPath, "raft-snap")

	if !fileutil.Exist(snapDir) {
		if err := os.MkdirAll(snapDir, 0750); err != nil {
			log.Fatalf("Cannot create dir for snap: %v", err)
		}
	}

	snapshotter := snap.New(zap.NewExample(), snapDir)
	snapshot, err := snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("Failed to load snapshot: %v", err)
	}

	w, storage := replayWAL(walDir, snapshot)

	c := &raft.Config{
		ID:              id,
		ElectionTick:    electionTicks,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
		CheckQuorum:     true,
		PreVote:         true,
	}

	var rpeers []raft.Peer
	var peerIDs []uint64
	for pID := range peers {
		rpeers = append(rpeers, raft.Peer{ID: pID})
		peerIDs = append(peerIDs, pID)
	}

	var n raft.Node
	hs, _, _ := storage.InitialState()
	lastIndex, _ := storage.LastIndex()

	if lastIndex > 0 || !raft.IsEmptyHardState(hs) {
		log.Printf("Restarting Raft Node %d", id)
		n = raft.RestartNode(c)
	} else {
		log.Printf("Starting new Raft Node %d", id)
		n = raft.StartNode(c, rpeers)
	}

	trans := NewTransport(peers)

	tr := &TitanRaft{
		Node:            n,
		ID:              id,
		raftStorage:     storage,
		wal:             w,
		snapshotter:     snapshotter,
		fsm:             fsm,
		transport:       trans,
		peers:           peerIDs,
		walDir:          walDir,
		readWaitC:       make(map[string]chan uint64),
		electionTimeout: time.Duration(electionTicks*tickMs) * time.Millisecond,
		pdClient: pdClient,
	}
	
	// 内部初始化 Batcher
	tr.batcher = NewBatcher(tr, 100, 10*time.Millisecond)

	tr.applyCond = sync.NewCond(&tr.applyMu)

	if snapshot != nil {
		atomic.StoreUint64(&tr.lastApplied, snapshot.Metadata.Index)
	}

	go tr.run()
	return tr
}

func (tr *TitanRaft) getApplied() uint64 {
	return atomic.LoadUint64(&tr.lastApplied)
}

// 核心读入口：线性一致性读
func (tr *TitanRaft) LinearizableRead(ctx context.Context) (uint64, error) {
	// 1. 尝试 Lease Read (Fast Path)
	if tr.isLeaseValid() {
		return tr.getApplied(), nil
	}

	// 2. 回退到标准 ReadIndex (Slow Path)
	return tr.requestReadIndex(ctx)
}

// 检查租约有效性
func (tr *TitanRaft) isLeaseValid() bool {
	if tr.Node.Status().Lead != tr.ID {
		return false
	}
	tr.leaseMu.Lock()
	defer tr.leaseMu.Unlock()
	
	safeTime := time.Now().Add(ClockDriftBound)
	return safeTime.Before(tr.leaseExpiration)
}

func (tr *TitanRaft) requestReadIndex(ctx context.Context) (uint64, error) {
	reqID := atomic.AddUint64(&tr.reqIDGen, 1)
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, reqID)
	idStr := string(idBytes)

	ch := make(chan uint64, 1)
	tr.readMu.Lock()
	tr.readWaitC[idStr] = ch
	tr.readMu.Unlock()

	if err := tr.Node.ReadIndex(ctx, idBytes); err != nil {
		tr.cleanupReadWait(idStr)
		return 0, err
	}

	select {
	case index := <-ch:
		return index, nil
	case <-ctx.Done():
		tr.cleanupReadWait(idStr)
		return 0, ctx.Err()
	}
}

func (tr *TitanRaft) cleanupReadWait(id string) {
	tr.readMu.Lock()
	delete(tr.readWaitC, id)
	tr.readMu.Unlock()
}

func (tr *TitanRaft) WaitApplied(ctx context.Context, targetIndex uint64) error {
	if tr.getApplied() >= targetIndex {
		return nil
	}

	doneC := make(chan struct{})
	go func() {
		tr.applyMu.Lock()
		defer tr.applyMu.Unlock()
		for tr.getApplied() < targetIndex {
			tr.applyCond.Wait()
		}
		close(doneC)
	}()

	select {
	case <-doneC:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Client 调用的 Propose (走 Batcher)
func (tr *TitanRaft) Propose(ctx context.Context, cmd *titankvpb.RaftCommand) error {
	return tr.batcher.Propose(ctx, cmd)
}

// Batcher 调用的批量 Propose
func (tr *TitanRaft) ProposeBatch(ctx context.Context, batch *titankvpb.BatchRaftCommand) error {
	data, err := proto.Marshal(batch)
	if err != nil {
		return err
	}
	return tr.Node.Propose(ctx, data)
}

func (tr *TitanRaft) Step(ctx context.Context, msg *titankvpb.RaftMessage) error {
	var rMsg raftpb.Message
	if err := rMsg.Unmarshal(msg.Data); err != nil {
		return err
	}

	// 拦截心跳，更新租约
	if rMsg.Type == raftpb.MsgHeartbeatResp {
		tr.peerLastActive.Store(rMsg.From, time.Now())
		tr.leaseMu.Lock()
		tr.updateLease()
		tr.leaseMu.Unlock()
	}

	return tr.Node.Step(ctx, rMsg)
}

func (tr *TitanRaft) updateLease() {
	if tr.Node.Status().Lead != tr.ID {
		return
	}

	activeCount := 1
	now := time.Now()
	
	tr.peerLastActive.Range(func(key, value interface{}) bool {
		lastActive := value.(time.Time)
		if now.Sub(lastActive) < tr.electionTimeout {
			activeCount++
		}
		return true
	})

	quorum := len(tr.peers)/2 + 1
	if activeCount >= quorum {
		newExpiry := now.Add(tr.electionTimeout)
		if newExpiry.After(tr.leaseExpiration) {
			tr.leaseExpiration = newExpiry
		}
	}
}

func (tr *TitanRaft) run() {
	// 独立 Ticker
	go func() {
		ticker := time.NewTicker(time.Duration(tickMs) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				tr.Node.Tick()
			}
		}
	}()

    // 【新增】Region 心跳定时器 (每 5 秒一次)
    pdHeartbeatTicker := time.NewTicker(5 * time.Second)
    defer pdHeartbeatTicker.Stop()
    storeHeartbeatTicker := time.NewTicker(10 * time.Second)
    defer storeHeartbeatTicker.Stop()

	for {
		select {
		// 【新增】触发 PD 心跳
          case <-pdHeartbeatTicker.C:
             tr.sendPDHeartbeat()
          // 【新增】发送 Store 心跳
          case <-storeHeartbeatTicker.C:
            tr.sendStoreHeartbeat()
		case rd := <-tr.Node.Ready():
			// 1. ReadIndex
			if len(rd.ReadStates) > 0 {
				tr.readMu.Lock()
				for _, rs := range rd.ReadStates {
					id := string(rs.RequestCtx)
					if ch, ok := tr.readWaitC[id]; ok {
						ch <- rs.Index
						delete(tr.readWaitC, id)
					}
				}
				tr.readMu.Unlock()
			}

			// 2. Snapshot
			if !raft.IsEmptySnap(rd.Snapshot) {
				tr.handleSnapshot(rd.Snapshot)
			}

			// 3. WAL
			if !raft.IsEmptyHardState(rd.HardState) || len(rd.Entries) > 0 {
				if err := tr.wal.Save(rd.HardState, rd.Entries); err != nil {
					log.Fatalf("Failed to save WAL: %v", err)
				}
			}

			tr.raftStorage.Append(rd.Entries)

			// 4. Send
			for _, msg := range rd.Messages {
				tr.sendRaftMessage(msg)
			}

			// 5. Apply
			for _, entry := range rd.CommittedEntries {
			     if entry.Type == raftpb.EntryConfChange {
	                    var cc raftpb.ConfChange
	                    cc.Unmarshal(entry.Data)
	                    
	                    // 1. 更新 Raft 内部成员列表
	                    tr.Node.ApplyConfChange(cc)
	                    
	                    // 2. 更新本地维护的 peers 列表 (用于快照和心跳)
	                    tr.updatePeers(cc)
	                    
                    	log.Printf("Applied ConfChange: %v", cc)
                	} else {
                		// 普通 Put/Delete
					tr.processEntry(entry)
				}
				currentApplied := tr.getApplied()
				if entry.Index > currentApplied {
					atomic.StoreUint64(&tr.lastApplied, entry.Index)
					tr.applyCond.Broadcast()
				}
			}

			tr.maybeTriggerSnapshot()
			tr.Node.Advance()
		}
	}
}

func (tr *TitanRaft) handleSnapshot(snap raftpb.Snapshot) {
	if raft.IsEmptySnap(snap) { return }
	
	log.Printf("Received snapshot! Index: %d", snap.Metadata.Index)
	if err := tr.snapshotter.SaveSnap(snap); err != nil {
		log.Fatalf("Save snap fail: %v", err)
	}
	if err := tr.raftStorage.ApplySnapshot(snap); err != nil {
		log.Fatalf("Apply snap fail: %v", err)
	}
	tr.processSnapshot(snap)

	if err := tr.wal.Close(); err != nil {
		log.Fatalf("Close WAL fail: %v", err)
	}
	if err := os.RemoveAll(tr.walDir); err != nil {
		log.Fatalf("Remove old WAL fail: %v", err)
	}
	if err := os.MkdirAll(tr.walDir, 0750); err != nil {
		log.Fatalf("Recreate wal dir fail: %v", err)
	}
	w, err := wal.Create(zap.NewExample(), tr.walDir, nil)
	if err != nil {
		log.Fatalf("Recreate WAL fail: %v", err)
	}
	tr.wal = w
	if err := tr.wal.SaveSnapshot(walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: &snap.Metadata.ConfState,
	}); err != nil {
		log.Fatalf("Save snap to WAL fail: %v", err)
	}
}

func (tr *TitanRaft) maybeTriggerSnapshot() {
	const snapshotCount = 5000
	first, _ := tr.raftStorage.FirstIndex()
	applied := tr.getApplied()

	if applied >= first && (applied-first >= snapshotCount) {
		log.Printf("Compacting log to %d...", applied)
		confState := raftpb.ConfState{Voters: tr.peers}
		snap, err := tr.raftStorage.CreateSnapshot(applied, &confState, nil)
		if err != nil {
			if err != raft.ErrSnapOutOfDate {
				log.Printf("CreateSnapshot fail: %v", err)
			}
			return
		}

		if err := tr.snapshotter.SaveSnap(snap); err != nil {
			log.Fatalf("Save snap fail: %v", err)
		}
		if err := tr.wal.ReleaseLockTo(snap.Metadata.Index); err != nil {
			log.Printf("Release WAL fail: %v", err)
		}
		if err := tr.raftStorage.Compact(applied); err != nil {
			log.Printf("Compact fail: %v", err)
		}
	}
}

func (tr *TitanRaft) sendRaftMessage(msg raftpb.Message) {
	data, err := msg.Marshal()
	if err != nil {
		log.Printf("Marshal fail: %v", err)
		return
	}
	go func() {
		tr.transport.Send(msg.To, &titankvpb.RaftMessage{Data: data})
		if msg.Type == raftpb.MsgSnap {
			tr.Node.ReportSnapshot(msg.To, raft.SnapshotFinish)
		}
	}()
}

func (tr *TitanRaft) processEntry(entry raftpb.Entry) {
	if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
		var batch titankvpb.BatchRaftCommand
		if err := proto.Unmarshal(entry.Data, &batch); err == nil && len(batch.Commands) > 0 {
			for _, cmd := range batch.Commands {
				tr.applySingleCommand(cmd)
			}
			return
		}

		var cmd titankvpb.RaftCommand
		if err := proto.Unmarshal(entry.Data, &cmd); err == nil {
			tr.applySingleCommand(&cmd)
		}
	}
}

func (tr *TitanRaft) applySingleCommand(cmd *titankvpb.RaftCommand) {
	if cmd.Op == titankvpb.RaftCommand_PUT {
		tr.fsm.Put(cmd.Key, cmd.Value)
	} else if cmd.Op == titankvpb.RaftCommand_DELETE {
		tr.fsm.Delete(cmd.Key)
	}
}

func (tr *TitanRaft) processSnapshot(snap raftpb.Snapshot) {
	atomic.StoreUint64(&tr.lastApplied, snap.Metadata.Index)
	tr.applyCond.Broadcast()
}

func (tr *TitanRaft) Stop() {
    tr.Node.Stop()
}

func (tr *TitanRaft) sendPDHeartbeat() {
    // 只有 Leader 才发 Region 心跳
    if tr.Node.Status().Lead != tr.ID {
        return
    }
    // 【测试 Hack】: 只有 Node 1 (假设 ID=1) 发送伪造的 Region 心跳
    /*if tr.ID == 1 {
        // 模拟报告 10 个 Region
        for i := uint64(100); i < 110; i++ {
            fakeRegion := &pdpb.Region{
                Id:       i,
                Peers:    []*pdpb.Peer{{Id: i*10 + 1, StoreId: 1}, {Id: i*10 + 2, StoreId: 2}, {Id: i*10 + 3, StoreId: 3}},
                // 伪造版本
                RegionEpoch: &pdpb.RegionEpoch{ConfVer: 1, Version: 1},
            }
            
            // 说我是 Leader
            fakeLeader := &pdpb.Peer{Id: i*10 + 1, StoreId: 1}
            
            go func(r *pdpb.Region, l *pdpb.Peer) {
                req := &pdpb.RegionHeartbeatRequest{
                    Region:          r,
                    Leader:          l,
                    ApproximateSize: 10,
                }
                // 发送
                tr.pdClient.RegionHeartbeat(context.Background(), req)
            }(fakeRegion, fakeLeader)
        }
    }*/
    // 1. 组装 Region 信息
    // 目前我们是单 Raft 组，假设 ID=1，范围是全集
    // 生产环境这些信息应该存储在 Raft Storage 的元数据中
    region := &pdpb.Region{
        Id:       1, // Hardcode for Phase 2
        StartKey: []byte(""),
        EndKey:   []byte(""),
        RegionEpoch: &pdpb.RegionEpoch{
            ConfVer: 1,
            Version: 1,
        },
        // Peers: 应该填入当前配置中的所有 Peer
        Peers: []*pdpb.Peer{}, 
    }
    
    // 填充 Peers (从 tr.peers 获取)
    // 注意：我们现在的 tr.peers 只是 ID 列表，Proto 需要 ID + StoreID
    // 简化：假设 PeerID == StoreID
    for _, pid := range tr.peers {
        region.Peers = append(region.Peers, &pdpb.Peer{Id: pid, StoreId: pid})
    }
    
    // 2. 组装 Leader 信息
    leaderPeer := &pdpb.Peer{Id: tr.ID, StoreId: tr.ID}

    // 3. 统计信息 (调用 C++ 接口获取，这里先 Mock)
    approxSize := uint64(100) // MB
    
    // 4. 发送请求 (异步，不要阻塞 Raft 循环)
    go func() {
        req := &pdpb.RegionHeartbeatRequest{
            Region:          region,
            Leader:          leaderPeer,
            ApproximateSize: approxSize,
        }
        
        // 使用短超时
        ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
        defer cancel()
        
        resp, err := tr.pdClient.RegionHeartbeat(ctx, req)
        if err != nil {
            log.Printf("Failed to send region heartbeat: %v", err)
        }
        // 【新增】处理调度指令
        if resp.TransferLeader != nil {
            log.Printf("Received TransferLeader to peer %d", resp.TransferLeader.PeerId)
            tr.Node.TransferLeadership(context.Background(), 1, resp.TransferLeader.PeerId)
        } else if resp.ChangePeer != nil {
            tr.handleConfChange(resp.ChangePeer)
        }
    }()
}

// 处理成员变更
func (tr *TitanRaft) handleConfChange(cp *pdpb.ChangePeer) {
    var cc raftpb.ConfChange
    cc.ID = tr.reqIDGen + 1 // 简单生成个 ID，或者用 atomic
    cc.NodeID = cp.Peer.Id
    
    if cp.ChangeType == pdpb.ChangePeer_ADD_NODE {
        cc.Type = raftpb.ConfChangeAddNode
        log.Printf("Received AddPeer %d on store %d", cp.Peer.Id, cp.Peer.StoreId)
    } else if cp.ChangeType == pdpb.ChangePeer_REMOVE_NODE {
        cc.Type = raftpb.ConfChangeRemoveNode
        log.Printf("Received RemovePeer %d on store %d", cp.Peer.Id, cp.Peer.StoreId)
    }

    // Propose ConfChange
    if err := tr.Node.ProposeConfChange(context.Background(), cc); err != nil {
        log.Printf("Failed to propose conf change: %v", err)
    }
}

func (tr *TitanRaft) updatePeers(cc raftpb.ConfChange) {
    if cc.Type == raftpb.ConfChangeAddNode {
        // 检查是否已存在
        for _, id := range tr.peers {
            if id == cc.NodeID { return }
        }
        tr.peers = append(tr.peers, cc.NodeID)
    } else if cc.Type == raftpb.ConfChangeRemoveNode {
        var newPeers []uint64
        for _, id := range tr.peers {
            if id != cc.NodeID {
                newPeers = append(newPeers, id)
            }
        }
        tr.peers = newPeers
        
        // 如果是自己被删除了，关闭节点
        if cc.NodeID == tr.ID {
            log.Printf("I have been removed from the cluster! Shutting down...")
            // tr.Stop() 
        }
    }
}

func (tr *TitanRaft) sendStoreHeartbeat() {
	// 1. 构造 Store 统计信息
    // 【修改】titankvpb -> pdpb
	stats := &pdpb.StoreStats{
		Capacity:    100 * 1024 * 1024 * 1024,
		Available:   50 * 1024 * 1024 * 1024,
		RegionCount: 0,
	}

	// 2. 发送心跳 (异步)
	go func() {
		// A. 先尝试注册 Store
        // 【修改】titankvpb -> pdpb
		meta := &pdpb.MetaStore{
			Id:      tr.ID,
			Address: "127.0.0.1:????", // 记得这里如果是测试最好填写真实地址或Mock
			State:   pdpb.StoreState_UP, // 【修改】titankvpb -> pdpb
		}
        // 【修改】titankvpb -> pdpb
		_, err := tr.pdClient.PutStore(context.Background(), &pdpb.PutStoreRequest{Store: meta})
		if err != nil {
			log.Printf("PutStore failed: %v", err)
			return
		}

		// B. 发送心跳
        // 【修改】titankvpb -> pdpb
		req := &pdpb.StoreHeartbeatRequest{
			StoreId: tr.ID,
			Stats:   stats,
		}
		_, err = tr.pdClient.StoreHeartbeat(context.Background(), req)
		if err != nil {
			log.Printf("StoreHeartbeat failed: %v", err)
		}
	}()
}