package raft

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
	"titankv/pkg/store"

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
	electionTicks = 20
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
	// batcher     *Batcher // 【删除】旧 Batcher 已废弃
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
	
	pdClient pdpb.PDClient
	region   atomic.Value // 存 *pdpb.Region
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
		pdClient:        pdClient,
	}

	// 初始化 Region 元数据
	initialRegion := &pdpb.Region{
		Id: 1,
		RegionEpoch: &pdpb.RegionEpoch{ConfVer: 0, Version: 1},
		Peers: []*pdpb.Peer{},
	}
	tr.region.Store(initialRegion)

	// 【删除】Batcher 初始化逻辑已移除

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

// LinearizableRead 获取线性一致性读的安全 Index
func (tr *TitanRaft) LinearizableRead(ctx context.Context) (uint64, error) {
	if tr.isLeaseValid() {
		// log.Printf("Lease Valid. Return local index: %d", tr.getApplied())
		return tr.getApplied(), nil
	} 
	// log.Printf("Lease Invalid! Fallback to ReadIndex.")
	return tr.requestReadIndex(ctx)
}

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

// 【关键修改】直接调用 Raft Node Propose，不再走 Batcher
func (tr *TitanRaft) Propose(ctx context.Context, cmd *titankvpb.RaftCommand) error {
	data, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	return tr.Node.Propose(ctx, data)
}

// 【删除】ProposeBatch 已移除

func (tr *TitanRaft) Step(ctx context.Context, msg *titankvpb.RaftMessage) error {
	var rMsg raftpb.Message
	if err := rMsg.Unmarshal(msg.Data); err != nil {
		return err
	}

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

    pdHeartbeatTicker := time.NewTicker(5 * time.Second)
    defer pdHeartbeatTicker.Stop()
    storeHeartbeatTicker := time.NewTicker(10 * time.Second)
    defer storeHeartbeatTicker.Stop()

	for {
		select {
          case <-pdHeartbeatTicker.C:
             tr.sendPDHeartbeat()
          case <-storeHeartbeatTicker.C:
            tr.sendStoreHeartbeat()
		case rd := <-tr.Node.Ready():
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

			if !raft.IsEmptySnap(rd.Snapshot) {
				tr.handleSnapshot(rd.Snapshot)
			}

			if !raft.IsEmptyHardState(rd.HardState) || len(rd.Entries) > 0 {
				if err := tr.wal.Save(rd.HardState, rd.Entries); err != nil {
					log.Fatalf("Failed to save WAL: %v", err)
				}
			}

			tr.raftStorage.Append(rd.Entries)

			for _, msg := range rd.Messages {
				tr.sendRaftMessage(msg)
			}

			for _, entry := range rd.CommittedEntries {
				if entry.Type == raftpb.EntryConfChange {
	                var cc raftpb.ConfChange
	                cc.Unmarshal(entry.Data)
					tr.applyConfChange(cc)
                } else {
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
	log.Printf("Snapshot applied. LastApplied: %d", snap.Metadata.Index)
}

func (tr *TitanRaft) Stop() {
    tr.Node.Stop()
}

func (tr *TitanRaft) sendPDHeartbeat() {
    if tr.Node.Status().Lead != tr.ID {
        return
    }

    region := &pdpb.Region{
        Id:       1,
        StartKey: []byte(""),
        EndKey:   []byte(""),
        RegionEpoch: &pdpb.RegionEpoch{
            ConfVer: 1,
            Version: 1,
        },
        Peers: []*pdpb.Peer{}, 
    }
    
    for _, pid := range tr.peers {
        region.Peers = append(region.Peers, &pdpb.Peer{Id: pid, StoreId: pid})
    }
    
    leaderPeer := &pdpb.Peer{Id: tr.ID, StoreId: tr.ID}
    approxSize := uint64(100)
    
    go func() {
        req := &pdpb.RegionHeartbeatRequest{
            Region:          region,
            Leader:          leaderPeer,
            ApproximateSize: approxSize,
        }
        
        ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
        defer cancel()
        
        resp, err := tr.pdClient.RegionHeartbeat(ctx, req)
        if err != nil {
            log.Printf("Failed to send region heartbeat: %v", err)
            return
        }

        if resp.TransferLeader != nil {
            log.Printf("Received TransferLeader to peer %d", resp.TransferLeader.PeerId)
            tr.Node.TransferLeadership(context.Background(), 1, resp.TransferLeader.PeerId)
        } else if resp.ChangePeer != nil {
            tr.handleConfChange(resp.ChangePeer)
        }
    }()
}

func (tr *TitanRaft) handleConfChange(cp *pdpb.ChangePeer) {
    var cc raftpb.ConfChange
    cc.ID = tr.reqIDGen + 1 
    cc.NodeID = cp.Peer.Id
    
    if cp.ChangeType == pdpb.ChangePeer_ADD_NODE {
        cc.Type = raftpb.ConfChangeAddNode
        log.Printf("Received AddPeer %d on store %d", cp.Peer.Id, cp.Peer.StoreId)
    } else if cp.ChangeType == pdpb.ChangePeer_REMOVE_NODE {
        cc.Type = raftpb.ConfChangeRemoveNode
        log.Printf("Received RemovePeer %d on store %d", cp.Peer.Id, cp.Peer.StoreId)
    }

    if err := tr.Node.ProposeConfChange(context.Background(), cc); err != nil {
        log.Printf("Failed to propose conf change: %v", err)
    }
}

func (tr *TitanRaft) updatePeersSlice(cc raftpb.ConfChange) {
	if cc.Type == raftpb.ConfChangeAddNode {
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
		if cc.NodeID == tr.ID {
			log.Printf("❌ I have been removed from the cluster! Shutting down...")
		}
	}
}

func (tr *TitanRaft) sendStoreHeartbeat() {
	stats := &pdpb.StoreStats{
		Capacity:    100 * 1024 * 1024 * 1024,
		Available:   50 * 1024 * 1024 * 1024,
		RegionCount: 0,
	}

	go func() {
		meta := &pdpb.MetaStore{
			Id:      tr.ID,
			Address: "127.0.0.1:????",
			State:   pdpb.StoreState_UP,
		}
		_, err := tr.pdClient.PutStore(context.Background(), &pdpb.PutStoreRequest{Store: meta})
		if err != nil {
			log.Printf("PutStore failed: %v", err)
			return
		}

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

func (tr *TitanRaft) CheckEpoch(reqEpoch *pdpb.RegionEpoch) error {
	val := tr.region.Load()
	if val == nil {
		return nil
	}
	current := val.(*pdpb.Region)

	if reqEpoch.Version != current.RegionEpoch.Version {
		return fmt.Errorf("epoch not match: version %d != %d", reqEpoch.Version, current.RegionEpoch.Version)
	}

	if reqEpoch.ConfVer != current.RegionEpoch.ConfVer {
		return fmt.Errorf("epoch not match: conf_ver %d != %d", reqEpoch.ConfVer, current.RegionEpoch.ConfVer)
	}

	return nil
}

func (tr *TitanRaft) applyConfChange(cc raftpb.ConfChange) {
	tr.Node.ApplyConfChange(cc)
	tr.updatePeersSlice(cc)
	tr.updateRegionMeta(cc)
	log.Printf("Applied ConfChange: Type=%v, NodeID=%d", cc.Type, cc.NodeID)
}

func (tr *TitanRaft) updateRegionMeta(cc raftpb.ConfChange) {
	oldRegion := tr.region.Load().(*pdpb.Region)
	newRegion := proto.Clone(oldRegion).(*pdpb.Region)

	newRegion.RegionEpoch.ConfVer++

	if cc.Type == raftpb.ConfChangeAddNode {
		exists := false
		for _, p := range newRegion.Peers {
			if p.Id == cc.NodeID { exists = true; break }
		}
		if !exists {
			newRegion.Peers = append(newRegion.Peers, &pdpb.Peer{
				Id:      cc.NodeID,
				StoreId: cc.NodeID,
			})
		}
	} else if cc.Type == raftpb.ConfChangeRemoveNode {
		var activePeers []*pdpb.Peer
		for _, p := range newRegion.Peers {
			if p.Id != cc.NodeID {
				activePeers = append(activePeers, p)
			}
		}
		newRegion.Peers = activePeers
	}

	tr.region.Store(newRegion)
}