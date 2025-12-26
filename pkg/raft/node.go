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

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type TitanRaft struct {
	Node        raft.Node
	ID          uint64
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL
	snapshotter *snap.Snapshotter
	fsm         *store.TitanStore
	transport   *Transport
	peers       []uint64
	walDir      string

	// lastApplied 保持 uint64，通过 atomic 函数操作
	lastApplied uint64

	// 原子计数器，用于生成 ReadIndex 的唯一 Request ID
	reqIDGen uint64

	// ReadIndex 通知机制
	readWaitC map[string]chan uint64
	readMu    sync.Mutex

	// 使用 Cond 优化 WaitApplied 性能
	applyMu   sync.Mutex
	applyCond *sync.Cond

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

func NewTitanRaft(id uint64, peers map[uint64]string, fsm *store.TitanStore, dbPath string) *TitanRaft {
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
		ElectionTick:    10,
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
	hs, _, err := storage.InitialState()
	if err != nil {
		log.Fatalf("Failed to get initial state: %v", err)
	}

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
		Node:        n,
		ID:          id,
		raftStorage: storage,
		wal:         w,
		snapshotter: snapshotter,
		fsm:         fsm,
		transport:   trans,
		peers:       peerIDs,
		walDir:      walDir,
		readWaitC:   make(map[string]chan uint64),
	}

	// 初始化 Condition Variable
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

func (tr *TitanRaft) LinearizableRead(ctx context.Context) (uint64, error) {
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
		// 注意：这里的 ctx 取消无法直接中断 Wait，只能放弃等待。
		// 被阻塞的 goroutine 会在下一次 Broadcast 后自行退出。
		return ctx.Err()
	}
}

func (tr *TitanRaft) cleanupReadWait(id string) {
	tr.readMu.Lock()
	delete(tr.readWaitC, id)
	tr.readMu.Unlock()
}

func (tr *TitanRaft) Propose(ctx context.Context, cmd *titankvpb.RaftCommand) error {
	data, err := proto.Marshal(cmd)
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
	return tr.Node.Step(ctx, rMsg)
}

func (tr *TitanRaft) run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	log.Printf("Raft run loop started for Node %d", tr.ID)

	for {
		select {
		case <-ticker.C:
			tr.Node.Tick()

		case rd := <-tr.Node.Ready():
			// 1. 处理 ReadStates (ReadIndex 回调)
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

			// 2. 处理快照
			if !raft.IsEmptySnap(rd.Snapshot) {
				tr.handleSnapshot(rd.Snapshot)
			}

			// 3. 持久化 WAL
			if !raft.IsEmptyHardState(rd.HardState) || len(rd.Entries) > 0 {
				if err := tr.wal.Save(rd.HardState, rd.Entries); err != nil {
					log.Fatalf("Failed to save WAL: %v", err)
				}
			}

			tr.raftStorage.Append(rd.Entries)

			for _, msg := range rd.Messages {
				tr.sendRaftMessage(msg)
			}

			// 4. 应用日志
			for _, entry := range rd.CommittedEntries {
				tr.processEntry(entry)
				
				// 原子更新并广播
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
	
	log.Printf("Received snapshot from leader! Index: %d", snap.Metadata.Index)
	if err := tr.snapshotter.SaveSnap(snap); err != nil {
		log.Fatalf("Failed to save received snapshot: %v", err)
	}
	if err := tr.raftStorage.ApplySnapshot(snap); err != nil {
		log.Fatalf("Failed to apply snapshot to storage: %v", err)
	}
	tr.processSnapshot(snap)

	if err := tr.wal.Close(); err != nil {
		log.Fatalf("Failed to close WAL: %v", err)
	}
	if err := os.RemoveAll(tr.walDir); err != nil {
		log.Fatalf("Failed to remove old WAL: %v", err)
	}
	if err := os.MkdirAll(tr.walDir, 0750); err != nil {
		log.Fatalf("Failed to recreate wal dir: %v", err)
	}
	w, err := wal.Create(zap.NewExample(), tr.walDir, nil)
	if err != nil {
		log.Fatalf("Failed to recreate WAL: %v", err)
	}
	tr.wal = w
	if err := tr.wal.SaveSnapshot(walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: &snap.Metadata.ConfState,
	}); err != nil {
		log.Fatalf("Failed to save snapshot to WAL: %v", err)
	}
}

func (tr *TitanRaft) maybeTriggerSnapshot() {
	const snapshotCount = 10
	first, _ := tr.raftStorage.FirstIndex()
	applied := tr.getApplied()

	if applied >= first && (applied-first >= snapshotCount) {
		log.Printf("Compacting log up to index %d...", applied)
		
		confState := raftpb.ConfState{Voters: tr.peers}
		snap, err := tr.raftStorage.CreateSnapshot(applied, &confState, nil)
		if err != nil {
			if err != raft.ErrSnapOutOfDate {
				log.Printf("CreateSnapshot failed: %v", err)
			}
			return
		}

		if err := tr.snapshotter.SaveSnap(snap); err != nil {
			log.Fatalf("Failed to save snapshot: %v", err)
		}
		
		if err := tr.wal.ReleaseLockTo(snap.Metadata.Index); err != nil {
			log.Printf("Failed to release WAL lock: %v", err)
		}

		if err := tr.raftStorage.Compact(applied); err != nil {
			log.Printf("Compact failed: %v", err)
			return
		}
	}
}

func (tr *TitanRaft) sendRaftMessage(msg raftpb.Message) {
	data, err := msg.Marshal()
	if err != nil {
		log.Printf("Failed to marshal raft msg: %v", err)
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
		// 1. 优先尝试解析为 BatchCommand
		var batch titankvpb.BatchRaftCommand
		if err := proto.Unmarshal(entry.Data, &batch); err == nil && len(batch.Commands) > 0 {
			// 是批处理，循环执行
			for _, cmd := range batch.Commands {
				tr.applySingleCommand(cmd)
			}
			return
		}

		// 2. 如果解析 Batch 失败，尝试解析为单条 Command (兼容旧数据)
		var cmd titankvpb.RaftCommand
		if err := proto.Unmarshal(entry.Data, &cmd); err == nil {
			tr.applySingleCommand(&cmd)
		} else {
			log.Printf("Failed to unmarshal raft entry data")
		}
	}
}

// 提取出的单条执行逻辑
func (tr *TitanRaft) applySingleCommand(cmd *titankvpb.RaftCommand) {
	if cmd.Op == titankvpb.RaftCommand_PUT {
		tr.fsm.Put(cmd.Key, cmd.Value)
		// 高并发下建议注释掉日志，否则 IO 会成为瓶颈
		// log.Printf("[Apply] Put Key=%s", string(cmd.Key))
	} else if cmd.Op == titankvpb.RaftCommand_DELETE {
		tr.fsm.Delete(cmd.Key)
	}
}

func (tr *TitanRaft) processSnapshot(snap raftpb.Snapshot) {
	atomic.StoreUint64(&tr.lastApplied, snap.Metadata.Index)
	tr.applyCond.Broadcast()
	log.Printf("Snapshot applied. LastApplied: %d", snap.Metadata.Index)
}

// 新增：ProposeBatch
func (tr *TitanRaft) ProposeBatch(ctx context.Context, batch *titankvpb.BatchRaftCommand) error {
	data, err := proto.Marshal(batch)
	if err != nil {
		return err
	}
	// 将整个 batch 作为一个 Log Entry 提交
	return tr.Node.Propose(ctx, data)
}