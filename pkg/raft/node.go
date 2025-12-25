package raft

import (
	"context"
	"log"
	"time"

	"titankv/api/titankvpb"
	"titankv/pkg/store"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type TitanRaft struct {
	Node        raft.Node
	ID          uint64
	
	raftStorage *raft.MemoryStorage
	fsm         *store.TitanStore
	transport   *Transport
	lastApplied uint64
}

func NewTitanRaft(id uint64, peers map[uint64]string, fsm *store.TitanStore) *TitanRaft {
	storage := raft.NewMemoryStorage()

	c := &raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}

	var rpeers []raft.Peer
	for pID := range peers {
		rpeers = append(rpeers, raft.Peer{ID: pID})
	}

	n := raft.StartNode(c, rpeers)
	trans := NewTransport(peers)

	tr := &TitanRaft{
		Node:        n,
		ID:          id,
		raftStorage: storage,
		fsm:         fsm,
		transport:   trans,
	}

	go tr.run()
	return tr
}

func (tr *TitanRaft) Propose(ctx context.Context, cmd *titankvpb.RaftCommand) error {
	data, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	// 【修改】tr.node -> tr.Node
	return tr.Node.Propose(ctx, data)
}

func (tr *TitanRaft) Step(ctx context.Context, msg *titankvpb.RaftMessage) error {
	var rMsg raftpb.Message
	if err := rMsg.Unmarshal(msg.Data); err != nil {
		return err
	}
	// 【修改】tr.node -> tr.Node
	return tr.Node.Step(ctx, rMsg)
}

func (tr *TitanRaft) run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 【修改】tr.node -> tr.Node
			tr.Node.Tick()

		// 【修改】tr.node -> tr.Node
		case rd := <-tr.Node.Ready():
			tr.raftStorage.Append(rd.Entries)

			for _, msg := range rd.Messages {
				tr.sendRaftMessage(msg)
			}

			for _, entry := range rd.CommittedEntries {
				tr.processEntry(entry)
				if entry.Index > tr.lastApplied {
					tr.lastApplied = entry.Index
				}
			}

			tr.maybeTriggerSnapshot()
			
			// 【修改】tr.node -> tr.Node
			tr.Node.Advance()
		}
	}
}

func (tr *TitanRaft) maybeTriggerSnapshot() {
	const snapshotCount = 10
	first, _ := tr.raftStorage.FirstIndex()

	// 【调试日志】打印当前状态，看看为什么没触发
	// log.Printf("[SnapshotCheck] applied=%d first=%d count=%d", tr.lastApplied, first, snapshotCount)

	// 【关键修复】
	// 1. 确保 lastApplied >= first，防止减法溢出 (uint64 0 - 1 会变成很大的数！)
	// 2. 使用更安全的比较方式
	if tr.lastApplied >= first && (tr.lastApplied-first >= snapshotCount) {
		log.Printf("Compacting log up to index %d (first=%d)...", tr.lastApplied, first)
		
		var confState raftpb.ConfState
		// CreateSnapshot 会生成快照元数据
		_, err := tr.raftStorage.CreateSnapshot(tr.lastApplied, &confState, nil)
		if err != nil {
			log.Printf("CreateSnapshot failed: %v", err)
			return
		}

		// Compact 会丢弃旧日志
		err = tr.raftStorage.Compact(tr.lastApplied)
		if err != nil {
			log.Printf("Compact failed: %v", err)
			return
		}
		
		log.Printf("Compaction success. New first index: %d", tr.lastApplied+1)
	}
}

func (tr *TitanRaft) sendRaftMessage(msg raftpb.Message) {
	data, err := msg.Marshal()
	if err != nil {
		log.Printf("Failed to marshal raft msg: %v", err)
		return
	}
	tr.transport.Send(msg.To, &titankvpb.RaftMessage{Data: data})
}

func (tr *TitanRaft) processEntry(entry raftpb.Entry) {
	if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
		var cmd titankvpb.RaftCommand
		if err := proto.Unmarshal(entry.Data, &cmd); err != nil {
			log.Printf("Failed to unmarshal command: %v", err)
			return
		}

		if cmd.Op == titankvpb.RaftCommand_PUT {
			tr.fsm.Put(cmd.Key, cmd.Value)
			log.Printf("[Apply] Put Key=%s", string(cmd.Key))
		} else if cmd.Op == titankvpb.RaftCommand_DELETE {
			tr.fsm.Delete(cmd.Key)
			log.Printf("[Apply] Delete Key=%s", string(cmd.Key))
		}
	}
}