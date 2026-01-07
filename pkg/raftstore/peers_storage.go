package raftstore

import (
	"fmt"
	"log"

	"titankv/api/titankvpb"
	"titankv/pkg/store" // C++ Store

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// PeerStorage 负责单个 Region 的 Raft 数据存储
type PeerStorage struct {
	engine  *store.TitanStore
	region  *titankvpb.Region
	
	// 缓存 RaftLocalState，因为 Raft 频繁访问 Term 和 Index
	raftState titankvpb.RaftLocalState
	// 缓存 ApplyState
	applyState titankvpb.RaftApplyState
}

func NewPeerStorage(engine *store.TitanStore, region *titankvpb.Region) (*PeerStorage, error) {
	s := &PeerStorage{
		engine: engine,
		region: region,
	}
	// 初始化时从磁盘加载状态
	if err := s.loadState(); err != nil {
		return nil, err
	}
	return s, nil
}

// 加载 HardState 和 ApplyState
func (s *PeerStorage) loadState() error {
	// 1. 加载 RaftState (Term, Vote, Commit)
	val, err := s.engine.Get(RaftStateKey(s.region.Id))
	if err != nil && err.Error() != "key not found" {
		return err
	}
	if len(val) > 0 {
		if err := proto.Unmarshal(val, &s.raftState); err != nil {
			return err
		}
	} else {
		// 新 Region，初始化
		// 实际上应该由 Split 流程初始化，这里做个兜底
	}

	// 2. 加载 ApplyState
	val, err = s.engine.Get(ApplyStateKey(s.region.Id))
	if err != nil && err.Error() != "key not found" {
		return err
	}
	if len(val) > 0 {
		if err := proto.Unmarshal(val, &s.applyState); err != nil {
			return err
		}
	}
	
	return nil
}

// --- 实现 raft.Storage 接口 ---

func (s *PeerStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	// 转换本地 Proto 到 Raftpb
	hs := raftpb.HardState{
		Term:   s.raftState.Term,
		Vote:   s.raftState.Vote,
		Commit: s.raftState.Commit,
	}
	// ConfState 需要从 Region 元数据中提取
	// 这里简化：假设 Region.Peers 就是 ConfState
	var cs raftpb.ConfState
	for _, p := range s.region.Peers {
		cs.Voters = append(cs.Voters, p.Id)
	}
	return hs, cs, nil
}

func (s *PeerStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	// 检查范围有效性
	if lo <= s.applyState.TruncatedState.Index {
		return nil, raft.ErrCompacted
	}
	if hi > s.raftState.LastIndex + 1 {
		return nil, raft.ErrUnavailable
	}

	var entries []raftpb.Entry
	var size uint64

	// 循环读取 C++ 引擎
	for i := lo; i < hi; i++ {
		key := RaftLogKey(s.region.Id, i)
		val, err := s.engine.Get(key)
		if err != nil {
			return nil, err
		}
		
		var ent raftpb.Entry
		if err := proto.Unmarshal(val, &ent); err != nil {
			return nil, err
		}
		
		entries = append(entries, ent)
		size += uint64(ent.Size())
		if size > maxSize {
			break
		}
	}
	return entries, nil
}

func (s *PeerStorage) Term(i uint64) (uint64, error) {
	if i == s.applyState.TruncatedState.Index {
		return s.applyState.TruncatedState.Term, nil
	}
	if i < s.applyState.TruncatedState.Index {
		return 0, raft.ErrCompacted
	}
	if i > s.raftState.LastIndex {
		return 0, raft.ErrUnavailable
	}

	// 读取 Log Entry 获取 Term
	key := RaftLogKey(s.region.Id, i)
	val, err := s.engine.Get(key)
	if err != nil {
		return 0, err
	}
	var ent raftpb.Entry
	proto.Unmarshal(val, &ent)
	return ent.Term, nil
}

func (s *PeerStorage) LastIndex() (uint64, error) {
	return s.raftState.LastIndex, nil
}

func (s *PeerStorage) FirstIndex() (uint64, error) {
	return s.applyState.TruncatedState.Index + 1, nil
}

func (s *PeerStorage) Snapshot() (raftpb.Snapshot, error) {
	// Multi-Raft 的 Snapshot 生成比较复杂，需要 Scan 整个 Region 的数据
	// Day 1 暂时留空或返回 ErrSnapshotTemporarilyUnavailable
	return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
}

// --- 辅助：保存状态 (供 RaftStore 循环调用) ---

// 注意：这里我们不直接写 DB，而是生成 WriteBatch 所需的 KV 对
// 因为 RaftStore 会把多个 Peer 的写入合并成一个大 Batch 写入 C++
// 这里为了演示逻辑，假设有一个 helper
func (s *PeerStorage) Append(entries []raftpb.Entry, raftState *raftpb.HardState) error {
    // 1. 保存 Entries
    for _, ent := range entries {
        key := RaftLogKey(s.region.Id, ent.Index)
        val, _ := proto.Marshal(&ent)
        s.engine.Put(key, val) // 实际上应该添加到 WriteBatch
    }
    
    // 2. 更新并保存 HardState
    if raftState != nil {
        s.raftState.Term = raftState.Term
        s.raftState.Vote = raftState.Vote
        s.raftState.Commit = raftState.Commit
    }
    
    // 更新 LastIndex
    if len(entries) > 0 {
        s.raftState.LastIndex = entries[len(entries)-1].Index
    }
    
    // 保存 RaftState
    key := RaftStateKey(s.region.Id)
    val, _ := proto.Marshal(&s.raftState)
    s.engine.Put(key, val) // 实际上添加到 WriteBatch

    return nil
}