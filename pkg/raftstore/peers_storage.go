package raftstore

import (
	"titankv/api/titankvpb"
	"titankv/pkg/store" // C++ Store

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type kvPair struct {
	key   []byte
	value []byte
}

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
		// 【修复】使用 proto.Unmarshal，传入指针
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
		// 【修复】使用 proto.Unmarshal
		if err := proto.Unmarshal(val, &s.applyState); err != nil {
			return err
		}
	}
	if s.applyState.TruncatedState == nil {
        s.applyState.TruncatedState = &titankvpb.RaftTruncatedState{
            Index: 0, // 截断位置为 0，意味着 FirstIndex 为 1
            Term:  0,
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
		// 【修复】raftpb.Entry 使用自带的 Unmarshal
		if err := ent.Unmarshal(val); err != nil {
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
	// 【修复】直接调用 Unmarshal
	ent.Unmarshal(val)
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


// pkg/raftstore/peers_storage.go

func (s *PeerStorage) Append(entries []raftpb.Entry, raftState *raftpb.HardState) ([]kvPair, error) {
    var batch []kvPair
    
    // 标记状态是否需要更新
    stateChanged := false

    // 1. 处理 Log Entries
    if len(entries) > 0 {
        for _, ent := range entries {
            key := RaftLogKey(s.region.Id, ent.Index)
            val, _ := ent.Marshal()
            batch = append(batch, kvPair{key, val})
        }
        
        // 【关键修复】更新内存中的 LastIndex
        lastIndex := entries[len(entries)-1].Index
        s.raftState.LastIndex = lastIndex
        stateChanged = true
    }
    
    // 2. 处理 HardState (Term, Vote, Commit)
    if !raft.IsEmptyHardState(*raftState) {
        s.raftState.Term = raftState.Term
        s.raftState.Vote = raftState.Vote
        s.raftState.Commit = raftState.Commit
        stateChanged = true
    }
    
    // 3. 持久化 RaftLocalState (包含 LastIndex 和 HardState)
    // 【关键修复】只要 LastIndex 变了，也必须保存，否则重启后数据丢失/状态不一致
    if stateChanged {
        key := RaftStateKey(s.region.Id)
        val, _ := proto.Marshal(&s.raftState)
        batch = append(batch, kvPair{key, val})
    }
    
    return batch, nil
}