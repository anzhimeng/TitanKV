package raftstore

import (
     "io/ioutil"
     "os"
     "fmt"

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

func (s *PeerStorage) Snapshot() (raftpb.Snapshot, error) {
    // 1. 生成快照元数据 (Index, Term, ConfState)
    // 这里的 Index 是 Applied Index
    index := s.applyState.AppliedIndex
    term := s.applyState.TruncatedState.Term // 近似值，或者查 Log
    
    // 2. 生成数据快照 (SST 文件)
    // 文件路径: /tmp/titan/snap/region_1_idx_100.sst
    fname := s.genSnapshotPath(index)
    
    // 调用 C++ 导出
    err := s.engine.DumpSST(s.region.StartKey, s.region.EndKey, fname)
    if err != nil {
        return raftpb.Snapshot{}, err
    }
    
    // 3. 返回 Snapshot 对象
    // Data 字段存放文件名，接收端根据文件名读取文件内容
    // (etcd/raft 默认 Data 是 []byte，这里我们存路径，或者把文件读进去)
    // 如果文件很大，应该通过 stream 发送，这里简化为存路径，Transport 层处理文件传输
    snap := raftpb.Snapshot{
        Metadata: raftpb.SnapshotMetadata{
            Index: index,
            Term:  term,
            ConfState: s.confState,
        },
        Data: []byte(fname), // Hack: 传路径
    }
    
    return snap, nil
}

func (s *PeerStorage) Snapshot() (raftpb.Snapshot, error) {
    // 1. 确定快照元数据
    index := s.applyState.AppliedIndex
    term := s.applyState.TruncatedState.Term
    
    // 如果没有 Term 信息（比如刚启动），尝试从 raftState 获取
    if term == 0 {
        term = s.raftState.Term
    }
    
    // 2. 准备 ConfState
    var cs raftpb.ConfState
    for _, p := range s.region.Peers {
        cs.Voters = append(cs.Voters, p.Id)
    }

    // 3. 生成 SST 文件
    // 路径：/tmp/titan_data/snap/1_100.sst (region_index)
    snapDir := s.engine.GetSnapDir() // 需要在 Store 中暴露这个路径
    if err := os.MkdirAll(snapDir, 0750); err != nil {
        return raftpb.Snapshot{}, err
    }
    
    fname := filepath.Join(snapDir, fmt.Sprintf("%d_%d.sst", s.region.Id, index))
    
    // 调用 CGO 导出
    // 注意：这里的 StartKey/EndKey 是编码后的 DataKey
    // 应该传入 DataKey(regionID, StartKey)
    start := DataKey(s.region.Id, s.region.StartKey)
    
    // 处理 EndKey 为空的情况 (无穷大)
    var end []byte
    if len(s.region.EndKey) > 0 {
        end = DataKey(s.region.Id, s.region.EndKey)
    } else {
        // 下一个 Region 的 Start
        end = DataKey(s.region.Id + 1, nil)
    }

    err := s.engine.DumpSST(start, end, fname)
    if err != nil {
        return raftpb.Snapshot{}, err
    }

    // 4. 读取文件内容放入 Snapshot (适用于小数据量)
    // 对于大数据量，应该只传 Metadata，文件通过流式传输
    // 这里采用【直接读入内存】的简化方案 (限制在几十MB以内)
    data, err := ioutil.ReadFile(fname)
    if err != nil {
        return raftpb.Snapshot{}, err
    }

    // 构造 Snapshot
    snap := raftpb.Snapshot{
        Metadata: raftpb.SnapshotMetadata{
            Index:     index,
            Term:      term,
            ConfState: cs,
        },
        Data: data, // 放入 SST 文件内容
    }

    return snap, nil
}