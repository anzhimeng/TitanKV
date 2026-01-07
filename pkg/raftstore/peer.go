package raftstore

import (
	"log"
	"titankv/api/titankvpb"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

var ErrKeyNotInRegion = errors.New("key not in region")
var ErrEpochNotMatch = errors.New("epoch not match")

type Peer struct {
	regionID   uint64
	peerID     uint64
	raftGroup  *raft.RawNode // 核心：使用的是 RawNode，不是 Node
	storage    *PeerStorage  // Day 1 实现的
}

func NewPeer(storeID uint64, region *titankvpb.Region) (*Peer, error) {
	// 1. 初始化 Storage
	// 需要传入全局 DB 引擎，这里假设外部传入了 engine
	// ps, err := NewPeerStorage(engine, region) 
    // 为了简化，构造函数参数先留空，实际集成时补上
	
	// 假设 storage 已经准备好了
	var ps *PeerStorage 

	// 2. 配置 Raft
	c := &raft.Config{
		ID:              1, // 需要从 region.Peers 中找到属于当前 store 的 peerID
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         ps,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
		CheckQuorum:     true,
	}

	// 3. 创建 RawNode
	// 如果是新建 Region (peers==1)，需要 initial peers；如果是重启，不需要
	rn, err := raft.NewRawNode(c)
	if err != nil {
		return nil, err
	}

	return &Peer{
		regionID:  region.Id,
		raftGroup: rn,
		storage:   ps,
	}, nil
}

func (p *Peer) step(msg Msg) {
    switch msg.Type {
    case MsgTypeRaftCmd:
        // 1. 校验 Epoch (Week 9 的逻辑移到这里)
        // ... (省略 Epoch 检查代码) ...

        // 2. 校验 Key Range
        // Put/Delete/Get 都需要校验
        key := msg.RaftCmd.Key
        if !p.isKeyInRange(key) {
            if msg.Callback != nil {
                msg.Callback(ErrKeyNotInRegion)
            }
            return
        }

        // 3. 提交给 Raft
        data, _ := proto.Marshal(msg.RaftCmd)
        // 这里需要把 Callback 存起来，等 Apply 的时候调用
        // 这涉及到 "Proposal 追踪"，比较复杂。
        // Day 3 简化版：我们假设 Propose 成功就是成功 (虽然不严谨)，
        // 或者直接在这里 callback(nil) 表示 "已提交队列"。
        // 真正的做法是：ProposalContext 携带一个 UUID，Apply 时根据 UUID 找 Callback。
        
        // 为了跑通流程，我们先在这里 callback
        // Week 10 Day 4 会完善 Apply 流程
        p.raftGroup.Propose(data)
        // 注意：Callback 应该在 Apply 后调用，这里先暂存 TODO
    }
}

// 辅助：检查 Key 是否在 Region 范围内 [Start, End)
func (p *Peer) isKeyInRange(key []byte) bool {
    start := p.region.StartKey
    end := p.region.EndKey
    
    // Start <= Key
    if len(start) > 0 && bytes.Compare(key, start) < 0 {
        return false
    }
    // Key < End (End 为空表示无穷大)
    if len(end) > 0 && bytes.Compare(key, end) >= 0 {
        return false
    }
    return true
}

// 检查是否有 Ready
func (p *Peer) hasReady() bool {
    return p.raftGroup.HasReady()
}