package raftstore

import (
	"log"
	"titankv/api/titankvpb"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

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

// 处理消息
func (p *Peer) step(msg Msg) {
	switch msg.Type {
	case MsgTypeRaftMessage:
		// 反序列化 raftpb.Message
		var rMsg raftpb.Message
		rMsg.Unmarshal(msg.RaftMessage.Data)
		p.raftGroup.Step(rMsg)

	case MsgTypeRaftCmd:
		// Propose
		// data, _ := proto.Marshal(msg.RaftCmd)
		// p.raftGroup.Propose(data)
        
    case MsgTypeTick:
        p.raftGroup.Tick()
	}
}

// 检查是否有 Ready
func (p *Peer) hasReady() bool {
    return p.raftGroup.HasReady()
}