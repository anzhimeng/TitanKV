package raftstore

import (
	"bytes"
	"errors"
	"log"
	"titankv/api/titankvpb"
	"titankv/pkg/store"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	ErrKeyNotInRegion = errors.New("key not in region")
	ErrEpochNotMatch  = errors.New("epoch not match")
)

type Peer struct {
	regionID   uint64
	peerID     uint64
	raftGroup  *raft.RawNode
	storage    *PeerStorage
	region     *titankvpb.Region
}

func NewPeer(storeID uint64, region *titankvpb.Region, engine *store.TitanStore) (*Peer, error) {
	// 1. 初始化 PeerStorage
	ps, err := NewPeerStorage(engine, region)
	if err != nil {
		return nil, err
	}

	// 2. 查找 PeerID
	var peerID uint64
	found := false
	for _, p := range region.Peers {
		if p.StoreId == storeID {
			peerID = p.Id
			found = true
			break
		}
	}
	// Bootstrap hack
	if !found && len(region.Peers) == 0 {
		peerID = 1 
	}

	// 3. 配置 Raft
	c := &raft.Config{
		ID:              peerID,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         ps,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
		CheckQuorum:     true,
	}

	// 4. 创建 RawNode (v3.5+ 不需要 peers 参数)
	rn, err := raft.NewRawNode(c)
	if err != nil {
		return nil, err
	}

	return &Peer{
		regionID:  region.Id,
		peerID:    peerID,
		raftGroup: rn,
		storage:   ps,
		region:    region,
	}, nil
}

// 处理消息
func (p *Peer) step(msg Msg) {
	switch msg.Type {
	case MsgTypeRaftMessage:
		var rMsg raftpb.Message
		if err := rMsg.Unmarshal(msg.RaftMessage.Data); err == nil {
			p.raftGroup.Step(rMsg)
		}

	case MsgTypeRaftCmd:
		// 1. 校验 Key Range
		key := msg.RaftCmd.Key
		if !p.isKeyInRange(key) {
			if msg.Callback != nil {
				msg.Callback(ErrKeyNotInRegion)
			}
			return
		}

		// 2. 提交给 Raft
		data, _ := proto.Marshal(msg.RaftCmd)
		// 注意：Callback 的处理需要 Proposal 追踪机制，Day 5 简化为立即回调 nil (表示已排队)
		// 或者留给 Apply 阶段回调 (需更复杂实现)
		if msg.Callback != nil {
			msg.Callback(nil) 
		}
		p.raftGroup.Propose(data)

	case MsgTypeTick:
		p.raftGroup.Tick()
	}
}

func (p *Peer) isKeyInRange(key []byte) bool {
	start := p.region.StartKey
	end := p.region.EndKey
	if len(start) > 0 && bytes.Compare(key, start) < 0 {
		return false
	}
	if len(end) > 0 && bytes.Compare(key, end) >= 0 {
		return false
	}
	return true
}

func (p *Peer) hasReady() bool {
	return p.raftGroup.HasReady()
}

// 应用日志
func (p *Peer) processEntry(entry raftpb.Entry) {
	if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
		var cmd titankvpb.RaftCommand
		if err := proto.Unmarshal(entry.Data, &cmd); err != nil {
			log.Printf("Failed to unmarshal raft cmd: %v", err)
			return
		}

		// 分发 Normal 和 Admin 请求
		if cmd.Type == titankvpb.RaftCommand_NORMAL {
			p.applyNormal(&cmd)
		} else if cmd.Type == titankvpb.RaftCommand_ADMIN {
			p.applyAdmin(&cmd, entry.Index, entry.Term)
		} else {
            // 兼容旧代码（Week 10 之前没有 Type 字段，默认为 NORMAL）
            // 如果你之前的 Put/Delete 逻辑没设置 Type，这里会进 else。
            // 建议：直接视为 Normal。
            p.applyNormal(&cmd)
        }

	} else if entry.Type == raftpb.EntryConfChange {
		// Week 12 Day 1 才会用到
		var cc raftpb.ConfChange
		cc.Unmarshal(entry.Data)
		p.raftGroup.ApplyConfChange(cc)
	}
}

func (p *Peer) applyAdmin(cmd *titankvpb.RaftCommand, index, term uint64) {
	req := cmd.AdminRequest
    if req == nil { return }

	switch req.CmdType {
	case titankvpb.AdminRequest_SPLIT:
		// Day 3 的重头戏：执行分裂！
        // 此时 Raft Log 已经提交，所有副本都会走到这里。
		log.Printf("[Apply] Split command committed! Region: %d, SplitKey: %s, NewRegionID: %d", 
            p.regionID, string(req.Split.SplitKey), req.Split.NewRegionId)
        
        // TODO (Week 11 Day 3): p.execSplit(req.Split)
        
	case titankvpb.AdminRequest_COMPACT:
		// 处理 Log Compaction 请求
	}
}

func (p *Peer) applyNormal(cmd *titankvpb.RaftCommand) {
	// Key Encoding: z{RegionID}_{UserKey}
	encodedKey := DataKey(p.regionID, cmd.Key)
	
	if cmd.Op == titankvpb.RaftCommand_PUT {
		p.storage.engine.Put(encodedKey, cmd.Value)
	} else if cmd.Op == titankvpb.RaftCommand_DELETE {
		p.storage.engine.Delete(encodedKey)
	}
}