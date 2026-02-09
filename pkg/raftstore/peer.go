package raftstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"titankv/api/raft_serverpb"
	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
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
	regionID  uint64
	peerID    uint64
	storeID   uint64
	raftGroup *raft.RawNode
	storage   *PeerStorage
	region    *titankvpb.Region
	stopped   bool
	// 【新增】ReadIndex 状态维护
	// key: requestCtx (string), value: callback channel
	pendingReads map[string]chan uint64
	readSeq      uint64 // 用于生成唯一 ctx

	// 【新增】Applied Index (原子变量，供 Server 检查)
	// 注意：etcd/raft 内部没有这个，我们需要自己维护
	appliedIndex uint64
	applyCond    *sync.Cond
	readMu       sync.Mutex
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
	// log.Printf("[DEBUG] NewPeer %d Region %d Peers: %v", peerID, region.Id, region.Peers)
	// Bootstrap hack
	if !found && len(region.Peers) == 0 {
		peerID = 1
	}

	// 3. 配置 Raft
	c := &raft.Config{
		ID:              peerID,
		ElectionTick:    50,
		HeartbeatTick:   5,
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

	p := &Peer{
		regionID:     region.Id,
		peerID:       peerID,
		storeID:      storeID,
		raftGroup:    rn,
		storage:      ps,
		region:       region,
		pendingReads: make(map[string]chan uint64),
	}

	// 【新增】初始化 Condition Variable
	p.applyCond = sync.NewCond(&p.readMu)

	writeRegionState(p.storage.engine, region)

	return p, nil
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
		if msg.RaftCmd.AdminRequest != nil {
			req := msg.RaftCmd.AdminRequest
			if req.CmdType == titankvpb.AdminRequest_CONF_CHANGE {
				p.proposeConfChange(req.ChangePeer)
				if msg.Callback != nil {
					msg.Callback(nil)
				}
				return
			}
		}
		if msg.RaftCmd.Header != nil {
			reqEpoch := &pdpb.RegionEpoch{
				ConfVer: msg.RaftCmd.Header.RegionEpoch.ConfVer,
				Version: msg.RaftCmd.Header.RegionEpoch.Version,
			}
			if err := p.CheckEpoch(reqEpoch); err != nil {
				if msg.Callback != nil {
					msg.Callback(err) // 返回 EpochNotMatch
				}
				return
			}
		}
		// 1. 校验 Key Range
		if msg.RaftCmd.Op == titankvpb.RaftCommand_PUT || msg.RaftCmd.Op == titankvpb.RaftCommand_DELETE {
			key := msg.RaftCmd.Key
			if !p.isKeyInRange(key) {
				if msg.Callback != nil {
					msg.Callback(ErrKeyNotInRegion)
				}
				return
			}
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
	case MsgTypeReadIndex:
		p.handleReadIndex(msg)
	}
}

func (p *Peer) proposeConfChange(cp *titankvpb.ChangePeer) {
	var cc raftpb.ConfChange
	cc.Type = raftpb.ConfChangeAddNode
	if cp.ChangeType == titankvpb.ChangePeer_REMOVE_NODE {
		cc.Type = raftpb.ConfChangeRemoveNode
	}
	cc.NodeID = cp.Peer.Id
	// 将 Peer 信息存入 Context，以便 Apply 时能读出来更新元数据
	data, _ := proto.Marshal(cp.Peer)
	cc.Context = data

	p.raftGroup.ProposeConfChange(cc)
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
func (p *Peer) processEntry(entry raftpb.Entry) *Peer {
	if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
		var cmd titankvpb.RaftCommand
		if err := proto.Unmarshal(entry.Data, &cmd); err != nil {
			return nil
		}

		if cmd.Type == titankvpb.RaftCommand_NORMAL {
			p.applyNormal(&cmd)
		} else if cmd.Type == titankvpb.RaftCommand_ADMIN {
			// 【修改】调用 applyAdmin 并返回
			return p.applyAdmin(&cmd, entry.Index, entry.Term)
		}
	} else if entry.Type == raftpb.EntryConfChange {
		// Week 12 Day 1: 处理 ConfChange Apply
		var cc raftpb.ConfChange
		cc.Unmarshal(entry.Data)

		// 1. 更新 Raft 内部状态
		p.raftGroup.ApplyConfChange(cc)

		// 2. 执行实际变更 (更新元数据)
		return p.applyConfChange(cc)
	}
	return nil
}

// 处理 ConfChange Apply (Week 12 新增)
func (p *Peer) applyConfChange(cc raftpb.ConfChange) *Peer {
	var peer titankvpb.Peer
	proto.Unmarshal(cc.Context, &peer)

	region := p.region
	changed := false
	removedSelf := false

	// 1. 更新 Peers 列表
	if cc.Type == raftpb.ConfChangeAddNode {
		exists := false
		for _, existing := range region.Peers {
			if existing.Id == peer.Id {
				exists = true
				break
			}
		}
		if !exists {
			region.Peers = append(region.Peers, &peer)
			changed = true
		}
	} else if cc.Type == raftpb.ConfChangeRemoveNode {
		var newPeers []*titankvpb.Peer
		for _, existing := range region.Peers {
			if existing.Id == peer.Id {
				removedSelf = existing.Id == p.peerID
				changed = true
				continue
			}
			newPeers = append(newPeers, existing)
		}
		region.Peers = newPeers
	}

	if changed {
		if region.RegionEpoch == nil {
			region.RegionEpoch = &titankvpb.RegionEpoch{}
		}
		region.RegionEpoch.ConfVer++

		sort.Slice(region.Peers, func(i, j int) bool {
			return region.Peers[i].Id < region.Peers[j].Id
		})

		writeRegionStateAtomic(p.storage.engine, region, &p.storage.raftState)
	}

	if removedSelf {
		log.Printf("Peer %d removed from Region %d", p.peerID, p.regionID)
		tomb := &raft_serverpb.RegionLocalState{
			State:  raft_serverpb.PeerState_Tombstone,
			Region: region,
		}
		regionVal, _ := proto.Marshal(tomb)
		raftVal, _ := proto.Marshal(&p.storage.raftState)
		_ = p.storage.engine.BatchPut(
			[][]byte{RegionStateKey(p.regionID), RaftStateKey(p.regionID)},
			[][]byte{regionVal, raftVal},
		)
		p.stopped = true
		return nil
	}

	return nil
}

// 增加 engine 参数 (因为 execSplit 需要写 DB)
func (p *Peer) applyAdmin(cmd *titankvpb.RaftCommand, index, term uint64) *Peer {
	req := cmd.AdminRequest
	if req == nil {
		return nil
	}

	switch req.CmdType {
	case titankvpb.AdminRequest_SPLIT:
		log.Printf("[Apply] Executing Split at %s", string(req.Split.SplitKey))
		// 【关键】传入 engine (从 p.storage.engine 获取)
		return p.execSplit(req.Split, p.storage.engine)

	case titankvpb.AdminRequest_COMPACT:
		// Day 3 不需要实现，留空即可
		return nil
	}
	return nil
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

func (p *Peer) execSplit(req *titankvpb.SplitRequest, engine *store.TitanStore) *Peer {
	splitKey := req.SplitKey
	newRegionID := req.NewRegionId

	// 1. 校验 Key 是否在范围内
	// 如果 splitKey < StartKey 或 splitKey >= EndKey，说明已经分裂过了，或者包过时
	if bytes.Compare(splitKey, p.region.StartKey) <= 0 ||
		(len(p.region.EndKey) > 0 && bytes.Compare(splitKey, p.region.EndKey) >= 0) {
		log.Printf("Split key %s out of range, ignore", string(splitKey))
		return nil
	}

	// 2. 修改当前 Region (Region A)
	// Copy 一份，防止并发问题
	regionA := proto.Clone(p.region).(*titankvpb.Region)
	regionA.RegionEpoch.Version++
	regionA.EndKey = splitKey

	// 3. 创建新 Region (Region B)
	regionB := proto.Clone(p.region).(*titankvpb.Region)
	regionB.Id = newRegionID
	regionB.RegionEpoch.Version++
	regionB.StartKey = splitKey
	// Peers 需要替换 ID
	// 假设 req.NewPeerIds 和 region.Peers 顺序一一对应
	//for i, peer := range regionB.Peers {
	//if i < len(req.NewPeerIds) {
	//peer.Id = req.NewPeerIds[i]
	//}
	//}
	// 直接使用 Request 里的 Peers，它们包含了正确的 StoreID 映射
	regionB.Peers = req.NewPeers
	log.Printf("Split Region B Peers: %v", regionB.Peers)

	// 4. 持久化 (Meta A & Meta B)
	// 这里应该用 WriteBatch 原子写入，Day 3 简化为两次 Put
	// 写入 RegionLocalState
	writeRegionState(engine, regionA)
	writeRegionState(engine, regionB)

	// 初始化 Region B 的 Raft 初始状态 (Term=5, Index=5, etc)
	// 这样 Region B 启动后不会从 0 开始
	initRaftState(engine, regionB)

	// 5. 更新内存状态
	p.region = regionA // 更新自己的 Range

	// 6. 创建新的 Peer 对象 (Region B)
	// 找到当前 Store 对应的 PeerID
	// (逻辑同 NewPeer)
	newPeer, err := NewPeer(p.storeID, regionB, engine) // PeerStorage storeID 需要存一下
	if err != nil {
		log.Fatalf("Failed to create new peer: %v", err)
	}

	// 启动新 Peer 的 Raft
	// 对于 Split 出来的 Region，它是 Follower 还是 Leader？
	// TiKV 的做法：初始都是 Follower，由原 Leader 发起 Campaign 转移 Leadership。
	// 这里我们让它作为 Follower 启动，等待选举。

	log.Printf("Split finish. Region A: [%s, %s), Region B: [%s, %s)",
		string(regionA.StartKey), string(regionA.EndKey),
		string(regionB.StartKey), string(regionB.EndKey))

	return newPeer
}

// 辅助：持久化 RegionLocalState
func writeRegionState(engine *store.TitanStore, region *titankvpb.Region) {
	state := &raft_serverpb.RegionLocalState{
		State:  raft_serverpb.PeerState_Normal,
		Region: region,
	}
	val, _ := proto.Marshal(state)
	key := RegionStateKey(region.Id)
	engine.Put(key, val)
}

func writeRegionStateAtomic(engine *store.TitanStore, region *titankvpb.Region, raftState *titankvpb.RaftLocalState) {
	state := &raft_serverpb.RegionLocalState{
		State:  raft_serverpb.PeerState_Normal,
		Region: region,
	}
	regionVal, _ := proto.Marshal(state)
	raftVal, _ := proto.Marshal(raftState)
	keys := [][]byte{RegionStateKey(region.Id), RaftStateKey(region.Id)}
	values := [][]byte{regionVal, raftVal}
	if err := engine.BatchPut(keys, values); err != nil {
		log.Fatalf("BatchPut failed: %v", err)
	}
}

// 辅助：初始化 Raft 状态 (HardState & ApplyState)
func initRaftState(engine *store.TitanStore, region *titankvpb.Region) {
	// HardState
	hs := &raft_serverpb.RaftLocalState{
		Term: 5, Commit: 5, LastIndex: 5,
	}
	val, _ := proto.Marshal(hs)
	engine.Put(RaftStateKey(region.Id), val)

	// ApplyState
	as := &raft_serverpb.RaftApplyState{
		AppliedIndex:   5,
		TruncatedState: &raft_serverpb.RaftTruncatedState{Index: 5, Term: 5},
	}
	val2, _ := proto.Marshal(as)
	engine.Put(ApplyStateKey(region.Id), val2)
}

// 检查 Epoch 是否匹配
func (p *Peer) CheckEpoch(reqEpoch *pdpb.RegionEpoch) error {
	// 容错：如果请求没带 Epoch (旧 Client)，或者本地还没初始化好，先放行
	// 生产环境应该严格拒绝
	if reqEpoch == nil {
		return nil
	}

	current := p.region.RegionEpoch

	// 1. Version (Split/Merge)
	// 如果请求的版本比我的旧，说明 Client 路由过期
	if reqEpoch.Version < current.Version {
		return fmt.Errorf("epoch not match: version %d < %d", reqEpoch.Version, current.Version)
	}

	// 2. ConfVer (成员变更)
	if reqEpoch.ConfVer < current.ConfVer {
		return fmt.Errorf("epoch not match: conf_ver %d < %d", reqEpoch.ConfVer, current.ConfVer)
	}

	return nil
}

func (p *Peer) applySnapshot(filePath string) {
	log.Printf("[Snapshot] Ingesting SST: %s", filePath)

	// 1. 调用 C++ 导入数据 (DeleteRange 已跳过)
	err := p.storage.engine.IngestSST(filePath)
	if err != nil {
		log.Fatalf("Ingest failed: %v", err)
	}

	// 2. 删除临时文件
	os.Remove(filePath)
}

// 处理 MsgReadIndex
func (p *Peer) handleReadIndex(msg Msg) {
	// 1. 生成唯一 Context
	seq := atomic.AddUint64(&p.readSeq, 1)
	ctxBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(ctxBytes, seq)

	// 2. 记录回调
	p.pendingReads[string(ctxBytes)] = msg.ReadIndexRet

	// 3. 发给 Raft
	p.raftGroup.ReadIndex(ctxBytes)
}

// 处理 Raft Ready 中的 ReadStates
func (p *Peer) handleReadStates(states []raft.ReadState) {
	for _, rs := range states {
		ctxStr := string(rs.RequestCtx)
		if ch, ok := p.pendingReads[ctxStr]; ok {
			// 通知 Server: 这个 Index 安全了
			ch <- rs.Index
			delete(p.pendingReads, ctxStr)
			close(ch) // 关闭 channel 表示一次性通知
		}
	}
}

// 获取当前的 AppliedIndex (原子读)
func (p *Peer) GetAppliedIndex() uint64 {
	return atomic.LoadUint64(&p.appliedIndex)
}

// 设置 AppliedIndex (原子写)
func (p *Peer) SetAppliedIndex(idx uint64) {
	atomic.StoreUint64(&p.appliedIndex, idx)
	log.Printf("[Peer %d] Applied Index Updated to %d. Broadcasting...", p.peerID, idx)
	p.applyCond.Broadcast()
}

// WaitApplied 阻塞直到 AppliedIndex >= targetIndex
func (p *Peer) WaitApplied(ctx context.Context, targetIndex uint64) error {
	// 1. 快速路径：无锁检查原子变量
	if atomic.LoadUint64(&p.appliedIndex) >= targetIndex {
		return nil
	}

	// 2. 慢路径：使用 Cond 等待
	// 需要开一个 goroutine 转换 Cond 阻塞为 Channel 信号，以便 select 处理 context timeout
	doneC := make(chan struct{})

	go func() {
		p.readMu.Lock()
		defer p.readMu.Unlock()

		// 循环检查，防止虚假唤醒
		for atomic.LoadUint64(&p.appliedIndex) < targetIndex {
			// 【调试】打印等待状态
			//log.Printf("[Peer %d] Waiting Applied: Current %d < Target %d",
			//p.peerID, atomic.LoadUint64(&p.appliedIndex), targetIndex)
			p.applyCond.Wait() // 释放锁并挂起
		}
		// 【调试】
		//log.Printf("[Peer %d] Wait Finished! Current %d >= Target %d",
		//p.peerID, atomic.LoadUint64(&p.appliedIndex), targetIndex)
		close(doneC)
	}()

	select {
	case <-doneC:
		return nil
	case <-ctx.Done():
		// 注意：goroutine 会泄露直到下一次 Broadcast，这是 Go Cond 的已知限制。
		// 但对于短连接或频繁更新的 Apply，这是可接受的。
		return ctx.Err()
	}
}
