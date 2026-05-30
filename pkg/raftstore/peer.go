package raftstore

import (
	"bytes"
	"context"
	"crypto/rand"
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
const raftCmdBatchPrefix byte = 1

type Peer struct {
	regionID  uint64
	peerID    uint64
	storeID   uint64
	raftGroup *raft.RawNode
	storage   *PeerStorage
	region    *titankvpb.Region
	mu        sync.RWMutex // Protects region and other mutable fields
	stopped   bool
	// 【新增】ReadIndex 状态维护
	// key: requestCtx (string), value: list of callback channels
	pendingReads map[string][]chan uint64
	readSeq      uint64 // 用于生成唯一 ctx

	// 【新增】Applied Index (原子变量，供 Server 检查)
	// 注意：etcd/raft 内部没有这个，我们需要自己维护
	appliedIndex  uint64
	applyNotifyCh chan struct{}
	readMu        sync.Mutex
	// Pending Proposals (UUID -> Callback)
	pendingProposals sync.Map
	// 【新增】Pending Merge Target (用于 Source Region 在停止后通知 Target)
	pendingMerge *titankvpb.MergeRequest
	// 【新增】Last Active Tick for Merge Idle Check
	lastActiveTick uint64
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
		MaxSizePerMsg:   4 * 1024 * 1024,
		MaxInflightMsgs: 4096,
		CheckQuorum:     true,
		PreVote:         true,
	}

	// 4. 创建 RawNode (v3.5+ 不需要 peers 参数)
	rn, err := raft.NewRawNode(c)
	if err != nil {
		return nil, err
	}

	p := &Peer{
		regionID:      region.Id,
		peerID:        peerID,
		storeID:       storeID,
		raftGroup:     rn,
		storage:       ps,
		region:        region,
		pendingReads:  make(map[string][]chan uint64),
		applyNotifyCh: make(chan struct{}),
	}

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

	case MsgTypeRaftMessageBatch:
		for _, m := range msg.RaftMessages {
			var rMsg raftpb.Message
			if err := rMsg.Unmarshal(m.Data); err == nil {
				p.raftGroup.Step(rMsg)
			}
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
		if msg.RaftCmd.Header != nil && msg.RaftCmd.Header.RegionEpoch != nil {
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
		p.handleReadIndexBatch([]Msg{msg})
	}
}

func (p *Peer) stepBatchCmds(cmds []*titankvpb.RaftCommand, callbacks []func(error)) {
	if len(cmds) == 0 {
		return
	}
	accepted := make([]*titankvpb.RaftCommand, 0, len(cmds))
	for i, cmd := range cmds {
		var cb func(error)
		if i < len(callbacks) {
			cb = callbacks[i]
		}
		if cmd == nil {
			if cb != nil {
				cb(errors.New("nil command"))
			}
			continue
		}
		if cmd.AdminRequest != nil {
			req := cmd.AdminRequest
			if req.CmdType == titankvpb.AdminRequest_CONF_CHANGE {
				p.proposeConfChange(req.ChangePeer)
				if cb != nil {
					cb(nil)
				}
				continue
			}
		}
		if cmd.Header != nil && cmd.Header.RegionEpoch != nil {
			reqEpoch := &pdpb.RegionEpoch{
				ConfVer: cmd.Header.RegionEpoch.ConfVer,
				Version: cmd.Header.RegionEpoch.Version,
			}
			if err := p.CheckEpoch(reqEpoch); err != nil {
				if cb != nil {
					cb(err)
				}
				continue
			}
		}
		if cmd.Op == titankvpb.RaftCommand_PUT || cmd.Op == titankvpb.RaftCommand_DELETE {
			key := cmd.Key
			if !p.isKeyInRange(key) {
				if cb != nil {
					cb(ErrKeyNotInRegion)
				}
				continue
			}
		}

		// 生成 UUID 并注册 Callback
		uuid := make([]byte, 16)
		rand.Read(uuid)
		if cmd.Header == nil {
			cmd.Header = &titankvpb.RaftRequestHeader{}
		}
		cmd.Header.Uuid = uuid

		if cb != nil {
			p.pendingProposals.Store(string(uuid), cb)
		}

		accepted = append(accepted, cmd)
	}
	if len(accepted) == 0 {
		return
	}
	batch := &titankvpb.BatchRaftCommand{Commands: accepted}
	data, _ := proto.Marshal(batch)
	data = append([]byte{raftCmdBatchPrefix}, data...)
	p.raftGroup.Propose(data)
}

func (p *Peer) stepBatch(msgs []Msg) {
	if len(msgs) == 0 {
		return
	}

	// 1. 分离 ReadIndex 请求和 RaftCmd 请求
	var readIndexMsgs []Msg
	var raftCmdMsgs []Msg

	for _, msg := range msgs {
		if msg.Type == MsgTypeReadIndex {
			readIndexMsgs = append(readIndexMsgs, msg)
		} else if msg.Type == MsgTypeRaftCmd {
			raftCmdMsgs = append(raftCmdMsgs, msg)
		}
	}

	// 2. 批量处理 ReadIndex
	if len(readIndexMsgs) > 0 {
		p.handleReadIndexBatch(readIndexMsgs)
	}

	// 3. 批量处理 RaftCmd
	if len(raftCmdMsgs) == 0 {
		return
	}

	cmds := make([]*titankvpb.RaftCommand, 0, len(raftCmdMsgs))
	for _, msg := range raftCmdMsgs {
		if msg.RaftCmd == nil {
			continue
		}
		if msg.RaftCmd.AdminRequest != nil {
			req := msg.RaftCmd.AdminRequest
			if req.CmdType == titankvpb.AdminRequest_CONF_CHANGE {
				p.proposeConfChange(req.ChangePeer)
				if msg.Callback != nil {
					msg.Callback(nil)
				}
				continue
			}
		}
		if msg.RaftCmd.Header != nil {
			reqEpoch := &pdpb.RegionEpoch{
				ConfVer: msg.RaftCmd.Header.RegionEpoch.ConfVer,
				Version: msg.RaftCmd.Header.RegionEpoch.Version,
			}
			if err := p.CheckEpoch(reqEpoch); err != nil {
				if msg.Callback != nil {
					msg.Callback(err)
				}
				continue
			}
		}
		if msg.RaftCmd.Type == titankvpb.RaftCommand_NORMAL && (msg.RaftCmd.Op == titankvpb.RaftCommand_PUT || msg.RaftCmd.Op == titankvpb.RaftCommand_DELETE) {
			key := msg.RaftCmd.Key
			if !p.isKeyInRange(key) {
				if msg.Callback != nil {
					msg.Callback(ErrKeyNotInRegion)
				}
				continue
			}
		}

		// 生成 UUID 并注册 Callback
		uuid := make([]byte, 16)
		rand.Read(uuid)
		if msg.RaftCmd.Header == nil {
			msg.RaftCmd.Header = &titankvpb.RaftRequestHeader{}
		}
		msg.RaftCmd.Header.Uuid = uuid

		if msg.Callback != nil {
			p.pendingProposals.Store(string(uuid), msg.Callback)
		}

		cmds = append(cmds, msg.RaftCmd)
	}

	if len(cmds) == 0 {
		return
	}

	batch := &titankvpb.BatchRaftCommand{Commands: cmds}
	data, _ := proto.Marshal(batch)
	data = append([]byte{raftCmdBatchPrefix}, data...)
	p.raftGroup.Propose(data)
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

func (p *Peer) handleReadIndexBatch(msgs []Msg) {
	if len(msgs) == 0 {
		return
	}

	// 收集所有的 Callback Channels
	callbacks := make([]chan uint64, 0, len(msgs))
	for _, msg := range msgs {
		if msg.ReadIndexRet != nil {
			callbacks = append(callbacks, msg.ReadIndexRet)
		}
	}
	if len(callbacks) == 0 {
		return
	}

	p.readMu.Lock()
	p.readSeq++
	ctx := fmt.Sprintf("%d-%d", p.peerID, p.readSeq)
	p.pendingReads[ctx] = callbacks
	p.readMu.Unlock()

	p.raftGroup.ReadIndex([]byte(ctx))
}

func (p *Peer) handleReadIndex(msg Msg) {
	p.handleReadIndexBatch([]Msg{msg})
}

func (p *Peer) isKeyInRange(key []byte) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	startKey := p.region.StartKey
	endKey := p.region.EndKey
	return bytes.Compare(key, startKey) >= 0 &&
		(len(endKey) == 0 || bytes.Compare(key, endKey) < 0)
}

func (p *Peer) hasReady() bool {
	return p.raftGroup.HasReady()
}

func (p *Peer) notifyCallback(cmd *titankvpb.RaftCommand, err error) {
	if cmd.Header == nil || len(cmd.Header.Uuid) == 0 {
		return
	}
	uuid := string(cmd.Header.Uuid)
	if val, ok := p.pendingProposals.Load(uuid); ok {
		if cb, ok := val.(func(error)); ok {
			cb(err)
		}
		p.pendingProposals.Delete(uuid)
	}
}

func (p *Peer) applyTxn(cmd *titankvpb.RaftCommand) error {
	if cmd.PrewriteRequest != nil {
		req := cmd.PrewriteRequest
		if req.Use_1Pc {
			return p.storage.engine.Prewrite1PC(req.Mutations, req.PrimaryKey, req.StartTs, req.CommitTs, req.LockTtl)
		}
		if req.UseAsyncCommit {
			isPessimistic := false
			if len(req.IsPessimisticLock) > 0 {
				isPessimistic = req.IsPessimisticLock[0]
			}
			return p.storage.engine.PrewriteAsync(req.Mutations, req.PrimaryKey, req.StartTs, req.LockTtl, req.MinCommitTs, isPessimistic, req.Secondaries)
		}
		return p.storage.engine.Prewrite(req.Mutations, req.PrimaryKey, req.StartTs, req.LockTtl)
	}
	if cmd.CommitRequest != nil {
		req := cmd.CommitRequest
		return p.storage.engine.Commit(req.Keys, req.StartTs, req.CommitTs)
	}
	if cmd.ResolveLockRequest != nil {
		req := cmd.ResolveLockRequest
		return p.storage.engine.Commit(req.Keys, req.StartTs, req.CommitTs)
	}
	if cmd.AcquirePessimisticLockRequest != nil {
		req := cmd.AcquirePessimisticLockRequest
		// log.Printf("[Peer] Applying AcquirePessimisticLock: %v", req.Mutations[0].Key)
		keys := make([][]byte, 0, len(req.Mutations))
		for _, m := range req.Mutations {
			keys = append(keys, m.Key)
		}
		_, _, err := p.storage.engine.AcquirePessimisticLock(keys, req.PrimaryKey, req.StartTs, req.LockTtl, req.ForUpdateTs, req.ReturnValues)
		return err
	}
	return nil
}

func (p *Peer) processEntry(entry raftpb.Entry) *Peer {
	if entry.Type == raftpb.EntryConfChange {
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(entry.Data); err == nil {
			p.applyConfChange(cc)
		}
		return nil
	}

	if len(entry.Data) == 0 {
		return nil
	}

	// 解码 Raft Command
	var cmd titankvpb.RaftCommand
	if entry.Data[0] == raftCmdBatchPrefix {
		// 批量命令
		var batch titankvpb.BatchRaftCommand
		if err := proto.Unmarshal(entry.Data[1:], &batch); err == nil {
			for _, c := range batch.Commands {
				if c.AdminRequest != nil {
					if split := p.applyAdmin(c, entry.Index, entry.Term); split != nil {
						return split
					}
				} else {
					p.applyNormal(c)
				}
				p.notifyCallback(c, nil)
			}
		}
	} else {
		// 单条命令
		if err := proto.Unmarshal(entry.Data, &cmd); err == nil {
			if cmd.AdminRequest != nil {
				if split := p.applyAdmin(&cmd, entry.Index, entry.Term); split != nil {
					return split
				}
			} else {
				p.applyNormal(&cmd)
			}
			p.notifyCallback(&cmd, nil)
		}
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

func (p *Peer) ApplyCommittedEntries(entries []raftpb.Entry) []*Peer {
	var newPeers []*Peer
	
	// Track the highest applied index in this batch
	finalApplied := p.appliedIndex

	// Batch buffers
	var keys [][]byte
	var values [][]byte
	var ops []int
	var callbacks []func()

	// Flush current batch to storage
	flush := func() {
		if len(keys) == 0 {
			return
		}
		if err := p.storage.engine.BatchWriteOps(keys, values, ops); err != nil {
			log.Fatalf("Batch apply failed: %v", err)
		}
		// Notify callbacks after successful write
		for _, cb := range callbacks {
			if cb != nil {
				cb()
			}
		}
		// Reset buffers
		keys = keys[:0]
		values = values[:0]
		ops = ops[:0]
		callbacks = callbacks[:0]
	}

	for _, entry := range entries {
		// Update applied index for every entry (even empty ones)
		if entry.Index > finalApplied {
			finalApplied = entry.Index
		}

		// 1. Handle Empty Entry (e.g. Leader Election)
		if len(entry.Data) == 0 && entry.Type != raftpb.EntryConfChange {
			flush() // Flush any pending writes
			continue
		}

		// 2. Handle ConfChange
		if entry.Type == raftpb.EntryConfChange {
			flush()
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(entry.Data); err == nil {
				p.applyConfChange(cc)
			}
			if p.stopped {
				break
			}
			continue
		}

		// 3. Handle Normal Entry
		var cmds []*titankvpb.RaftCommand
		if entry.Data[0] == raftCmdBatchPrefix {
			var batch titankvpb.BatchRaftCommand
			if err := proto.Unmarshal(entry.Data[1:], &batch); err == nil {
				cmds = batch.Commands
			}
		} else {
			var cmd titankvpb.RaftCommand
			if err := proto.Unmarshal(entry.Data, &cmd); err == nil {
				cmds = []*titankvpb.RaftCommand{&cmd}
			}
		}

		for _, cmd := range cmds {
			if cmd.AdminRequest != nil {
				flush()
				if splitPeer := p.applyAdmin(cmd, entry.Index, entry.Term); splitPeer != nil {
					newPeers = append(newPeers, splitPeer)
				}
				p.notifyCallback(cmd, nil)
			} else if cmd.PrewriteRequest != nil || cmd.CommitRequest != nil || cmd.ResolveLockRequest != nil || cmd.AcquirePessimisticLockRequest != nil {
				// Transaction Commands
				flush()
				err := p.applyTxn(cmd)
				p.notifyCallback(cmd, err)
			} else {
				// Batch Put/Delete
				encodedKey := DataKey(p.regionID, cmd.Key)
				if cmd.Op == titankvpb.RaftCommand_PUT {
					keys = append(keys, encodedKey)
					values = append(values, cmd.Value)
					ops = append(ops, 0) // 0: Put
				} else if cmd.Op == titankvpb.RaftCommand_DELETE {
					keys = append(keys, encodedKey)
					values = append(values, nil)
					ops = append(ops, 1) // 1: Delete
				}
				
				// Capture callback
				c := cmd
				callbacks = append(callbacks, func() {
					p.notifyCallback(c, nil)
				})
			}
		}
	}
	
	// Flush remaining
	flush()
	
	// Update AppliedIndex atomically and notify waiters
	if finalApplied > p.appliedIndex {
		p.SetAppliedIndex(finalApplied)
	}

	return newPeers
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

	case titankvpb.AdminRequest_MERGE:
		log.Printf("[Apply] Executing Merge Source=%d Target=%d", req.Merge.SourceRegion.Id, req.Merge.TargetRegionId)
		return p.execMerge(req.Merge, p.storage.engine)
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

func (p *Peer) execMerge(req *titankvpb.MergeRequest, engine *store.TitanStore) *Peer {
	// 区分 Source (被合并) 和 Target (合并目标)
	if p.regionID == req.SourceRegion.Id {
		// I am Source
		log.Printf("[Merge] Region %d (Source) merging into %d", p.regionID, req.TargetRegionId)

		// 1. 记录 Pending Merge，供 StoreWorker 通知 Target
		p.pendingMerge = req

		// 2. 标记停止 (Self-destruct)
		// 注意：Tombstone 持久化逻辑在 removePeer 中处理
		p.stopped = true

		return nil
	} else if p.regionID == req.TargetRegionId {
		// I am Target
		log.Printf("[Merge] Region %d (Target) absorbing %d", p.regionID, req.SourceRegion.Id)

		// 1. 扩展 Range
		// 校验 Source 是否相邻
		if bytes.Equal(p.region.StartKey, req.SourceRegion.EndKey) {
			// Source 在前: [Source][Target] -> [Source+Target]
			p.region.StartKey = req.SourceRegion.StartKey
		} else if bytes.Equal(p.region.EndKey, req.SourceRegion.StartKey) {
			// Source 在后: [Target][Source] -> [Target+Source]
			p.region.EndKey = req.SourceRegion.EndKey
		} else {
			// 不连续，忽略 (可能已经 Split 或其他并发变更)
			log.Printf("[Merge] Source %d not adjacent to Target %d, ignore", req.SourceRegion.Id, p.regionID)
			return nil
		}

		// 2. 更新 Epoch
		if p.region.RegionEpoch == nil {
			p.region.RegionEpoch = &titankvpb.RegionEpoch{}
		}
		p.region.RegionEpoch.Version++
		// ConfVer 也可以增加，表示拓扑变更

		// 3. Prepare Batch
		var keys [][]byte
		var values [][]byte

		// Data
		if len(req.Data) > 0 {
			for _, kv := range req.Data {
				// Encode Key: z{TargetID}{UserKey}
				encodedKey := DataKey(p.regionID, kv.Key)
				keys = append(keys, encodedKey)
				values = append(values, kv.Value)
			}
		}

		// Metadata: RegionState
		state := &raft_serverpb.RegionLocalState{
			State:  raft_serverpb.PeerState_Normal,
			Region: p.region,
		}
		regionVal, _ := proto.Marshal(state)
		keys = append(keys, RegionStateKey(p.region.Id))
		values = append(values, regionVal)

		// Metadata: RaftState
		raftVal, _ := proto.Marshal(&p.storage.raftState)
		keys = append(keys, RaftStateKey(p.region.Id))
		values = append(values, raftVal)

		// Atomic Write
		if err := engine.BatchPut(keys, values); err != nil {
			log.Fatalf("[Merge] Atomic BatchPut failed: %v", err)
		}
		
		log.Printf("[Merge] Ingested %d keys from Source %d", len(req.Data), req.SourceRegion.Id)
		
		return nil
	}
	return nil
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

func (p *Peer) CheckEpoch(req *pdpb.RegionEpoch) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if req.ConfVer < p.region.RegionEpoch.ConfVer || req.Version < p.region.RegionEpoch.Version {
		return ErrEpochNotMatch
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

// 处理 Raft Ready 中的 ReadStates
func (p *Peer) handleReadStates(states []raft.ReadState) {
	for _, rs := range states {
		ctxStr := string(rs.RequestCtx)
		
		p.readMu.Lock()
		chs, ok := p.pendingReads[ctxStr]
		if ok {
			delete(p.pendingReads, ctxStr)
		}
		p.readMu.Unlock()
		
		if ok {
			// 通知: Raft 确认该 Index 安全
			for _, ch := range chs {
				select {
				case ch <- rs.Index:
				default:
				}
			}
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
	p.readMu.Lock()
	close(p.applyNotifyCh)
	p.applyNotifyCh = make(chan struct{})
	p.readMu.Unlock()
}

func (p *Peer) GetRegion() *titankvpb.Region {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.region
}

// WaitApplied 阻塞直到 AppliedIndex >= targetIndex
func (p *Peer) WaitApplied(ctx context.Context, targetIndex uint64) error {
	// 1. 快速路径：无锁检查原子变量
	if atomic.LoadUint64(&p.appliedIndex) >= targetIndex {
		return nil
	}

	for {
		p.readMu.Lock()
		if atomic.LoadUint64(&p.appliedIndex) >= targetIndex {
			p.readMu.Unlock()
			return nil
		}
		ch := p.applyNotifyCh
		p.readMu.Unlock()

		select {
		case <-ch:
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
