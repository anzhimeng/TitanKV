package raftstore

import (
	"log"
	"time"
	"context"
	"encoding/binary"
	
	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
	"titankv/pkg/store" // C++ 引擎

	"google.golang.org/protobuf/proto"
	"go.etcd.io/etcd/raft/v3"
)

const (
	batchSize = 128
	// Region 最大阈值 (96MB)
     MaxRegionSize = /*96*/1 * 1024 * 1024 
     // 检查间隔 (每隔多少次 Tick 检查一次，避免频繁调用 CGO)
     SplitCheckInterval = 10 
     PDHeartbeatTickInterval = 50
)

type MsgAddPeer struct {
	Peer *Peer
}

type StoreWorker struct {
	peers        map[uint64]*Peer
	receiver     PeerSender
	router       *Router
	pendingPeers map[uint64]*Peer
	store        *store.TitanStore
	transport    *Transport // 【新增】持有 Transport
	// 【新增】PD Client
	pdClient pdpb.PDClient
	
	tickCount uint64
	ctx    context.Context
     cancel context.CancelFunc
}

func NewStoreWorker(router *Router, trans *Transport, s *store.TitanStore, client pdpb.PDClient) *StoreWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &StoreWorker{
		peers:        make(map[uint64]*Peer),
		receiver:     make(PeerSender, 4096),
		router:       router,
		pendingPeers: make(map[uint64]*Peer),
		store:        s,
		transport:    trans, // 赋值
		pdClient:     client, // 赋值
		ctx:    ctx,
          cancel: cancel,
	}
}

func (w *StoreWorker) Stop() {
    w.cancel()
}

func (w *StoreWorker) Receiver() PeerSender {
	return w.receiver
}

func (w *StoreWorker) Run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	msgs := make([]Msg, 0, batchSize)

	for {
		select {
		case msg := <-w.receiver:
			msgs = append(msgs, msg)
		case <-ticker.C:
			w.onTick()
		case <-w.ctx.Done(): // 【新增】退出信号
            return
		}

		pending := len(w.receiver)
		if pending > batchSize {
			pending = batchSize
		}
		for i := 0; i < pending; i++ {
			msgs = append(msgs, <-w.receiver)
		}

		for _, msg := range msgs {
			w.processMsg(msg)
		}
		msgs = msgs[:0]

		w.handleReady()
	}
}

func (w *StoreWorker) processMsg(msg Msg) {
	// 【修改】优先处理不需要 Peer 上下文的消息 (如创建 Peer)
	// 如果以后有 MsgCreatePeer，放在这里
	
	// 获取 Peer
	peer, ok := w.peers[msg.RegionID]
	if !ok {
		// Region 不存在，可能是已经被 Split 移除，或者发错了
		// log.Printf("[Worker] Msg %v for non-existent region %d", msg.Type, msg.RegionID)
		return
	}

	// 【Day 2 新增】处理 SplitCheck
	if msg.Type == MsgTypeSplitCheck {
		w.onSplitCheck(msg.RegionID)
		return
	}

	// 处理其他消息 (RaftMessage, RaftCmd, Tick)
	peer.step(msg)
	
	// 标记为活跃，以便后续 handleReady 处理 IO
	w.pendingPeers[msg.RegionID] = peer
}
func (w *StoreWorker) onTick() {
    w.tickCount++
    
    for _, peer := range w.peers {
    	   if peer == nil { continue }
        // 1. Raft Tick
        peer.step(NewMsgTick())
        w.pendingPeers[peer.regionID] = peer

        // 2. 【新增】PD Heartbeat
        // 只有 Leader 发心跳
        if peer.raftGroup.Status().Lead == peer.peerID {
            // 使用简单的取模来决定发送频率
            if w.tickCount % PDHeartbeatTickInterval == 0 {
                w.sendRegionHeartbeat(peer)
            }
        }
    }
    // 2. Split Check (低频执行)
    if w.tickCount % SplitCheckInterval == 0 {
        w.checkSplit()
    }
}


func (w *StoreWorker) handleReady() {
	var messages []*titankvpb.RaftMessage
	var readyPeers []*Peer
	
	// CGO Batch Write 需要的切片
	var batchKeys [][]byte
	var batchValues [][]byte

	for _, peer := range w.pendingPeers {
	     if peer == nil { 
            continue 
          }
		if !peer.hasReady() {
			continue
		}
		
		rd := peer.raftGroup.Ready()
		if len(rd.ReadStates) > 0 {
            peer.handleReadStates(rd.ReadStates)
        	}
		// A. 收集日志和状态 (WAL)
		// peer.storage.Append 返回 []kvPair
		// 【新增】处理 Snapshot
          if !raft.IsEmptySnap(rd.Snapshot) {
          	// 1. 获取文件路径
          	filePath := string(rd.Snapshot.Data)
            
            	// 2. 调用 Peer 导入数据
            	peer.applySnapshot(filePath)
            
            	// 3. 调用 Storage 应用元数据 (Week 4 已有)
            	peer.storage.ApplySnapshot(rd.Snapshot)
        	}
		kvPairs, err := peer.storage.Append(rd.Entries, &rd.HardState)
		if err != nil {
			log.Fatalf("Append failed: %v", err)
		}
		
		// 拆解 kvPair 到两个切片
		for _, kv := range kvPairs {
			batchKeys = append(batchKeys, kv.key)
			batchValues = append(batchValues, kv.value)
		}
		
		// B. 收集网络消息
		for _, msg := range rd.Messages {
			// 查找目标 Peer 的 StoreID
	          toStoreId := uint64(0)
	          for _, p := range peer.region.Peers {
	              if p.Id == msg.To {
	                  toStoreId = p.StoreId
	                  break
	              }
	          }
	          // 如果找不到（可能是发给正在 Remove 的节点，或者是新加入的节点还未更新 Peers），
	          // 这种情况下通常忽略，或者尝试广播（不推荐）。
	          // 对于 AddPeer，我们在 ConfChange 时已经把新 Peer 加到 Region.Peers 了，所以能找到。
	          if toStoreId == 0 {
	              // log.Printf("Peer %d not found in region %d", msg.To, peer.regionID)
	              continue 
	          }
			data, _ := msg.Marshal()
			tm := &titankvpb.RaftMessage{
				RegionId:   peer.regionID,
				FromPeerId: msg.From,
				ToPeerId:   msg.To,
				ToStoreId:  toStoreId,
				Data:       data,
			}

			messages = append(messages, tm)
		}
		
		readyPeers = append(readyPeers, peer)
	}

	// 2. 执行阶段：批量写盘 (Atomic & Batch)
	if len(batchKeys) > 0 {
		// 调用 CGO BatchPut
		err := w.store.BatchPut(batchKeys, batchValues)
		if err != nil {
			 log.Fatalf("BatchPut failed: %v", err)
		}
	}

	// 3. 执行阶段：批量发送
	if len(messages) > 0 {
		w.transport.Send(messages)
	}

	// 4. 后处理阶段：Apply & Advance
	for _, peer := range readyPeers {
	     if peer == nil {
             log.Printf("Create panic: readyPeers contains nil")
             continue
          }
		rd := peer.raftGroup.Ready()
		for _, entry := range rd.CommittedEntries {
            // 【修改】接收返回值
            newPeer := peer.processEntry(entry)
            if peer != nil {
                if entry.Index > peer.GetAppliedIndex() {
                    peer.SetAppliedIndex(entry.Index)
                }
            }
            // 检查是否被移除
            if peer.stopped {
                w.removePeer(peer)
                break 
            }
            // 如果产生了分裂，注册新 Peer
            // log.Printf("Updating applied index for region %d", peer.regionID)
            if peer == nil {
                log.Printf("!!! PANIC ALERT !!! peer became nil in loop!")
                break
            }
            if newPeer != nil {
                w.registerPeer(newPeer)
            }
		}
		if !peer.stopped {
            peer.raftGroup.Advance(rd)
        	}
	}

	w.pendingPeers = make(map[uint64]*Peer)
}

func (w *StoreWorker) removePeer(p *Peer) {
	// 1. 从内存移除 (停止服务)
	delete(w.peers, p.regionID)
	delete(w.pendingPeers, p.regionID)
	w.router.Unregister(p.regionID)

	// 2. 异步执行物理清理 (避免阻塞 Worker 主循环)
	// 我们需要清理两部分数据：
	// A. Data (z{RegionID}...)
	// B. Raft Log/State (r{RegionID}...)
	
	// 为了在 goroutine 中使用，拷贝需要的数据
	store := w.store
	regionID := p.regionID
	startKey := p.region.StartKey
	endKey := p.region.EndKey

	go func() {
		log.Printf("[GC] Clearing data for removed Region %d...", regionID)

		// --- 清理 Data ---
		// 构造物理范围
		dataStart := DataKey(regionID, startKey)
		var dataEnd []byte
		if len(endKey) > 0 {
			dataEnd = DataKey(regionID, endKey)
		} else {
			// 如果 EndKey 无穷大，物理上是下一个 RegionID 的开始
			// DataKey 编码规则: 'z' + RegionID(8B) + UserKey
			// 所以下一个 Region 的前缀是 'z' + (RegionID+1)
			dataEnd = DataKey(regionID+1, nil)
		}

		if err := store.DeleteRange(dataStart, dataEnd); err != nil {
			log.Printf("[GC] Failed to delete data range: %v", err)
		}

		// --- 清理 Raft Meta (Log, HardState, ApplyState) ---
		// Raft Key 编码规则: 'r' + RegionID(8B) + Suffix
		// 我们可以删除 'r' + RegionID 到 'r' + (RegionID+1) 之间的所有 Key

		// raftStart := RaftLogKey(regionID, 0) 
		
		// 保留手动构造的部分：
		raftPrefixStart := make([]byte, 9)
		raftPrefixStart[0] = 'r'
		binary.BigEndian.PutUint64(raftPrefixStart[1:], regionID)
				
		raftPrefixEnd := make([]byte, 9)
		raftPrefixEnd[0] = 'r'
		binary.BigEndian.PutUint64(raftPrefixEnd[1:], regionID+1)
		
		if err := store.DeleteRange(raftPrefixStart, raftPrefixEnd); err != nil {
			log.Printf("[GC] Failed to delete raft meta: %v", err)
		}

		log.Printf("[GC] Region %d cleanup finished.", regionID)
	}()

	log.Printf("Peer %d removed from Region %d (scheduled for GC)", p.peerID, p.regionID)
}

func (w *StoreWorker) AddPeer(p *Peer) {
	w.peers[p.regionID] = p
	w.pendingPeers[p.regionID] = p 
}

func (w *StoreWorker) registerPeer(p *Peer) {
    w.peers[p.regionID] = p
    w.router.Register(p.regionID, w.receiver, p) // 注册路由
    
    // 如果新 Peer 也是本 Worker 管理，需要启动心跳吗？
    // onTick 会遍历 w.peers，所以自动生效
    log.Printf("Registered new peer for Region %d", p.regionID)
}

func (w *StoreWorker) checkSplit() {
    for _, peer := range w.peers {
        if peer.raftGroup.Status().Lead != peer.peerID {
            continue
        }
        
        // 【修复】去掉 raftstore. 前缀，直接调用 DataKey
        start := DataKey(peer.regionID, peer.region.StartKey)
        
        var end []byte
        if len(peer.region.EndKey) > 0 {
            end = DataKey(peer.regionID, peer.region.EndKey)
        } else {
            end = DataKey(peer.regionID + 1, nil)
        }

        // 【修复】构造成切片 [][]byte
        sizes := w.store.GetApproximateSizes([][]byte{start}, [][]byte{end})
        if len(sizes) == 0 { continue }
        
        size := sizes[0]
        
        if size >= MaxRegionSize {
            log.Printf("Region %d size %d exceeds threshold, triggering split...", peer.regionID, size)
            // 【修复】去掉 raftstore. 前缀
            w.processMsg(NewMsgSplitCheck(peer.regionID))
        }
    }
}

func (w *StoreWorker) onSplitCheck(regionID uint64) {
    peer, ok := w.peers[regionID]
    if !ok { return }
    
    // 【修改】动态计算 Split Key (字典序中间值)
    // 这是一个简化实现，假设 Key 是 "key-00000" 格式
    // 生产环境应调用 C++ Engine 的 ApproximateMiddle
    
    // 简单粗暴：直接取 start 往后偏移一点，或者硬编码一个更合理的中间值
    // 在本次压测中 (2000 个 Key)，中间大概是 key-01000
    // 为了确保 SplitKey 落在 Range 内，我们取 "key-01000"
    splitKey := []byte("key-01000") 

    
    // 校验一下是否在范围内，如果不在，可能不需要分裂或者逻辑有误
    if !peer.isKeyInRange(splitKey) {
        log.Printf("Calculated split key %s out of range, skip", splitKey)
        return
    }
    
    // 2. 向 PD 申请新 ID
    // 新 Region 的 ID
    newRegionID, err := w.askPDAllocID()
    if err != nil { return }
    
    // 新 Region 对应的 Peers 的 ID (每个副本都需要一个新 ID)
    var newPeerIDs []uint64
    for range peer.region.Peers {
        pid, _ := w.askPDAllocID()
        newPeerIDs = append(newPeerIDs, pid)
    }
    
    // 3. 构造 Admin Request
    adminReq := &titankvpb.AdminRequest{
        CmdType: titankvpb.AdminRequest_SPLIT,
        Split: &titankvpb.SplitRequest{
            SplitKey:    splitKey,
            NewRegionId: newRegionID,
            NewPeerIds:  newPeerIDs,
        },
    }
    
    cmd := &titankvpb.RaftCommand{
        Type:         titankvpb.RaftCommand_ADMIN,
        AdminRequest: adminReq,
    }
    
    // 4. Propose
    // Admin Command 和普通 Put/Delete 一样走 Raft 流程
    data, _ := proto.Marshal(cmd)
    peer.raftGroup.Propose(data)
    
    log.Printf("[Split] Proposing split at key %s for Region %d", string(splitKey), regionID)
}

func (w *StoreWorker) askPDAllocID() (uint64, error) {
    // 调用 PD RPC
    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()
    resp, err := w.pdClient.AllocID(ctx, &pdpb.AllocIDRequest{})
    if err != nil {
        return 0, err
    }
    return resp.Id, nil
}


func (w *StoreWorker) sendRegionHeartbeat(p *Peer) {
	// 1. 转换 Region (titankvpb -> pdpb)
    // 这一步必须深拷贝所有字段
	region := &pdpb.Region{
		Id:       p.region.Id,
		StartKey: p.region.StartKey,
		EndKey:   p.region.EndKey,
	}
    if p.region.RegionEpoch != nil {
        region.RegionEpoch = &pdpb.RegionEpoch{
            ConfVer: p.region.RegionEpoch.ConfVer,
            Version: p.region.RegionEpoch.Version,
        }
    }
    for _, peer := range p.region.Peers {
        region.Peers = append(region.Peers, &pdpb.Peer{
            Id: peer.Id, 
            StoreId: peer.StoreId,
        })
    }

	// 2. 转换 Leader Peer (titankvpb -> pdpb)
	leaderPeer := &pdpb.Peer{
		Id:      p.peerID,
		StoreId: p.storeID,
	}

	approxSize := uint64(100) 
    approxKeys := uint64(10000)

	go func() {
		req := &pdpb.RegionHeartbeatRequest{
			Region:          region,
			Leader:          leaderPeer,
			ApproximateSize: approxSize,
			ApproximateKeys: approxKeys,
		}

		ctx, cancel := context.WithTimeout(w.ctx, 3*time.Second)
		defer cancel()

		resp, err := w.pdClient.RegionHeartbeat(ctx, req)
		if err != nil {
			log.Printf("Failed to send region heartbeat: %v", err)
			return
		}
		// 【调试点】打印接收到的 Response
          log.Printf("[DEBUG] Recv Heartbeat Resp: %v", resp)

		if resp.TransferLeader != nil {
			log.Printf("[Schedule] Received TransferLeader to peer %d", resp.TransferLeader.PeerId)
            // 简单实现：直接调用 RawNode
            p.raftGroup.TransferLeader(resp.TransferLeader.PeerId)
		} else if resp.ChangePeer != nil {
			cp := resp.ChangePeer
            
            // 3. 转换 ChangePeer (pdpb -> titankvpb)
            tkChangeType := titankvpb.ChangePeer_ADD_NODE
            if cp.ChangeType == pdpb.ChangePeer_REMOVE_NODE {
                tkChangeType = titankvpb.ChangePeer_REMOVE_NODE
            }
            
            tkPeer := &titankvpb.Peer{
                Id: cp.Peer.Id,
                StoreId: cp.Peer.StoreId,
            }

			adminReq := &titankvpb.AdminRequest{
				CmdType: titankvpb.AdminRequest_CONF_CHANGE,
				ChangePeer: &titankvpb.ChangePeer{
					ChangeType: tkChangeType, // 使用转换后的类型
					Peer:       tkPeer,       // 使用转换后的 Peer
				},
			}
			
			cmd := &titankvpb.RaftCommand{
				Type:         titankvpb.RaftCommand_ADMIN,
				AdminRequest: adminReq,
			}
			
            // NewMsgRaftCmd 需要传入 regionID
			w.router.Send(p.regionID, NewMsgRaftCmd(p.regionID, cmd, nil))
		}
	}()
}