package service

import (
 	"context"
     "io"
     "os"
     "strings"
     "log"
	
	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
	"titankv/pkg/raftstore"
	"titankv/pkg/store"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Server struct {
	titankvpb.UnimplementedTitanKVServer
    // 【修改】只依赖 Router 和 Store
	router *raftstore.Router
	store  *store.TitanStore
}

func NewServer(router *raftstore.Router, s *store.TitanStore) *Server {
	return &Server{
		router: router,
		store:  s,
	}
}

// 辅助：获取 Peer 以进行 Epoch 检查
func (s *Server) getPeer(regionID uint64) (*raftstore.Peer, error) {
	peer := s.router.GetLocalPeer(regionID).(*raftstore.Peer)
	if peer == nil {
		return nil, status.Error(codes.NotFound, "region not found on this store")
	}
	return peer, nil
}

// 辅助：Epoch 检查
func (s *Server) checkEpoch(ctx context.Context, regionID uint64, reqEpoch *titankvpb.RegionEpoch) error {
	peer, err := s.getPeer(regionID)
	if err != nil {
		return err
	}
	
	// 【修改】直接传 titankvpb.RegionEpoch，不要转 pdpb
	// 如果你的 Peer.CheckEpoch 定义是接收 *pdpb.RegionEpoch，那你应该去改 Peer
	// 让我们假设 Peer 用的是 titankvpb (因为 Peer.region 是 titankvpb.Region)
	if err := peer.CheckEpoch(reqEpoch); err != nil {
		return status.Error(codes.Aborted, "EpochNotMatch")
	}
	return nil
}


func (s *Server) Put(ctx context.Context, req *titankvpb.PutRequest) (*titankvpb.PutResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	if req.Context == nil {
		return nil, status.Error(codes.InvalidArgument, "missing region context")
	}

	regionID := req.Context.RegionId

    cmd := &titankvpb.RaftCommand{
        Header: &titankvpb.RaftRequestHeader{
            RegionId:    regionID,
            RegionEpoch: req.Context.RegionEpoch,
            Peer:        req.Context.Peer,
        },
        Type:  titankvpb.RaftCommand_NORMAL,
        Op:    titankvpb.RaftCommand_PUT,
        Key:   req.Key,
        Value: req.Value,
    }

    // 创建回调通道
	waitCh := make(chan error, 1)
    
    // 构造回调函数
    cb := func(err error) {
        waitCh <- err
    }

	// 【修复】传入回调函数
	msg := raftstore.NewMsgRaftCmd(regionID, cmd, cb)

	if !s.router.Send(regionID, msg) {
		return nil, status.Error(codes.NotFound, "region not found on this store")
	}

	select {
	case err := <-waitCh:
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return &titankvpb.PutResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Server) Get(ctx context.Context, req *titankvpb.GetRequest) (*titankvpb.GetResponse, error) {
    if len(req.Key) == 0 {
        return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
    }
    if req.Context == nil {
        return nil, status.Error(codes.InvalidArgument, "missing region context")
    }
    if req.StartTs == 0 {
        return nil, status.Error(codes.InvalidArgument, "missing start_ts")
    }

    regionID := req.Context.RegionId

    // =========================================================
    // Phase 1: Linearizability Check (ReadIndex)
    // 确保我们读到的是最新的数据状态 (防止脑裂读旧数据)
    // =========================================================
    
    // 1.1 构造响应通道
    retCh := make(chan uint64, 1)
    
    // 1.2 发送 MsgReadIndex
    msg := raftstore.NewMsgReadIndex(regionID, retCh)
    if !s.router.Send(regionID, msg) {
        return nil, status.Error(codes.NotFound, "region not found")
    }
    
    // 1.3 等待 Raft 确认 (ReadIndex)
    var safeIndex uint64
    select {
    case idx := <-retCh:
        safeIndex = idx
    case <-ctx.Done():
        return nil, status.Error(codes.DeadlineExceeded, "read index timeout")
    }
    
    // 1.4 等待状态机追赶 (Wait Applied)
    peer := s.router.GetLocalPeer(regionID)
    if peer == nil {
        return nil, status.Error(codes.NotFound, "region lost during read")
    }

    
    // 【修改】调用 Peer 的高效等待接口
    if err := peer.WaitApplied(ctx, safeIndex); err != nil {
        return nil, status.Error(codes.DeadlineExceeded, "wait applied timeout")
    }


    // =========================================================
    // Phase 2: MVCC Read (Snapshot Read)
    // 在本地引擎中，根据 StartTS 读取可见版本
    // =========================================================

    // 注意：Store 里的 Key 不需要再加 z{RegionID} 前缀了！
    // 为什么？因为我们在 C++ 层实现的 PutCF/GetCF 会自动处理 MVCC 编码。
    // 但是！C++ 层的 MVCC Key 是基于 User Key 的。
    // 如果我们想支持 Multi-Raft，底层的 Key 应该是 z{RegionID}_{MvccKey}。
    // 这涉及到 C++ 层的改造。
    // 
    // 【关键回顾】：Week 13 Day 1 我们实现的 EncodeMvccKey 是： Prefix(1) + UserKey + TS(8)。
    // 它并没有包含 RegionID！
    // 这意味着目前的 MVCC 实现是单机版的，不支持 Multi-Raft 数据隔离。
    //
    // 为了 Week 14 能跑通，我们需要做一个适配：
    // 将 z{RegionID}_{UserKey} 作为一个整体，当作 MVCC 的 "User Key" 传给 C++。
    // 这样 C++ 编码后就是：Prefix(1) + z{RegionID}_{UserKey} + TS(8)。
    // 虽然多了一层前缀，但逻辑是完全正确的，且实现了隔离。
    
    encodedKey := raftstore.DataKey(regionID, req.Key)
    
    // 调用 MvccGet
    val, err := s.store.MvccGet(encodedKey, req.StartTs)
    if err != nil {
        if err.Error() == "Key is locked" {
             return nil, status.Error(codes.Aborted, "KeyLocked")
        }
        if err.Error() == "Key deleted" || err.Error() == "key not found" {
             return nil, status.Error(codes.NotFound, "key not found")
        }
        return nil, status.Error(codes.Internal, err.Error())
    }

    return &titankvpb.GetResponse{Value: val}, nil
}

func (s *Server) Delete(ctx context.Context, req *titankvpb.DeleteRequest) (*titankvpb.DeleteResponse, error) {
    if req.Context == nil {
        return nil, status.Error(codes.InvalidArgument, "missing region context")
    }
    regionID := req.Context.RegionId

    cmd := &titankvpb.RaftCommand{
        Header: &titankvpb.RaftRequestHeader{
            RegionId:    req.Context.RegionId,
            RegionEpoch: req.Context.RegionEpoch,
            Peer:        req.Context.Peer,
        },
        Type: titankvpb.RaftCommand_NORMAL,
        Op:   titankvpb.RaftCommand_DELETE,
        Key:  req.Key,
    }
    
    waitCh := make(chan error, 1)
    cb := func(err error) { waitCh <- err }
    msg := raftstore.NewMsgRaftCmd(regionID, cmd, cb)

    if !s.router.Send(regionID, msg) {
        return nil, status.Error(codes.NotFound, "region not found")
    }

    select {
    case err := <-waitCh:
        if err != nil {
            return nil, status.Error(codes.Internal, err.Error())
        }
        return &titankvpb.DeleteResponse{}, nil
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}

// 处理 Raft 消息 (节点间通信)
func (s *Server) Raft(ctx context.Context, req *titankvpb.RaftMessage) (*titankvpb.RaftResponse, error) {
    // 使用 Router 分发
    msg := raftstore.NewMsgRaftMessage(req)
    if !s.router.Send(req.RegionId, msg) {
        // Region 可能正在创建中或者还没 Ready，甚至不存在
        // 生产环境可能需要重试或者返回错误
        // return nil, status.Error(codes.NotFound, "region not found")
    }
    return &titankvpb.RaftResponse{}, nil
}

// --- PD 交互接口 (暂未适配 Multi-Raft，先 Stub 掉以通过编译) ---
// Week 10 Day 5 联调时我们主要测 Put/Get，不需要 PD 介入 Store 管理

// UpdateConfig 接口实现 (Week 7 遗留)
func (s *Server) UpdateConfig(ctx context.Context, req *titankvpb.UpdateConfigRequest) (*titankvpb.UpdateConfigResponse, error) {
    if req.GcThreshold > 0 {
        s.store.SetGCThreshold(req.GcThreshold)
    }
    return &titankvpb.UpdateConfigResponse{}, nil
}

// 辅助转换
func toPdpbEpoch(e *titankvpb.RegionEpoch) *pdpb.RegionEpoch {
    if e == nil {
        return &pdpb.RegionEpoch{}
    }
    return &pdpb.RegionEpoch{ConfVer: e.ConfVer, Version: e.Version}
}

func (s *Server) StreamSnapshot(stream titankvpb.TitanKV_StreamSnapshotServer) error {
    var file *os.File
    var regionID uint64
    var raftSnapshot raftpb.Snapshot // 【新增】暂存元数据
    
    // 1. 接收 Loop
    for {
        chunk, err := stream.Recv()
        if err == io.EOF {
            // 传输完成
            // 把文件路径塞回 Snapshot.Data
            raftSnapshot.Data = []byte(file.Name())
            
            s.finishSnapshot(regionID, &raftSnapshot)
            return stream.SendAndClose(&titankvpb.RaftResponse{})
        }
        if err != nil { return err }
        
        if file == nil {
            regionID = chunk.RegionId
            file, err = os.CreateTemp("", "snap-*.sst")
        }
        file.Write(chunk.Data)
        
        // 【新增】如果包含元数据，保存下来
        if len(chunk.RaftSnapshotData) > 0 {
            raftSnapshot.Unmarshal(chunk.RaftSnapshotData)
        }
    }
}

func (s *Server) finishSnapshot(regionID uint64, snap *raftpb.Snapshot) {
    // 构造 MsgSnap 消息
    // 注意：我们需要把 raftpb.Snapshot 包装进 raftpb.Message
    rMsg := raftpb.Message{
        Type: raftpb.MsgSnap,
        Snapshot: *snap,
    }
    data, _ := rMsg.Marshal()
    
    msg := raftstore.Msg{
        Type:     raftstore.MsgTypeRaftMessage, // 当作普通 Raft 消息处理
        RegionID: regionID,
        RaftMessage: &titankvpb.RaftMessage{
            RegionId: regionID,
            Data:     data,
        },
    }
    s.router.Send(regionID, msg)
}

func (s *Server) BatchRaft(stream titankvpb.TitanKV_BatchRaftServer) error {
    for {
        batch, err := stream.Recv()
        if err != nil {
            return err
        }
        
        for _, msg := range batch.Msgs {
            // 分发逻辑同 Raft 接口
            raftMsg := raftstore.NewMsgRaftMessage(msg)
            s.router.Send(msg.RegionId, raftMsg)
        }
        
        // 也可以不回包，或者定期回一个 ACK
        // stream.Send(&titankvpb.RaftResponse{})
    }
}

func (s *Server) Prewrite(ctx context.Context, req *titankvpb.PrewriteRequest) (*titankvpb.PrewriteResponse, error) {
	// log.Println("!!! SERVER RECEIVED PREWRITE !!!")
	if req.Context == nil {
		return nil, status.Error(codes.InvalidArgument, "missing context")
	}
	
	// 1. 检查 Epoch
	if err := s.checkEpoch(ctx, req.Context.RegionId, req.Context.RegionEpoch); err != nil {
		return nil, err
	}

	regionID := req.Context.RegionId

	// 2. 编码 Keys (Multi-Raft 隔离)
	var encodedMutations []*titankvpb.Mutation
	for _, m := range req.Mutations {
		encKey := raftstore.DataKey(regionID, m.Key)
		encodedMutations = append(encodedMutations, &titankvpb.Mutation{
			Op:    m.Op,
			Key:   encKey,
			Value: m.Value,
		})
	}
	
	// 【修复】使用编码后的 Primary Key
	encPrimary := raftstore.DataKey(regionID, req.PrimaryKey)

	// 3. 调用 Store
	// log.Printf("[Server] Calling Store.Prewrite...")
	err := s.store.Prewrite(encodedMutations, encPrimary, req.StartTs, req.LockTtl)
	
	if err != nil {
		// 解析错误
		log.Printf("[Server] Store.Prewrite failed: %v", err)
		if strings.Contains(err.Error(), "Key is locked") {
			return &titankvpb.PrewriteResponse{Error: "KeyLocked"}, nil
		}
		// 还可以处理 WriteConflict
		return &titankvpb.PrewriteResponse{Error: err.Error()}, nil
	} else {
        // log.Printf("[Server] Store.Prewrite success.")
     }

	return &titankvpb.PrewriteResponse{}, nil
}

func (s *Server) Commit(ctx context.Context, req *titankvpb.CommitRequest) (*titankvpb.CommitResponse, error) {
	if req.Context == nil {
		return nil, status.Error(codes.InvalidArgument, "missing context")
	}

	// 1. 检查 Epoch
	if err := s.checkEpoch(ctx, req.Context.RegionId, req.Context.RegionEpoch); err != nil {
		return nil, err
	}

	regionID := req.Context.RegionId

	// 2. 编码 Keys
	var encodedKeys [][]byte
	for _, k := range req.Keys {
		encodedKeys = append(encodedKeys, raftstore.DataKey(regionID, k))
	}

	// 3. 调用 Store
	err := s.store.Commit(encodedKeys, req.StartTs, req.CommitTs)
	if err != nil {
		return &titankvpb.CommitResponse{Error: err.Error()}, nil
	}

	return &titankvpb.CommitResponse{}, nil
}

func (s *Server) CheckTxnStatus(ctx context.Context, req *titankvpb.CheckTxnStatusRequest) (*titankvpb.CheckTxnStatusResponse, error) {
    // 路由检查...
    
    action, commitTS, err := s.store.CheckTxnStatus(req.PrimaryKey, req.LockTs, req.CurrentTs)
    if err != nil {
        return nil, err
    }
    
    return &titankvpb.CheckTxnStatusResponse{
        Action:   titankvpb.CheckTxnStatusResponse_Action(action),
        CommitTs: commitTS,
    }, nil
}