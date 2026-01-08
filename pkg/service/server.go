package service

import (
	"context"
	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
	"titankv/pkg/raftstore"
	"titankv/pkg/store"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
    // Week 10: 暂时直接读 Store，Linearizable Read 需后续适配
    // key 需要编码吗？
    // 注意：Store 里的 key 是编码后的 (z{RegionID}_{UserKey})
    // 这是一个大坑！如果 Client 传的是 UserKey，我们需要知道 RegionID 才能读到数据。
    // 所以 Get 请求也必须带 RegionContext。
    
    if req.Context == nil {
        return nil, status.Error(codes.InvalidArgument, "missing region context")
    }
    
    // 编码 Key
    encodedKey := raftstore.DataKey(req.Context.RegionId, req.Key)
    
	val, err := s.store.Get(encodedKey)
	if err != nil {
		if err.Error() == "key not found" {
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