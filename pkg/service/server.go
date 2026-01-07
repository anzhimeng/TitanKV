package service

import (
	"context"
	"log"
	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
	"titankv/pkg/raft"
	"titankv/pkg/store" // 用于 Get 直接读
	"titankv/pkg/raftstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	titankvpb.UnimplementedTitanKVServer
	router *raftstore.Router    // 【新增】路由组件
	store    *store.TitanStore // 保留 store 用于读 (Day 3 暂不实现 ReadIndex)
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

    // 1. 获取上下文中的 RegionID
    // (Week 9 我们加上了 Context)
    if req.Context == nil {
        return nil, status.Error(codes.InvalidArgument, "missing region context")
    }
    regionID := req.Context.RegionId

    // 2. 构造 RaftCmd 消息
    // 我们需要在 Proto 里定义 RaftCmd (Week 3 定义过，复用)
    cmd := &titankvpb.RaftCommand{
        Op:    titankvpb.RaftCommand_PUT,
        Key:   req.Key,
        Value: req.Value,
        // 这里还需要带上 Epoch，供 Worker 校验
    }

    // 3. 通过 Router 发送
    msg := raftstore.NewMsgRaftCmd(regionID, cmd)
    // 这里我们需要一个机制来等待结果 (Wait Response)
    // MsgRaftCmd 需要携带一个 Callback channel
    // 让我们去修改 message.go 增加 Callback
    
    waitCh := make(chan error, 1)
    msg.Callback = waitCh // 需要修改 Msg 结构体

    if !s.router.Send(regionID, msg) {
        // Region 不在本节点
        return nil, status.Error(codes.NotFound, "region not found on this store")
    }

    // 4. 等待结果
    select {
    case err := <-waitCh:
        if err != nil {
            // 这里可能返回 KeyNotInRegion 错误
            // 需要转换为 gRPC 错误或者自定义 Response
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
     // Epoch 检查 【关键修复】
     if req.Context != nil {
         if err := s.raftNode.CheckEpoch(toPdpbEpoch(req.Context.RegionEpoch)); err != nil {
             return nil, status.Error(codes.Aborted, "EpochNotMatch")
         }
     }
	// 【Day 4 核心逻辑】
	// 1. 发起 ReadIndex (这一步会阻塞直到 Leader 确认身份)
	readIndex, err := s.raftNode.LinearizableRead(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "raft read index failed: "+err.Error())
	}

	// 2. 等待状态机追上 ReadIndex
	if err := s.raftNode.WaitApplied(ctx, readIndex); err != nil {
		return nil, status.Error(codes.DeadlineExceeded, "wait applied failed")
	}

	// 3. 安全读取 C++ 引擎
	val, err := s.store.Get(req.Key)
	if err != nil {
		if err.Error() == "key not found" {
			return nil, status.Error(codes.NotFound, "key not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &titankvpb.GetResponse{Value: val}, nil
}

// Delete 也要走 Raft
func (s *Server) Delete(ctx context.Context, req *titankvpb.DeleteRequest) (*titankvpb.DeleteResponse, error) {
    // Epoch 检查 【关键修复】
    if req.Context != nil {
        if err := s.raftNode.CheckEpoch(toPdpbEpoch(req.Context.RegionEpoch)); err != nil {
            return nil, status.Error(codes.Aborted, "EpochNotMatch")
        }
    }
    cmd := &titankvpb.RaftCommand{
		Op:  titankvpb.RaftCommand_DELETE,
		Key: req.Key,
	}
    s.raftNode.Propose(ctx, cmd)
    return &titankvpb.DeleteResponse{}, nil
}

// 实现 Raft RPC
func (s *Server) Raft(ctx context.Context, req *titankvpb.RaftMessage) (*titankvpb.RaftResponse, error) {
    // 直接转交给 TitanRaft
    err := s.raftNode.Step(ctx, req)
    if err != nil {
        return nil, status.Error(codes.Internal, err.Error())
    }
    return &titankvpb.RaftResponse{}, nil
}

func (s *Server) UpdateConfig(ctx context.Context, req *titankvpb.UpdateConfigRequest) (*titankvpb.UpdateConfigResponse, error) {
    // 只有 Leader 才能改配置？其实这是单机配置，每个节点都可以独立改
    // 但为了集群一致性，通常通过 Raft 走 Config Change，这里简化为直接改本地
    
    if req.GcThreshold > 0 {
        log.Printf("Updating GC Threshold to %.2f", req.GcThreshold)
        s.store.SetGCThreshold(req.GcThreshold)
    }
    return &titankvpb.UpdateConfigResponse{}, nil
}

func toPdpbEpoch(e *titankvpb.RegionEpoch) *pdpb.RegionEpoch {
    if e == nil {
        return &pdpb.RegionEpoch{} // 默认空值
    }
    return &pdpb.RegionEpoch{
        ConfVer: e.ConfVer,
        Version: e.Version,
    }
}