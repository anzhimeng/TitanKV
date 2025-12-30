package service

import (
	"context"
	"log"
	"titankv/api/titankvpb"
	"titankv/pkg/raft"
	"titankv/pkg/store" // 用于 Get 直接读
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	titankvpb.UnimplementedTitanKVServer
	raftNode *raft.TitanRaft // 改用 RaftNode
	batcher  *raft.Batcher
	store    *store.TitanStore // 保留 store 用于读 (Day 3 暂不实现 ReadIndex)
}

func NewServer(r *raft.TitanRaft, b *raft.Batcher, s *store.TitanStore) *Server {
	return &Server{
		raftNode: r,
		batcher:  b,
		store:    s,
	}
}

func (s *Server) Put(ctx context.Context, req *titankvpb.PutRequest) (*titankvpb.PutResponse, error) {
    // 1. 检查 Leader
    if s.raftNode.Node.Status().Lead != s.raftNode.ID {
        return &titankvpb.PutResponse{
            ErrCode:  1,
            LeaderId: s.raftNode.Node.Status().Lead,
        }, nil
    }
    
	cmd := &titankvpb.RaftCommand{
		Op:    titankvpb.RaftCommand_PUT,
		Key:   req.Key,
		Value: req.Value,
	}

	err := s.batcher.Propose(ctx, cmd)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &titankvpb.PutResponse{ErrCode: 0}, nil
}

func (s *Server) Get(ctx context.Context, req *titankvpb.GetRequest) (*titankvpb.GetResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
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