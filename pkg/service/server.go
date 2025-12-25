package service

import (
	"context"
	"titankv/api/titankvpb"
	"titankv/pkg/raft"
	"titankv/pkg/store" // 用于 Get 直接读
	"log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	titankvpb.UnimplementedTitanKVServer
	raftNode *raft.TitanRaft // 改用 RaftNode
	store    *store.TitanStore // 保留 store 用于读 (Day 3 暂不实现 ReadIndex)
}

func NewServer(r *raft.TitanRaft, s *store.TitanStore) *Server {
	return &Server{raftNode: r, store: s}
}

func (s *Server) Put(ctx context.Context, req *titankvpb.PutRequest) (*titankvpb.PutResponse, error) {
    // 1. 检查自己是不是 Leader
    // Status() 开销较大，生产环境通常缓存 Leader ID
    if s.raftNode.Node.Status().Lead != s.raftNode.ID {
        return &titankvpb.PutResponse{
            ErrCode:  1, // Not Leader
            LeaderId: s.raftNode.Node.Status().Lead,
        }, nil
    }

    // 2. 是 Leader，正常处理
    cmd := &titankvpb.RaftCommand{
        Op:    titankvpb.RaftCommand_PUT,
        Key:   req.Key,
        Value: req.Value,
    }

    err := s.raftNode.Propose(ctx, cmd)
    if err != nil {
        return nil, status.Error(codes.Internal, err.Error())
    }

    return &titankvpb.PutResponse{ErrCode: 0}, nil
}

// Get 依然直接查 store (暂时牺牲强一致性，为了先跑通写链路)
func (s *Server) Get(ctx context.Context, req *titankvpb.GetRequest) (*titankvpb.GetResponse, error) {
    // ... 保持 Day 2 的逻辑不变 ...
    val, err := s.store.Get(req.Key)
    	if err != nil {
		log.Fatalf("Failed to get key: %v", req.Key)
	}
    // ...
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