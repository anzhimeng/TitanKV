package raftstore

import (
	"sync"
	"context"
)

// PeerSender 本质上就是 Worker 的信箱 (Channel)
type PeerSender chan Msg

type Router struct {
	mu      sync.RWMutex
	// RegionID -> Worker Channel
	// 为什么是 Map？因为可能有多个 StoreWorker (线程池模式)，
     // 我们需要把同一个 Region 的消息总是发给同一个 Worker 以保证顺序。
	regions map[uint64]PeerSender
	peers sync.Map
	storeSender PeerSender // 【新增】全局信箱
}

func NewRouter() *Router {
	return &Router{
		regions: make(map[uint64]PeerSender),
	}
}

func (r *Router) Register(regionID uint64, sender PeerSender, peer *Peer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.regions[regionID] = sender

	// 【新增】存入缓存
	r.peers.Store(regionID, peer)
}

func (r *Router) Unregister(regionID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.regions, regionID)

	// 【新增】删除缓存
	r.peers.Delete(regionID)
}

// 注册全局信箱
func (r *Router) RegisterStore(sender PeerSender) {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.storeSender = sender
}

func (r *Router) Send(regionID uint64, msg Msg) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sender, ok := r.regions[regionID]
	if !ok {
		return false // Region 不在本节点
	}
	
	// 非阻塞发送，防止 Worker 卡死导致 gRPC 卡死
    // 实际生产中可能需要带超时或缓冲区满策略
	select {
	case sender <- msg:
		return true
	default:
		return false // 队列满
	}
    // 【新增】如果找不到 Region，发给全局 StoreSender
    // 只有 RaftMessage 需要全局处理（可能是创建 Peer 的消息）
    if msg.Type == MsgTypeRaftMessage && r.storeSender != nil {
        select {
        case r.storeSender <- msg:
            return true
        default:
            return false
        }
    }
    return false
}

type PeerStateReader interface {
    GetAppliedIndex() uint64
    WaitApplied(ctx context.Context, targetIndex uint64) error
}

func (r *Router) GetLocalPeer(regionID uint64) PeerStateReader {
    if v, ok := r.peers.Load(regionID); ok {
        return v.(*Peer)
    }
    return nil
}
