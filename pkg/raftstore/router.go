package raftstore

import (
	"sync"
)

// PeerSender 本质上就是 Worker 的信箱 (Channel)
type PeerSender chan Msg

type Router struct {
	mu      sync.RWMutex
	// RegionID -> Worker Channel
	// 为什么是 Map？因为可能有多个 StoreWorker (线程池模式)，
    // 我们需要把同一个 Region 的消息总是发给同一个 Worker 以保证顺序。
	regions map[uint64]PeerSender
}

func NewRouter() *Router {
	return &Router{
		regions: make(map[uint64]PeerSender),
	}
}

func (r *Router) Register(regionID uint64, sender PeerSender) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.regions[regionID] = sender
}

func (r *Router) Unregister(regionID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.regions, regionID)
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
}