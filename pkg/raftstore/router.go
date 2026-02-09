package raftstore

import (
	"sync"
	"context"
	"time"

	"titankv/pd/api/pdpb"
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
	sender, ok := r.regions[regionID]
	storeSender := r.storeSender
	r.mu.RUnlock()

	timeout := 200 * time.Millisecond
	switch msg.Type {
	case MsgTypeRaftMessage, MsgTypeRaftCmd, MsgTypeReadIndex:
		timeout = time.Second
	}
	deadline := time.Now().Add(timeout)

	if ok {
		for {
			select {
			case sender <- msg:
				return true
			default:
				if time.Now().After(deadline) {
					return false
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}

	if msg.Type == MsgTypeRaftMessage && storeSender != nil {
		for {
			select {
			case storeSender <- msg:
				return true
			default:
				if time.Now().After(deadline) {
					return false
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}
	return false
}

type PeerStateReader interface {
    GetAppliedIndex() uint64
    WaitApplied(ctx context.Context, targetIndex uint64) error
    CheckEpoch(reqEpoch *pdpb.RegionEpoch) error
}

func (r *Router) GetLocalPeer(regionID uint64) PeerStateReader {
    if v, ok := r.peers.Load(regionID); ok {
		if peer, ok := v.(*Peer); ok {
			return peer
		}
    }
    return nil
}
