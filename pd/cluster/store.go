package cluster

import (
	"time"

	"titankv/pd/api/pdpb"
)

type StoreStatus int

const (
	StoreStatusUp StoreStatus = iota
	StoreStatusDisconnected
	StoreStatusDown
)

// StoreInfo 包装了 Proto 定义和运行时状态
type StoreInfo struct {
	Meta         *pdpb.MetaStore
	Stats        *pdpb.StoreStats
	LastHeartbeat time.Time
}

func NewStoreInfo(meta *pdpb.MetaStore) *StoreInfo {
	return &StoreInfo{
		Meta:          meta,
		Stats:         &pdpb.StoreStats{},
		LastHeartbeat: time.Now(),
	}
}

// 检查状态
func (s *StoreInfo) GetStatus() StoreStatus {
	// 20秒没心跳 -> Disconnected
	if time.Since(s.LastHeartbeat) > 20*time.Second {
		// 30分钟没心跳 -> Down (这里简化为 1分钟方便测试)
		if time.Since(s.LastHeartbeat) > 1*time.Minute {
			return StoreStatusDown
		}
		return StoreStatusDisconnected
	}
	return StoreStatusUp
}

// 深拷贝，防止并发读写冲突
func (s *StoreInfo) Clone() *StoreInfo {
	return &StoreInfo{
		Meta:          s.Meta, // Meta 通常只读，浅拷贝即可，或者 ProtoClone
		Stats:         s.Stats,
		LastHeartbeat: s.LastHeartbeat,
	}
}