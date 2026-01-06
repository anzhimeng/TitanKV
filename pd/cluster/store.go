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
	// 【新增】实时维护的 Leader 数量
     LeaderCount int
     // 【新增】实时维护的 Region 数量 (副本数)
     RegionCount int 
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
		// 【新增】
          LeaderCount:   s.LeaderCount,
          RegionCount:   s.RegionCount,
	}
}

// 获取资源分数 (使用率: 0.0 ~ 1.0)
// 分数越高，说明磁盘越满，越需要把数据搬走
func (s *StoreInfo) GetResourceScore() float64 {
    if s.Stats == nil || s.Stats.Capacity == 0 {
        return 0
    }
    used := s.Stats.Capacity - s.Stats.Available
    return float64(used) / float64(s.Stats.Capacity)
}