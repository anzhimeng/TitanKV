package cluster

import (
	"context"
	"log"
	"time"
)

// StartMonitor 启动后台监控线程
func (c *RaftCluster) StartMonitor(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.checkStores()
		}
	}
}

// checkStores 检查所有 Store 的健康状态
func (c *RaftCluster) checkStores() {
	// 1. 获取快照 (避免长时间持有锁)
	c.mu.RLock()
	stores := make([]*StoreInfo, 0, len(c.stores))
	for _, s := range c.stores {
		stores = append(stores, s)
	}
	c.mu.RUnlock()

	// 2. 检查状态
	for _, s := range stores {
		status := s.GetStatus()
		if status == StoreStatusDown {
			log.Printf("[Alert] Store %d is DOWN! Last heartbeat: %v", s.Meta.Id, s.LastHeartbeat)
			// TODO: Week 9 在这里触发调度逻辑 (补副本)
		} else if status == StoreStatusDisconnected {
			log.Printf("[Warn] Store %d is Disconnected.", s.Meta.Id)
		}
	}
}