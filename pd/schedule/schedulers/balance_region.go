package schedulers

import (
	"log"
	"sort"
	"context"
	"titankv/pd/cluster"
	"titankv/pd/schedule"
)

const (
    // 磁盘使用率差异阈值 (例如相差 20% 才搬迁)
	minRegionScoreDiff = 0.2 
)

type IDAllocator interface {
    Alloc(ctx context.Context) (uint64, error)
}

type balanceRegionScheduler struct{
    alloc IDAllocator // 【新增】
}

func NewBalanceRegionScheduler(alloc IDAllocator) schedule.Scheduler {
    return &balanceRegionScheduler{alloc: alloc}
}

func (s *balanceRegionScheduler) Name() string {
	return "balance-region-scheduler"
}

func (s *balanceRegionScheduler) Schedule(c *cluster.RaftCluster) *schedule.Operator {
	// 1. 获取所有存活的 Store
	stores := c.GetStores()
	var suitableStores []*cluster.StoreInfo
	for _, store := range stores {
		if store.GetStatus() == cluster.StoreStatusUp {
			suitableStores = append(suitableStores, store)
		}
	}

	if len(suitableStores) < 2 {
		return nil
	}

	// 2. 排序：按资源分数 (Disk Usage) 从小到大
	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].GetResourceScore() < suitableStores[j].GetResourceScore()
	})

	// Target: 最空的 (Score 最小)
	// Source: 最满的 (Score 最大)
	target := suitableStores[0]
	source := suitableStores[len(suitableStores)-1]

	// 3. 检查阈值
	// 如果最满的和最空的差别不大，就不折腾了，毕竟搬数据很贵
	if source.GetResourceScore() - target.GetResourceScore() < minRegionScoreDiff {
		return nil
	}

	// 4. 挑选 Region
	// 从 Source 选一个能搬到 Target 的 Region
	region := c.RandRegionOnStore(source.Meta.Id, target.Meta.Id)
	if region == nil {
		return nil
	}

	log.Printf("[Schedule] Move peer of region %d from store %d to %d (Usage: %.2f vs %.2f)", 
        region.Meta.Id, source.Meta.Id, target.Meta.Id, 
        source.GetResourceScore(), target.GetResourceScore())

    // 5. 生成 Operator (MovePeer = Add + Remove)
    // 我们需要为新 Peer 分配一个 ID (这里暂时 Mock，Day 5 结合 AllocID 完善)
    // 假设我们直接用 target store id 作为 peer id 的基底 (仅演示，实际必须 AllocID)
    // newPeerID := target.Meta.Id 
    // 正确做法：应该在 Schedule 接口传入 ID Allocator，或者 Operator 执行时分配。
    // 5. 生成 Operator
    // 【关键修复】分配真正的 PeerID
    newPeerID, err := s.alloc.Alloc(context.Background())
    newPeerID += 100
    if err != nil {
        log.Printf("Failed to alloc peer id: %v", err)
        return nil
    }
	return schedule.NewOperator(
		region.Meta.Id,
		"balance-region",
		// 第一步：在目标节点加副本
		&schedule.AddPeer{
			ToStore: target.Meta.Id,
			PeerID:  newPeerID, 
		},
		// 第二步：在源节点删副本
		&schedule.RemovePeer{
			FromStore: source.Meta.Id,
		},
	)
}