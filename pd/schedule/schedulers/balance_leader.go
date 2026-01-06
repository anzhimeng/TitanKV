package schedulers

import (
	"log"
	"sort"
	"titankv/pd/api/pdpb"
	"titankv/pd/cluster"
	"titankv/pd/schedule"
)

const (
    // 只有当 Leader 数量差值大于此值时才调度
	minLeaderBalanceDiff = 2 
)

type balanceLeaderScheduler struct{}

func NewBalanceLeaderScheduler() schedule.Scheduler {
	return &balanceLeaderScheduler{}
}

func (s *balanceLeaderScheduler) Name() string {
	return "balance-leader-scheduler"
}

func (s *balanceLeaderScheduler) Schedule(c *cluster.RaftCluster) *schedule.Operator {
	// 1. 获取所有健康的 Store
	stores := c.GetStores()
     // 【新增调试】看看 PD 到底看到了几个 Store
     log.Printf("[Debug] Scheduler saw %d stores", len(stores))
	var suitableStores []*cluster.StoreInfo
	for _, store := range stores {
		if store.GetStatus() == cluster.StoreStatusUp {
			suitableStores = append(suitableStores, store)
		} else {
            // 【新增调试】看看为什么 Store 不健康
            log.Printf("[Debug] Store %d is not UP. LastHeartbeat: %v", store.Meta.Id, store.LastHeartbeat)
        }
	}

	if len(suitableStores) < 2 {
		return nil
	}

	// 2. 排序：LeaderCount 从小到大
	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].LeaderCount < suitableStores[j].LeaderCount
	})

	// Target: Leader 最少的
	// Source: Leader 最多的
	target := suitableStores[0]
	source := suitableStores[len(suitableStores)-1]
	// 【新增调试】打印排序后的 Leader 数量
     if len(suitableStores) >= 2 {
     log.Printf("[Debug] Source(ID=%d, Leaders=%d) -> Target(ID=%d, Leaders=%d)", 
             source.Meta.Id, source.LeaderCount, target.Meta.Id, target.LeaderCount)
     }
	// 3. 检查阈值
	if source.LeaderCount-target.LeaderCount < minLeaderBalanceDiff {
		return nil // 比较平衡，无需调度
	}

	// 4. 在 Source 上寻找一个合适的 Region
	// 该 Region 必须满足：
	// a. 当前 Leader 在 Source 上 (显然)
	// b. 该 Region 有一个副本在 Target 上 (否则无法转让 Leader)
	// c. (进阶) 该 Region 没有 Pending Peers
	
	// 注意：GetRegions 返回的是拷贝，虽然有性能损耗，但在当前规模下可接受
	regions := c.GetRegions()
	for _, region := range regions {
		// 检查 Leader 是否在 Source
		if region.Leader.StoreId != source.Meta.Id {
			continue
		}

		// 检查 Target 是否在 Peers 列表中
		var targetPeer *pdpb.Peer
		for _, p := range region.Meta.Peers {
			if p.StoreId == target.Meta.Id {
				targetPeer = p
				break
			}
		}

		if targetPeer != nil {
			// 找到目标！生成 Operator
			log.Printf("[Schedule] Move leader region %d from store %d to %d", 
                region.Meta.Id, source.Meta.Id, target.Meta.Id)
            
			return schedule.NewOperator(
				region.Meta.Id,
				"balance-leader",
				&schedule.TransferLeader{
					FromStore: source.Meta.Id,
					ToStore:   target.Meta.Id,
				},
			)
		}
	}

	// 没找到合适的 Region (虽然 Source 很忙，但它负责的 Region 在 Target 上都没有副本)
	// 这种情况需要 BalanceRegion 调度器先搬副本（Day 4 内容）
	return nil
}