package schedulers

import (
	"context"
	"log"
	"titankv/pd/api/pdpb"
	"titankv/pd/cluster"
	"titankv/pd/schedule"
)

const DefaultMaxReplicas = 3

type replicaScheduler struct {
	alloc IDAllocator
}

func NewReplicaScheduler(alloc IDAllocator) schedule.Scheduler {
	return &replicaScheduler{alloc: alloc}
}

func (s *replicaScheduler) Name() string {
	return "replica-scheduler"
}

func (s *replicaScheduler) Schedule(c *cluster.RaftCluster) *schedule.Operator {
	// 1. 遍历所有 Region (生产环境应有优先队列，优先处理副本缺失严重的)
	regions := c.GetRegions()
	for _, r := range regions {
		op := s.checkRegion(c, r)
		if op != nil {
			return op
		}
	}
	return nil
}

func (s *replicaScheduler) checkRegion(c *cluster.RaftCluster, r *cluster.RegionInfo) *schedule.Operator {
	// 1. 统计有效副本数
	// Store 状态: Up=有效, Disconnected=暂时无效但保留, Down=永久无效(需补副本)
	validPeers := 0
	downPeers := []*pdpb.Peer{}
	
	stores := c.GetStores()
	storeMap := make(map[uint64]*cluster.StoreInfo)
	for _, st := range stores {
		storeMap[st.Meta.Id] = st
	}

	for _, p := range r.Meta.Peers {
		st, ok := storeMap[p.StoreId]
		if !ok {
			// Store 没了? 视为 Down
			downPeers = append(downPeers, p)
			continue
		}
		if st.GetStatus() == cluster.StoreStatusUp || st.GetStatus() == cluster.StoreStatusDisconnected {
			validPeers++
		} else {
			downPeers = append(downPeers, p)
		}
	}

	// 2. 补副本逻辑
	if validPeers < DefaultMaxReplicas {
		log.Printf("[Replica] Region %d has %d valid peers. Need %d.", r.Meta.Id, validPeers, DefaultMaxReplicas)
		
		// 选一个 Best Store (不在现有 Peer 列表中，且状态为 Up)
		targetStore := s.selectBestStore(c, r.Meta.Peers)
		if targetStore == 0 {
			// log.Printf("No store available for new replica")
			return nil
		}

		newPeerID, err := s.alloc.Alloc(context.Background())
		if err != nil { return nil }

		return schedule.NewOperator(
			r.Meta.Id,
			"add-replica",
			&schedule.AddPeer{
				ToStore: targetStore,
				PeerID:  newPeerID,
			},
		)
	}

	// 3. 【新增】清理 Down 的副本
    // 如果有效副本数已达标，且存在 Down 的副本，则移除 Down 的副本
    // 每次只移除一个，防止过于激进
    if validPeers >= DefaultMaxReplicas && len(downPeers) > 0 {
        victim := downPeers[0]
        log.Printf("[Replica] Region %d has redundant down peer %d on store %d. Removing.", 
            r.Meta.Id, victim.Id, victim.StoreId)
            
        return schedule.NewOperator(
            r.Meta.Id,
            "remove-down-peer",
            &schedule.RemovePeer{
                FromStore: victim.StoreId,
            },
        )
    }
	
	return nil
}

func (s *replicaScheduler) selectBestStore(c *cluster.RaftCluster, existingPeers []*pdpb.Peer) uint64 {
	// 简单策略：选一个最空的、且不在 existingPeers 里的 Store
	// 复用 BalanceRegion 的逻辑，或者简化遍历
	stores := c.GetStores()
	var bestStore *cluster.StoreInfo
	var maxAvail uint64 = 0

	for _, st := range stores {
		if st.GetStatus() != cluster.StoreStatusUp { continue }
		
		// 排除已有副本的 Store
		exist := false
		for _, p := range existingPeers {
			if p.StoreId == st.Meta.Id { exist = true; break }
		}
		if exist { continue }

		available := uint64(0)
		if st.Stats != nil {
			available = st.Stats.Available
		}

		if bestStore == nil || available > maxAvail {
			maxAvail = available
			bestStore = st
		}
	}

	if bestStore != nil {
		return bestStore.Meta.Id
	}
	return 0
}
