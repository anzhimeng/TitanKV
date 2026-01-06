package schedule_test

import (
	"context"
	"testing"

	"titankv/pd/api/pdpb"
	"titankv/pd/cluster"
	"titankv/pd/schedule" // 引用 schedule 包
	"titankv/pd/schedule/schedulers"
)

type mockIDAllocator struct{}

func (m *mockIDAllocator) Alloc(ctx context.Context) (uint64, error) {
	return 999, nil // 测试中随便返回一个 ID 即可
}

func TestBalanceLeader(t *testing.T) {
	// 1. 构造集群
	c := cluster.NewRaftCluster(nil)

	// 注册 Store
	c.PutStore(context.Background(), &pdpb.MetaStore{Id: 1, Address: "addr1", State: pdpb.StoreState_UP})
	c.PutStore(context.Background(), &pdpb.MetaStore{Id: 2, Address: "addr2", State: pdpb.StoreState_UP})

	// 2. 注入 Region 心跳 (建立拓扑关系)
	req := &pdpb.RegionHeartbeatRequest{
		Region: &pdpb.Region{
			Id: 1,
			Peers: []*pdpb.Peer{
				{Id: 101, StoreId: 1},
				{Id: 102, StoreId: 2},
			},
		},
		Leader: &pdpb.Peer{Id: 102, StoreId: 2}, // Leader 在 Store 2
	}
	c.HandleRegionHeartbeat(context.Background(), req)

	// 3. 【关键修复】在心跳之后强制设置 Count，覆盖心跳的副作用
	// 场景 A: 构造微小差异 (11 - 10 = 1 < 2)，不应调度
	c.SetStoreLeaderCountForTest(1, 10)
	c.SetStoreLeaderCountForTest(2, 11)

	// 运行调度
	sched := schedulers.NewBalanceLeaderScheduler()
	op := sched.Schedule(c)

	// 验证：应该没有生成 Operator
	if op != nil {
		t.Fatalf("Case A Failed: Expected nil (oscillation protection), got %v", op)
	} else {
		t.Log("Case A Passed: Oscillation prevented (Diff=1).")
	}

	// 4. 场景 B: 构造显著差异 (13 - 10 = 3 > 2)，应该调度
	c.SetStoreLeaderCountForTest(2, 13) // 更新 Store 2

	op = sched.Schedule(c)

	// 验证：应该生成 Operator
	if op == nil {
		t.Fatalf("Case B Failed: Expected operator (Diff=3), got nil")
	} else {
		t.Logf("Case B Passed: Scheduling triggered. Operator: %s", op.String())
	}
}

func TestBalanceRegion(t *testing.T) {
	c := cluster.NewRaftCluster(nil)

	// Store 1: 满 (100GB / 100GB), Usage = 1.0
	c.PutStore(context.Background(), &pdpb.MetaStore{Id: 1, Address: "addr1", State: pdpb.StoreState_UP})
	c.HandleStoreHeartbeat(&pdpb.StoreHeartbeatRequest{
		StoreId: 1,
		Stats:   &pdpb.StoreStats{Capacity: 100, Available: 0},
	})

	// Store 2: 空 (0GB / 100GB), Usage = 0.0
	c.PutStore(context.Background(), &pdpb.MetaStore{Id: 2, Address: "addr2", State: pdpb.StoreState_UP})
	c.HandleStoreHeartbeat(&pdpb.StoreHeartbeatRequest{
		StoreId: 2,
		Stats:   &pdpb.StoreStats{Capacity: 100, Available: 100},
	})

	// Region 1 在 Store 1 上
	req := &pdpb.RegionHeartbeatRequest{
		Region: &pdpb.Region{
			Id: 1,
			Peers: []*pdpb.Peer{{Id: 101, StoreId: 1}}, 
		},
		Leader: &pdpb.Peer{Id: 101, StoreId: 1},
	}
	c.HandleRegionHeartbeat(context.Background(), req)

	// 运行调度
	// 【关键修复】传入 mockIDAllocator
	sched := schedulers.NewBalanceRegionScheduler(&mockIDAllocator{})
	op := sched.Schedule(c)

	if op == nil {
		t.Fatal("Expected balance-region operator, got nil")
	}
    
    // 验证 Operator 步骤
    if len(op.Steps) != 2 {
        t.Fatalf("Expected 2 steps (Add+Remove), got %d", len(op.Steps))
    }
    
    addStep, ok := op.Steps[0].(*schedule.AddPeer)
    if !ok || addStep.ToStore != 2 {
        t.Error("Step 1 should be AddPeer to Store 2")
    }

    removeStep, ok2 := op.Steps[1].(*schedule.RemovePeer)
    if !ok2 || removeStep.FromStore != 1 {
        t.Error("Step 2 should be RemovePeer from Store 1")
    }

	t.Logf("Generated Operator: %s", op.String())
}

func TestBalanceLeaderOscillation(t *testing.T) {
	// 1. 构造集群
	c := cluster.NewRaftCluster(nil)

	// 注册 Store 1
	c.PutStore(context.Background(), &pdpb.MetaStore{Id: 1, Address: "addr1", State: pdpb.StoreState_UP})
	// 注册 Store 2
	c.PutStore(context.Background(), &pdpb.MetaStore{Id: 2, Address: "addr2", State: pdpb.StoreState_UP})

	// 2. 场景 A: 构造微小差异 (不足以触发调度)
	// Store 1 有 10 个 Leader
	// Store 2 有 11 个 Leader
	// 差值 = 1 < 阈值(2)，不应该调度
	setStoreLeaderCount(c, 1, 10)
	setStoreLeaderCount(c, 2, 11)

	// 注入一个 Region (Leader 在 Store 2，Peers 在 1, 2)
	req := &pdpb.RegionHeartbeatRequest{
		Region: &pdpb.Region{
			Id: 1,
			Peers: []*pdpb.Peer{
				{Id: 101, StoreId: 1},
				{Id: 102, StoreId: 2},
			},
		},
		Leader: &pdpb.Peer{Id: 102, StoreId: 2}, // Leader 在 Store 2
	}
	c.HandleRegionHeartbeat(context.Background(), req)

	// 运行调度
	sched := schedulers.NewBalanceLeaderScheduler()
	op := sched.Schedule(c)

	// 验证：应该没有生成 Operator
	if op != nil {
		t.Fatalf("Case A Failed: Expected nil (oscillation protection), got %v", op)
	} else {
		t.Log("Case A Passed: Oscillation prevented (Diff=1).")
	}

	// 3. 场景 B: 构造显著差异 (触发调度)
	// Store 1 有 10 个 Leader
	// Store 2 有 13 个 Leader
	// 差值 = 3 > 阈值(2)，应该调度
	c.SetStoreLeaderCountForTest( 2, 13) // 更新 Store 2

	op = sched.Schedule(c)

	// 验证：应该生成 Operator
	if op == nil {
		t.Fatalf("Case B Failed: Expected operator (Diff=3), got nil")
	} else {
		t.Logf("Case B Passed: Scheduling triggered. Operator: %s", op.String())
	}
}

// 辅助函数：暴力修改 Store 的 LeaderCount (仅用于测试)
func setStoreLeaderCount(c *cluster.RaftCluster, storeID uint64, count int) {
	// 注意：这里利用了同一个包内可以访问私有 map 的特性
    // 或者是通过 GetStores 获取副本后无法修改内部状态，
    // 所以我们需要一种 Hack 方式，或者通过 HandleRegionHeartbeat 累加。
    // 为了简单，我们假设你在 cluster 包内加了个 SetLeaderCountForTest 方法，
    // 或者直接在这里通过 HandleStoreHeartbeat 更新 Stats 里的 RegionCount 
    // (注意：LeaderCount 是根据 Region 心跳动态计算的，不是通过 Store 心跳上报的)
    
    // **修正方案**：最正规的方法是伪造 N 个 Region 心跳。
    // 但为了代码简短，我们假设你在 cluster.go 里加了这个后门：
    // func (c *RaftCluster) SetStoreLeaderCount(id uint64, count int) { c.stores[id].LeaderCount = count }
    
    // 如果没有后门，我们需要手动构造 dummy regions。
    // 下面通过反射或者直接修改 cluster 包代码来支持测试。
    // **建议**：直接在 pd/cluster/cluster.go 底部加个 SetLeaderCountForTest
}