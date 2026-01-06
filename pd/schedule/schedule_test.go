package schedule_test

import (
	"context"
	"testing"

	"titankv/pd/api/pdpb"
	"titankv/pd/cluster"
	"titankv/pd/schedule" // 引用 schedule 包
	"titankv/pd/schedule/schedulers"
)

func TestBalanceLeader(t *testing.T) {
	// 1. 构造内存集群 (client = nil)
	c := cluster.NewRaftCluster(nil)
	ctx := context.Background()

	// 2. 注册 Store 1 和 Store 2
	// Store 1
	c.PutStore(ctx, &pdpb.MetaStore{
		Id:      1,
		Address: "addr1",
		State:   pdpb.StoreState_UP,
	})
	// Store 2
	c.PutStore(ctx, &pdpb.MetaStore{
		Id:      2,
		Address: "addr2",
		State:   pdpb.StoreState_UP,
	})

	// 3. 注入 Region 心跳，构造不平衡状态
	// 我们制造 3 个 Region，Leader 都在 Store 1 上
	// 并且这些 Region 在 Store 2 上都有副本（否则无法转移）
	for i := uint64(1); i <= 3; i++ {
		req := &pdpb.RegionHeartbeatRequest{
			Region: &pdpb.Region{
				Id:       i,
				StartKey: []byte{byte(i)},
				EndKey:   []byte{byte(i + 1)},
				Peers: []*pdpb.Peer{
					{Id: 100 + i, StoreId: 1}, // Peer on Store 1
					{Id: 200 + i, StoreId: 2}, // Peer on Store 2
				},
				RegionEpoch: &pdpb.RegionEpoch{ConfVer: 1, Version: 1},
			},
			Leader: &pdpb.Peer{Id: 100 + i, StoreId: 1}, // Leader 在 Store 1
			ApproximateSize: 10,
		}
		// 处理心跳，这会更新 Store 的 LeaderCount
		err := c.HandleRegionHeartbeat(ctx, req)
		if err != nil {
			t.Fatalf("Heartbeat failed: %v", err)
		}
	}

	// 4. 验证当前负载状态
	// 此时 Store 1 LeaderCount = 3, Store 2 LeaderCount = 0
	stores := c.GetStores()
	for _, s := range stores {
		if s.Meta.Id == 1 && s.LeaderCount != 3 {
			t.Errorf("Store 1 leader count should be 3, got %d", s.LeaderCount)
		}
		if s.Meta.Id == 2 && s.LeaderCount != 0 {
			t.Errorf("Store 2 leader count should be 0, got %d", s.LeaderCount)
		}
	}

	// 5. 运行调度器
	s := schedulers.NewBalanceLeaderScheduler()
	op := s.Schedule(c)

	// 6. 验证生成的 Operator
	if op == nil {
		t.Fatal("Expected an operator, got nil")
	}

	// 验证 Operator 的意图
	// 应该包含一个 TransferLeader 步骤
	if op.Kind != "balance-leader" {
		t.Errorf("Expected balance-leader op, got %s", op.Kind)
	}

	// 验证是否是从 Store 1 转到 Store 2
	// 我们虽然不能直接访问 op.Steps (如果是接口类型)，但可以通过 String() 或者断言
	foundTransfer := false
	for _, step := range op.Steps {
		if transfer, ok := step.(*schedule.TransferLeader); ok {
			if transfer.FromStore == 1 && transfer.ToStore == 2 {
				foundTransfer = true
			}
		}
	}

	if !foundTransfer {
		t.Errorf("Expected TransferLeader from 1 to 2, got steps: %v", op.Steps)
	}
	
	t.Logf("Successfully generated operator: %s", op.String())
}