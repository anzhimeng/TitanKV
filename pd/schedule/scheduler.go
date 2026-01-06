package schedule

import "titankv/pd/cluster"

// Scheduler 是所有调度策略必须实现的接口
type Scheduler interface {
	Name() string
	// Type() string // 比如 "leader", "region"

	// 核心方法：给定集群视图，返回一个 Operator（如果没有需要调度的，返回 nil）
	Schedule(c *cluster.RaftCluster) *Operator
}