package schedule

import (
	"titankv/pd/cluster"
)

type DummyScheduler struct{}

func (s *DummyScheduler) Name() string {
	return "dummy-scheduler"
}

func (s *DummyScheduler) Schedule(c *cluster.RaftCluster) *Operator {
	// Day 1: 暂时什么都不做，只打印日志证明被调用了
	// log.Println("[DummyScheduler] Checking cluster state...")
	
	// 在 Day 2 我们有了 Region 信息后，这里可以尝试返回一个 Operator
	return nil
}