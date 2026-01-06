package schedule

import (
	"context"
	"log"
	"sync"
	"time"

	"titankv/pd/cluster"
)

type Coordinator struct {
	cluster    *cluster.RaftCluster
	schedulers []Scheduler
	
	// 记录正在执行的 Operator (RegionID -> Operator)
	// 防止同一个 Region 同时被多个调度器操作
	operators map[uint64]*Operator 
	mu        sync.Mutex
}

func NewCoordinator(c *cluster.RaftCluster) *Coordinator {
	return &Coordinator{
		cluster:   c,
		operators: make(map[uint64]*Operator),
	}
}

// 注册调度器
func (c *Coordinator) AddScheduler(s Scheduler) {
	c.schedulers = append(c.schedulers, s)
	log.Printf("[Coordinator] Added scheduler: %s", s.Name())
}

// 启动主循环
func (c *Coordinator) Run(ctx context.Context) {
	// 假设每 2 秒轮询一次调度
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.runSchedulers()
		}
	}
}

func (c *Coordinator) runSchedulers() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 轮询每一个调度器
	for _, sched := range c.schedulers {
		// 调用调度器逻辑
		op := sched.Schedule(c.cluster)
		
		if op != nil {
			// 检查是否冲突（该 Region 是否已经有正在运行的 Operator）
			if _, ok := c.operators[op.RegionID]; ok {
				// 冲突，忽略本次调度
				continue 
			}
			
			// 接纳 Operator
			c.operators[op.RegionID] = op
			log.Printf("[Coordinator] Generated Operator: %s", op.String())
		}
	}
	
	// TODO: 清理已完成或超时的 Operator (Day 5 完善)
}

// Day 5 接口：获取 Region 的待执行 Operator
func (c *Coordinator) GetOperator(regionID uint64) *Operator {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.operators[regionID]
}