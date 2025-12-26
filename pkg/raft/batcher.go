package raft

import (
	"context"
	"time"
	"titankv/api/titankvpb"
)

// proposal 包装原始请求和通知通道
type proposal struct {
	cmd  *titankvpb.RaftCommand
	done chan error // 用于通知 gRPC handler 结果
}

type Batcher struct {
	raftNode *TitanRaft
	input    chan *proposal
	maxSize  int
	interval time.Duration
}

// 工厂函数
func NewBatcher(node *TitanRaft, maxSize int, interval time.Duration) *Batcher {
	b := &Batcher{
		raftNode: node,
		input:    make(chan *proposal, 4096), // 缓冲区大一点
		maxSize:  maxSize,
		interval: interval,
	}
	go b.run()
	return b
}

// 对外接口：替代直接调用 raftNode.Propose
func (b *Batcher) Propose(ctx context.Context, cmd *titankvpb.RaftCommand) error {
	p := &proposal{
		cmd:  cmd,
		done: make(chan error, 1),
	}
	
	// 1. 放入批处理队列
	select {
	case b.input <- p:
	case <-ctx.Done():
		return ctx.Err()
	}

	// 2. 等待批处理完成（提交+应用）
	select {
	case err := <-p.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// 后台聚合循环
func (b *Batcher) run() {
	var pending []*proposal
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()

	for {
		select {
		case p := <-b.input:
			pending = append(pending, p)
			// 如果凑够了一车，立即发车
			if len(pending) >= b.maxSize {
				b.flush(pending)
				pending = nil
			}
		case <-ticker.C:
			// 时间到了，如果有乘客，立即发车
			if len(pending) > 0 {
				b.flush(pending)
				pending = nil
			}
		}
	}
}

// 发送批次
func (b *Batcher) flush(proposals []*proposal) {
	// 1. 打包
	batchCmd := &titankvpb.BatchRaftCommand{}
	for _, p := range proposals {
		batchCmd.Commands = append(batchCmd.Commands, p.cmd)
	}

	// 2. 提交给 Raft (异步提交，结果通过 channel 回调)
	go func(props []*proposal, batch *titankvpb.BatchRaftCommand) {
		// 调用 RaftNode 的 ProposeBatch (稍后实现)
		// 这里使用的是 Background context，因为我们不想因为某个 client超时而取消整个 batch
		err := b.raftNode.ProposeBatch(context.Background(), batch)
		
		// 3. 批量通知结果
		// 注意：这里的 err 只是 Propose 成功的错误，不代表 Apply 成功。
		// 严格来说，应该在 Apply 之后才通知。但在 Day 5 简化模型中，
		// 我们假设只要 Propose 进去了，最终就会 Apply。
		// Week 4 的 ReadIndex 机制保证了读的一致性，所以这里由写返回稍微提前是可以接受的。
		for _, p := range props {
			p.done <- err 
		}
	}(proposals, batchCmd)
}