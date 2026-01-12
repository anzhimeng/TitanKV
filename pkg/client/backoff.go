package client

import (
	"context"
	"math/rand"
	"time"
)

const (
	baseDelay = 10 * time.Millisecond
	maxDelay  = 1000 * time.Millisecond
)

type Backoffer struct {
	attempts int
	ctx      context.Context
}

func NewBackoffer(ctx context.Context) *Backoffer {
	return &Backoffer{ctx: ctx}
}

// Sleep 等待一段时间，如果 Context 结束则返回 error
func (b *Backoffer) Sleep() error {
	delay := baseDelay * time.Duration(1<<b.attempts)
	if delay > maxDelay {
		delay = maxDelay
	}
	// 增加一点随机抖动 (Jitter)，防止惊群
	delay += time.Duration(rand.Intn(int(baseDelay)))
	
	b.attempts++

	select {
	case <-time.After(delay):
		return nil
	case <-b.ctx.Done():
		return b.ctx.Err()
	}
}