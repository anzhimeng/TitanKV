package tso

import (
	"context"
	"encoding/binary"
	"errors"
	"log"
	"sync"
	"time"

	"titankv/pd/api/pdpb"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// 提前多久去更新下一个窗口 (例如提前 50ms)
	updateTimestampGuard = time.Millisecond * 50
	// 每次向 Etcd 预申请的窗口大小 (3秒)
	tsoSaveInterval = time.Second * 3
	// Etcd 存储 Key
	tsoPath = "/pd/tso"
)

type Allocator struct {
	client *clientv3.Client
	mu     sync.Mutex

	// 当前内存中允许分配的最大物理时间
	tsoWindowEnd time.Time

	// 上次分配的时间
	lastPhysical time.Time
	lastLogical  int64
}

func NewAllocator(client *clientv3.Client) *Allocator {
	return &Allocator{
		client: client,
	}
}

// 【新增】启动时初始化：从 Etcd 读取上次保存的时间
func (a *Allocator) Initialize(ctx context.Context) error {
	resp, err := a.client.Get(ctx, tsoPath)
	if err != nil {
		return err
	}

	var lastSavedTime time.Time

	if len(resp.Kvs) == 0 {
		// 第一次启动，使用当前时间
		lastSavedTime = time.Now()
	} else {
		// 解析 Etcd 中的时间
		val := resp.Kvs[0].Value
		tsMilli := decodeInt64(val)
		lastSavedTime = time.UnixMilli(tsMilli)
	}

	// 立即申请一个新的窗口
	// 下一个窗口结束时间 = max(当前时间, 上次保存时间) + 3秒
	now := time.Now()
	if lastSavedTime.After(now) {
		now = lastSavedTime
	}
	now = time.UnixMilli(now.UnixMilli())

	newEnd := now.Add(tsoSaveInterval)

	// 持久化到 Etcd
	if err := a.saveTsoToEtcd(ctx, newEnd); err != nil {
		return err
	}

	a.mu.Lock()
	a.tsoWindowEnd = newEnd
	a.lastPhysical = now // 指针拨到现在
	a.lastLogical = 0
	a.mu.Unlock()

	log.Printf("[TSO] Initialized. Window end: %v", newEnd)
	return nil
}

// Generate 生成 TSO
func (a *Allocator) Generate(count uint32) (pdpb.Timestamp, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.lastPhysical.IsZero() {
		return pdpb.Timestamp{}, errors.New("timestamp not synced yet")
	}

	// 1. 获取物理时间
	now := time.UnixMilli(time.Now().UnixMilli())

	// 2. 追赶：如果系统时钟回退，强制使用 lastPhysical
	if now.Before(a.lastPhysical) {
		now = a.lastPhysical
	}

	// 3. 检查窗口耗尽
	if now.After(a.tsoWindowEnd) {
		return pdpb.Timestamp{}, errors.New("timestamp window exhausted, waiting for sync")
	}

	// 4. 更新逻辑时钟
	if now.Equal(a.lastPhysical) {
		nextLogical := a.lastLogical + int64(count)
		if nextLogical >= (1 << 18) {
			time.Sleep(time.Millisecond)
			now = time.Now()
			minNext := time.UnixMilli(a.lastPhysical.UnixMilli() + 1)
			if now.Before(minNext) {
				now = minNext
			}
			if now.After(a.tsoWindowEnd) {
				return pdpb.Timestamp{}, errors.New("timestamp window exhausted, waiting for sync")
			}
			a.lastLogical = int64(count) - 1
		} else {
			a.lastLogical = nextLogical
		}
	} else {
		a.lastLogical = int64(count) - 1
	}

	a.lastPhysical = now

	return pdpb.Timestamp{
		Physical: now.UnixMilli(),
		Logical:  a.lastLogical,
	}, nil
}

// 后台同步循环
func (a *Allocator) SyncLoop(ctx context.Context) {
	ticker := time.NewTicker(updateTimestampGuard)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.updateTso(ctx)
		}
	}
}

func (a *Allocator) updateTso(ctx context.Context) {
	a.mu.Lock()
	windowEnd := a.tsoWindowEnd
	a.mu.Unlock()

	// 如果离过期还早，跳过
	if time.Until(windowEnd) > updateTimestampGuard {
		return
	}

	// 预分配下一个 3秒
	nextEnd := time.Now().Add(tsoSaveInterval)

	// 写入 Etcd
	if err := a.saveTsoToEtcd(ctx, nextEnd); err != nil {
		log.Printf("[TSO] Failed to sync timestamp: %v", err)
		return
	}

	a.mu.Lock()
	a.tsoWindowEnd = nextEnd
	a.mu.Unlock()

	// log.Printf("[TSO] Window updated to %v", nextEnd) // 调试用
}

// 辅助：写入 Etcd
func (a *Allocator) saveTsoToEtcd(ctx context.Context, ts time.Time) error {
	val := encodeInt64(ts.UnixMilli())
	_, err := a.client.Put(ctx, tsoPath, string(val))
	return err
}

// --- 编码工具 ---

func encodeInt64(n int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
}

func decodeInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}
