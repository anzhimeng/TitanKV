package client

import (
	"bytes"
	"sync"

	"titankv/pd/api/pdpb"
	"github.com/google/btree"
)

// 包装 Region 信息以存入 BTree
type regionItem struct {
	region *pdpb.Region
	leader *pdpb.Peer
}

// BTree 比较接口: 按 StartKey 排序
func (r *regionItem) Less(than btree.Item) bool {
	return bytes.Compare(r.region.StartKey, than.(*regionItem).region.StartKey) < 0
}

type RegionCache struct {
	mu    sync.RWMutex
	tree  *btree.BTree
	// 辅助 Map: StoreID -> Address (用于解析 Peer 对应的真实 IP)
	stores map[uint64]string
}

func NewRegionCache() *RegionCache {
	return &RegionCache{
		tree:   btree.New(32), // degree 32
		stores: make(map[uint64]string),
	}
}

// 查找 Key 所在的 Region
func (c *RegionCache) Search(key []byte) (*pdpb.Region, *pdpb.Peer) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var target *regionItem

	// BTree 搜索逻辑：找到最后一个 StartKey <= key 的 Item
	c.tree.DescendLessOrEqual(&regionItem{region: &pdpb.Region{StartKey: key}}, func(i btree.Item) bool {
		target = i.(*regionItem)
		return false // 找到一个就停止
	})

	if target == nil {
		return nil, nil
	}

	// 检查 Key 是否真的在 [Start, End) 范围内
	// EndKey 为空表示无穷大
	if len(target.region.EndKey) > 0 && bytes.Compare(key, target.region.EndKey) >= 0 {
		return nil, nil
	}

	return target.region, target.leader
}

// 更新缓存 (通常在 PD 返回新路由或 Server 报错时调用)
func (c *RegionCache) UpdateRegion(region *pdpb.Region, leader *pdpb.Peer) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 删除旧的重叠 Region (简化版：直接覆盖/插入)
	// 生产环境需要处理 Range 重叠清理，这里假设 PD 返回的是最新的
	c.tree.ReplaceOrInsert(&regionItem{
		region: region,
		leader: leader,
	})
}

// 移除缓存 (当 Server 报 KeyNotInRegion 时)
func (c *RegionCache) Invalidate(key []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// 找到 Key 对应的 Region 并删除
	var target *regionItem
	c.tree.DescendLessOrEqual(&regionItem{region: &pdpb.Region{StartKey: key}}, func(i btree.Item) bool {
		target = i.(*regionItem)
		return false
	})
	
	if target != nil {
		c.tree.Delete(target)
	}
}

// 更新 Store 地址缓存
func (c *RegionCache) UpdateStore(storeID uint64, addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stores[storeID] = addr
}

func (c *RegionCache) GetStoreAddr(storeID uint64) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stores[storeID]
}