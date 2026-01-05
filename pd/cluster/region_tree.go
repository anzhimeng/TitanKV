package cluster

import (
	"bytes"
	"sync"

	"titankv/pd/api/pdpb"

	"github.com/google/btree"
)

// RegionItem 是 B-Tree 中的元素
type RegionItem struct {
	Region *pdpb.Region
}

// Less 接口实现：按 StartKey 排序
func (r RegionItem) Less(than btree.Item) bool {
	return bytes.Compare(r.Region.StartKey, than.(RegionItem).Region.StartKey) < 0
}

type RegionTree struct {
	tree *btree.BTree
	mu   sync.RWMutex
}

func NewRegionTree() *RegionTree {
	return &RegionTree{
		// degree=32 是经验值，适合内存索引
		tree: btree.New(32),
	}
}

// Update 更新或插入 Region
func (t *RegionTree) Update(region *pdpb.Region) {
	t.mu.Lock()
	defer t.mu.Unlock()

	item := RegionItem{Region: region}
	t.tree.ReplaceOrInsert(item)
}

// Search 查找包含 key 的 Region
// 返回 nil 表示没找到
func (t *RegionTree) Search(key []byte) *pdpb.Region {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var found *pdpb.Region

	// B-Tree 搜索逻辑：
	// 我们需要找到 StartKey <= key 的最后一个 Region。
	// AscendGreaterOrEqual 找的是 >= key 的。
	// DescendLessOrEqual 找的是 <= key 的。
	
	// 注意：Region 范围是 [StartKey, EndKey)
	// 特例：EndKey 为空表示无穷大
	
	t.tree.DescendLessOrEqual(RegionItem{Region: &pdpb.Region{StartKey: key}}, func(i btree.Item) bool {
		region := i.(RegionItem).Region
		
		// 检查 key 是否在 [Start, End) 区间内
		if len(region.EndKey) > 0 && bytes.Compare(key, region.EndKey) >= 0 {
			// key >= EndKey，说明 key 不在这个 region 里，而是在后面的空洞里
			// 这种情况下，说明该 Key 所在的 Range 还没汇报给 PD (或者数据丢失)
			// found 保持为 nil
		} else {
			found = region
		}
		
		// 只需要找一个（最近的一个），所以返回 false 停止遍历
		return false
	})

	return found
}

// Scan 从 startKey 开始扫描所有 Region
func (t *RegionTree) Scan(startKey []byte, limit int) []*pdpb.Region {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var regions []*pdpb.Region
	
	t.tree.AscendGreaterOrEqual(RegionItem{Region: &pdpb.Region{StartKey: startKey}}, func(i btree.Item) bool {
		if len(regions) >= limit {
			return false
		}
		regions = append(regions, i.(RegionItem).Region)
		return true
	})
	
	return regions
}

// Remove 删除 Region (用于 Merge 或迁移)
func (t *RegionTree) Remove(region *pdpb.Region) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tree.Delete(RegionItem{Region: region})
}