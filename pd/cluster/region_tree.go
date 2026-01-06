package cluster

import (
	"bytes"
	// "sync" // 【修复】删除未使用的 sync

	"titankv/pd/api/pdpb"

	"github.com/google/btree"
)

// RegionItem 是 B-Tree 中的元素
type RegionItem struct {
	Region *RegionInfo
}

// Less 接口实现：按 StartKey 排序
func (r RegionItem) Less(than btree.Item) bool {
	// 【修复】访问 StartKey 需要通过 .Meta
	return bytes.Compare(r.Region.Meta.StartKey, than.(RegionItem).Region.Meta.StartKey) < 0
}

type RegionTree struct {
	tree *btree.BTree
	// mu   sync.RWMutex // 【修复】移除锁，由 RaftCluster 统一加锁
}

func NewRegionTree() *RegionTree {
	return &RegionTree{
		tree: btree.New(32),
	}
}

// Update 更新或插入 Region
func (t *RegionTree) Update(region *RegionInfo) {
	// t.mu.Lock() // 移除
	// defer t.mu.Unlock()

	item := RegionItem{Region: region}
	t.tree.ReplaceOrInsert(item)
}

// Search 查找包含 key 的 Region
func (t *RegionTree) Search(key []byte) *RegionInfo {
	// t.mu.RLock() // 移除
	// defer t.mu.RUnlock()

	var found *RegionInfo

	// 【修复】构造正确的嵌套结构用于搜索
	// RegionItem -> RegionInfo -> Meta -> StartKey
	searchItem := RegionItem{
		Region: &RegionInfo{
			Meta: &pdpb.Region{StartKey: key},
		},
	}

	t.tree.DescendLessOrEqual(searchItem, func(i btree.Item) bool {
		region := i.(RegionItem).Region
		
		// 检查 key 是否在 [Start, End) 区间内
		// 【修复】访问 EndKey 需要通过 .Meta
		if len(region.Meta.EndKey) > 0 && bytes.Compare(key, region.Meta.EndKey) >= 0 {
			// key >= EndKey，属于空洞
		} else {
			found = region
		}
		return false
	})

	return found
}

// Remove 删除 Region
func (t *RegionTree) Remove(region *RegionInfo) {
	// t.mu.Lock() // 移除
	// defer t.mu.Unlock()
	t.tree.Delete(RegionItem{Region: region})
}

// Scan 扫描
func (t *RegionTree) Scan(startKey []byte, limit int) []*RegionInfo {
	// t.mu.RLock() // 移除
	// defer t.mu.RUnlock()

	var regions []*RegionInfo
	
	// 【修复】构造正确的搜索项
	searchItem := RegionItem{
		Region: &RegionInfo{
			Meta: &pdpb.Region{StartKey: startKey},
		},
	}

	t.tree.AscendGreaterOrEqual(searchItem, func(i btree.Item) bool {
		if len(regions) >= limit {
			return false
		}
		regions = append(regions, i.(RegionItem).Region)
		return true
	})
	
	return regions
}