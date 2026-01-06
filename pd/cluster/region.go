package cluster

import (
	"bytes"
	"titankv/pd/api/pdpb"

	"github.com/google/btree"
)

// RegionInfo 包装了 Proto 和运行时状态 (比如 Leader 在哪)
type RegionInfo struct {
	Meta   *pdpb.Region
	Leader *pdpb.Peer
    
    // 运行时统计 (用于调度)
    ApproximateSize uint64
    ApproximateKeys uint64
}

func NewRegionInfo(region *pdpb.Region, leader *pdpb.Peer) *RegionInfo {
	return &RegionInfo{
		Meta:   region,
		Leader: leader,
	}
}

// B-Tree 接口实现: 按 StartKey 排序
func (r *RegionInfo) Less(than btree.Item) bool {
	return bytes.Compare(r.Meta.StartKey, than.(*RegionInfo).Meta.StartKey) < 0
}

// 复制一份，防止并发修改
func (r *RegionInfo) Clone() *RegionInfo {
    // Proto 的 Clone 需要 deep copy，这里简化处理
    metaCopy := *r.Meta
    return &RegionInfo{
        Meta:            &metaCopy,
        Leader:          r.Leader,
        ApproximateSize: r.ApproximateSize,
        ApproximateKeys: r.ApproximateKeys,
    }
}