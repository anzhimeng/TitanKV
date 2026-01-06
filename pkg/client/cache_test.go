package client

import (
	"testing"
	"titankv/pd/api/pdpb"
)

func TestRegionCache(t *testing.T) {
	cache := NewRegionCache()
    // 模拟 Region: [a, m)
    r1 := &pdpb.Region{
        Id:       1,
        StartKey: []byte("a"),
        EndKey:   []byte("m"),
        // 【建议新增】初始化 Epoch，养成好习惯
        RegionEpoch: &pdpb.RegionEpoch{ConfVer: 1, Version: 1}, 
    }
	l1 := &pdpb.Peer{Id: 1, StoreId: 10}
	cache.UpdateRegion(r1, l1)

	// 模拟 Region: [m, z) -> Leader 2
	r2 := &pdpb.Region{Id: 2, StartKey: []byte("m"), EndKey: []byte("z")}
	l2 := &pdpb.Peer{Id: 2, StoreId: 20}
	cache.UpdateRegion(r2, l2)

	// 测试 1: 查 "b" -> 应该命中 r1
	reg, peer := cache.Search([]byte("b"))
	if reg == nil || reg.Id != 1 {
		t.Errorf("Search 'b' failed. Got %v", reg)
	}
	if peer.StoreId != 10 {
		t.Errorf("Search 'b' peer wrong")
	}

	// 测试 2: 查 "m" -> 应该命中 r2 (左闭)
	reg, peer = cache.Search([]byte("m"))
	if reg == nil || reg.Id != 2 {
		t.Errorf("Search 'm' failed")
	}

	// 测试 3: 查 "z" -> 应该未命中 (右开)
	reg, _ = cache.Search([]byte("z"))
	if reg != nil {
		t.Errorf("Search 'z' should be nil")
	}

	// 测试 4: Invalidate "b" -> 再查应该未命中
	cache.Invalidate([]byte("b"))
	reg, _ = cache.Search([]byte("b"))
	if reg != nil {
		t.Errorf("Invalidate failed")
	}
}