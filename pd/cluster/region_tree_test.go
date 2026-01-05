package cluster

import (
	"testing"
	"titankv/pd/api/pdpb"
)

func TestRegionTreeSearch(t *testing.T) {
	tree := NewRegionTree()

	// 构造 3 个 Region:
	// Region 1: [nil, "k1")  -- 负无穷到 k1
	// Region 2: ["k1", "k5")
	// Region 3: ["k5", nil)  -- k5 到 正无穷
	
	r1 := &pdpb.Region{Id: 1, StartKey: []byte{}, EndKey: []byte("k1")}
	r2 := &pdpb.Region{Id: 2, StartKey: []byte("k1"), EndKey: []byte("k5")}
	r3 := &pdpb.Region{Id: 3, StartKey: []byte("k5"), EndKey: []byte{}}

	tree.Update(r1)
	tree.Update(r2)
	tree.Update(r3)

	tests := []struct {
		key      string
		expectID uint64
	}{
		{"", 1},      // Start Key of R1
		{"abc", 1},   // Inside R1
		{"k1", 2},    // Start Key of R2 (Boundary)
		{"k3", 2},    // Inside R2
		{"k499", 2},  // Inside R2
		{"k5", 3},    // Start Key of R3
		{"zzz", 3},   // Inside R3
	}

	for _, tt := range tests {
		res := tree.Search([]byte(tt.key))
		if res == nil {
			t.Errorf("Search(%s) returned nil", tt.key)
		} else if res.Id != tt.expectID {
			t.Errorf("Search(%s) expected Region %d, got %d", tt.key, tt.expectID, res.Id)
		}
	}
}

// 测试空洞 (Hole)
func TestRegionTreeHole(t *testing.T) {
	tree := NewRegionTree()
	
	// Region 1: ["a", "b")
	// Region 3: ["c", "d")
	// 缺失 ["b", "c")
	
	tree.Update(&pdpb.Region{Id: 1, StartKey: []byte("a"), EndKey: []byte("b")})
	tree.Update(&pdpb.Region{Id: 3, StartKey: []byte("c"), EndKey: []byte("d")})

	// Search "b" (Hole)
	if r := tree.Search([]byte("b")); r != nil {
		t.Errorf("Search('b') should return nil, got %v", r)
	}
}