package client

import (
	"context"
	"testing"
	"titankv/pd/api/pdpb"
)

func TestFollowerReadRouting(t *testing.T) {
	// 1. Setup Client with Cache
	c := &Client{
		cache: NewRegionCache(),
	}

	// 2. Mock Region with 3 Peers (1 Leader, 2 Followers)
	regionID := uint64(1)
	leaderPeer := &pdpb.Peer{Id: 101, StoreId: 1}
	followerPeer1 := &pdpb.Peer{Id: 102, StoreId: 2}
	followerPeer2 := &pdpb.Peer{Id: 103, StoreId: 3}

	region := &pdpb.Region{
		Id:       regionID,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers:    []*pdpb.Peer{leaderPeer, followerPeer1, followerPeer2},
	}

	// 3. Populate Cache
	// UpdateRegion updates the BTree with region info
	// Note: We need to use a wrapper or manually insert if UpdateRegion is not enough?
	// UpdateRegion calls ReplaceOrInsert which puts regionItem into BTree.
	// regionItem.region is what we need.
	c.cache.UpdateRegion(region, leaderPeer)
	
	// Update Store Addrs
	c.cache.UpdateStore(1, "leader-addr:9091")
	c.cache.UpdateStore(2, "follower-1:9091")
	c.cache.UpdateStore(3, "follower-2:9091")

	// 4. Test Normal Read (Should go to Leader)
	ctx := context.Background()
	key := []byte("key1")
	addr, err := c.getAddrForReq(ctx, regionID, key)
	if err != nil {
		t.Fatalf("Normal read failed: %v", err)
	}
	if addr != "leader-addr:9091" {
		t.Errorf("Expected leader address, got %s", addr)
	}

	// 5. Test Follower Read (Should go to Follower)
	followerCtx := WithFollowerRead(ctx)
	
	// Run multiple times to verify it hits followers
	hitFollower := false
	hitLeader := false 

	for i := 0; i < 20; i++ {
		addr, err := c.getAddrForReq(followerCtx, regionID, key)
		if err != nil {
			t.Fatalf("Follower read failed: %v", err)
		}
		if addr == "leader-addr:9091" {
			hitLeader = true
		} else if addr == "follower-1:9091" || addr == "follower-2:9091" {
			hitFollower = true
		} else {
			t.Errorf("Unknown address: %s", addr)
		}
	}

	if hitLeader {
		t.Errorf("Follower Read should not hit Leader when followers are available")
	}
	if !hitFollower {
		t.Errorf("Follower Read should hit Followers")
	}
}
