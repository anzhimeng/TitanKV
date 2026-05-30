package service

import (
	"context"
	"fmt"
	"os"
	"testing"

	"titankv/api/titankvpb"
	"titankv/pkg/raftstore"
	"titankv/pkg/store"

	"github.com/stretchr/testify/assert"
)

func TestCoprocessorIntegration(t *testing.T) {
	// 1. Setup Store
	dir, err := os.MkdirTemp("", "titan-coprocessor-integration")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := store.DefaultOptions()
	opts.CreateIfMissing = true
	// Enable BlockCache and BloomFilter for better performance simulation
	opts.BlockCacheSize = 8 * 1024 * 1024
	opts.BloomFilterBits = 10
	
	s, err := store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 2. Setup Server (Partial)
	// We only need the store field for ExecuteCoprocessor
	server := &Server{
		store: s,
	}

	// 3. Prepare Data
	// Write 1000 keys: "0000" to "0999"
	// Value: same as key
	// RegionID: 1
	regionID := uint64(1)
	count := 1000
	
	ctx := context.Background()
	startTS := uint64(100)
	commitTS := uint64(110)
	
	t.Logf("Writing %d keys...", count)
	for i := 0; i < count; i++ {
		keyStr := fmt.Sprintf("%04d", i) // "0000" - "0999"
		valStr := keyStr
		
		userKey := []byte(keyStr)
		encodedKey := raftstore.DataKey(regionID, userKey) // z{RegionID}_{Key}
		
		// Prewrite
		mut := &titankvpb.Mutation{
			Op:    titankvpb.Mutation_Put,
			Key:   encodedKey,
			Value: []byte(valStr),
		}
		
		err := s.PrewriteAsync([]*titankvpb.Mutation{mut}, encodedKey, startTS, 0, 0, false, nil)
		assert.NoError(t, err)
		
		// Commit
		err = s.Commit([][]byte{encodedKey}, startTS, commitTS)
		assert.NoError(t, err)
	}
	
	// 4. Test Coprocessor: Count (All)
	t.Log("Testing Coprocessor: Count All")
	req := &titankvpb.CoprocessorRequest{
		Context: &titankvpb.RegionContext{
			RegionId: regionID,
		},
		Type: titankvpb.CoprocessorRequest_COUNT,
		StartTs: commitTS + 1, // Read after commit
	}
	
	resp, err := server.ExecuteCoprocessor(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, uint64(count), resp.Count)
	
	// 5. Test Coprocessor: Filter Greater "0500"
	// "0501" ... "0999" -> 499 keys
	t.Log("Testing Coprocessor: Filter Greater '0500'")
	req = &titankvpb.CoprocessorRequest{
		Context: &titankvpb.RegionContext{
			RegionId: regionID,
		},
		Type: titankvpb.CoprocessorRequest_COUNT,
		StartTs: commitTS + 1,
		FilterValue: []byte("0500"),
		FilterOperator: titankvpb.CoprocessorRequest_GREATER,
	}
	resp, err = server.ExecuteCoprocessor(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, uint64(499), resp.Count)
	
	// 6. Test Coprocessor: Sum (Values are strings "0000"..."0999")
	// Sum(0..999) = 999 * 1000 / 2 = 499500
	t.Log("Testing Coprocessor: Sum All")
	req = &titankvpb.CoprocessorRequest{
		Context: &titankvpb.RegionContext{
			RegionId: regionID,
		},
		Type: titankvpb.CoprocessorRequest_SUM,
		StartTs: commitTS + 1,
	}
	resp, err = server.ExecuteCoprocessor(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, int64(499500), resp.Sum)
	
	// 7. Test Coprocessor: Sum with Filter (Less "0010")
	// "0000" ... "0009" -> Sum(0..9) = 45
	t.Log("Testing Coprocessor: Sum with Filter Less '0010'")
	req = &titankvpb.CoprocessorRequest{
		Context: &titankvpb.RegionContext{
			RegionId: regionID,
		},
		Type: titankvpb.CoprocessorRequest_SUM,
		StartTs: commitTS + 1,
		FilterValue: []byte("0010"),
		FilterOperator: titankvpb.CoprocessorRequest_LESS,
	}
	resp, err = server.ExecuteCoprocessor(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, int64(45), resp.Sum)

    // 8. Test Region Isolation
    // Query Region 2 (should be empty)
    t.Log("Testing Coprocessor: Region Isolation (Region 2)")
    req = &titankvpb.CoprocessorRequest{
        Context: &titankvpb.RegionContext{
            RegionId: regionID + 1,
        },
        Type: titankvpb.CoprocessorRequest_COUNT,
        StartTs: commitTS + 1,
    }
    resp, err = server.ExecuteCoprocessor(ctx, req)
    assert.NoError(t, err)
    assert.Equal(t, uint64(0), resp.Count)
}
