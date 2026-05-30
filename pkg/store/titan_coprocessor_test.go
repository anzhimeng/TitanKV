package store

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"titankv/api/titankvpb"
)

func TestCoprocessor(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/titankv_coprocessor_test_%d", time.Now().UnixNano())
	defer os.RemoveAll(dbPath)

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	s, err := Open(dbPath, opts)
	assert.NoError(t, err)
	defer s.Close()

	// 1. Prepare data (MVCC)
	// Write 10 keys: k0..k9, value = "1" (for easy sum)
	startTS := uint64(100)
	commitTS := uint64(110)
	ttl := uint64(0)

	var keys [][]byte
	var mutations []*titankvpb.Mutation

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("k%d", i))
		val := []byte("1")
		keys = append(keys, key)
		mutations = append(mutations, &titankvpb.Mutation{
			Op:    titankvpb.Mutation_Put,
			Key:   key,
			Value: val,
		})
	}

	// Prewrite
	err = s.Prewrite(mutations, keys[0], startTS, ttl)
	assert.NoError(t, err)

	// Commit
	err = s.Commit(keys, startTS, commitTS)
	assert.NoError(t, err)

	// 2. Test Coprocessor Count
	req := &CoprocessorRequest{
		Type:    CoprocessorTypeCount,
		StartTS: commitTS + 1, // Read after commit
	}
	resp, err := s.ExecuteCoprocessor(req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, uint64(10), resp.Count, "Count should be 10")

	// 3. Test Coprocessor Sum
	reqSum := &CoprocessorRequest{
		Type:    CoprocessorTypeSum,
		StartTS: commitTS + 1,
	}
	respSum, err := s.ExecuteCoprocessor(reqSum)
	assert.NoError(t, err)
	assert.NotNil(t, respSum)
	assert.Equal(t, int64(10), respSum.Sum, "Sum should be 10 (10 * 1)")
    
    // 4. Test Coprocessor Count with Range
    // k0, k1, k2 (start=k0, end=k3)
    reqRange := &CoprocessorRequest{
        Type:     CoprocessorTypeCount,
        StartKey: []byte("k0"),
        EndKey:   []byte("k3"),
        StartTS:  commitTS + 1,
    }
    respRange, err := s.ExecuteCoprocessor(reqRange)
    assert.NoError(t, err)
    // k0, k1, k2 are in range [k0, k3)
    assert.Equal(t, uint64(3), respRange.Count, "Count with range k0-k3 should be 3")

    // 5. Test Coprocessor Filter
    // Filter values > "1" (none, all are "1")
    reqFilter := &CoprocessorRequest{
        Type:           CoprocessorTypeCount,
        StartTS:        commitTS + 1,
        FilterValue:    []byte("1"),
        FilterOperator: FilterOperatorGreater,
    }
    respFilter, err := s.ExecuteCoprocessor(reqFilter)
    assert.NoError(t, err)
    assert.Equal(t, uint64(0), respFilter.Count, "Count > '1' should be 0")

    // Filter values == "1" (all 10)
    reqFilterEq := &CoprocessorRequest{
        Type:           CoprocessorTypeCount,
        StartTS:        commitTS + 1,
        FilterValue:    []byte("1"),
        FilterOperator: FilterOperatorEqual,
    }
    respFilterEq, err := s.ExecuteCoprocessor(reqFilterEq)
    assert.NoError(t, err)
    assert.Equal(t, uint64(10), respFilterEq.Count, "Count == '1' should be 10")
}
