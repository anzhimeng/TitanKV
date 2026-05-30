package service

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"titankv/api/titankvpb"
	"titankv/pkg/store"

	"github.com/stretchr/testify/assert"
)

// TestBankTransfer simulates concurrent bank transfers to verify transaction atomicity and isolation.
func TestBankTransfer(t *testing.T) {
	// 1. Setup Store
	dir, err := os.MkdirTemp("", "titan-bank-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := store.DefaultOptions()
	opts.CreateIfMissing = true
	// Enable BlockCache/BloomFilter to mimic production
	opts.BlockCacheSize = 8 * 1024 * 1024
	opts.BloomFilterBits = 10
	
	s, err := store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 2. Constants & Initialization
	numAccounts := 5
	initialBalance := 1000
	totalExpected := numAccounts * initialBalance
	
	// Reduce iterations for faster feedback, but keep concurrency high
	numWorkers := 10
	numTransfers := 50 

	var ts uint64 = 1 // Simulated TSO

	accounts := make([]string, numAccounts)
	t.Log("Initializing accounts...")
	for i := 0; i < numAccounts; i++ {
		accounts[i] = fmt.Sprintf("acc-%d", i)
		key := []byte(accounts[i])
		val := []byte(strconv.Itoa(initialBalance))
		
		// Initial Write (using 2PC)
		startTS := atomic.AddUint64(&ts, 1)
		mut := &titankvpb.Mutation{
			Op:    titankvpb.Mutation_Put,
			Key:   key,
			Value: val,
		}
		// Primary = key
		err := s.PrewriteAsync([]*titankvpb.Mutation{mut}, key, startTS, 1000, 0, false, nil)
		assert.NoError(t, err)
		
		commitTS := atomic.AddUint64(&ts, 1)
		err = s.Commit([][]byte{key}, startTS, commitTS)
		assert.NoError(t, err)
	}

	// 3. Run Concurrent Transfers
	t.Logf("Starting %d workers, %d transfers each...", numWorkers, numTransfers)
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			
			for j := 0; j < numTransfers; j++ {
				// Pick two distinct accounts
				fromIdx := r.Intn(numAccounts)
				toIdx := r.Intn(numAccounts)
				if fromIdx == toIdx {
					toIdx = (fromIdx + 1) % numAccounts
				}
				fromKey := []byte(accounts[fromIdx])
				toKey := []byte(accounts[toIdx])
				amount := 10

				// Transaction Loop (Retry until success)
				for {
					startTS := atomic.AddUint64(&ts, 1)
					
					// Step A: Read Balances
					fromValBytes, err1 := s.MvccGet(fromKey, startTS)
					toValBytes, err2 := s.MvccGet(toKey, startTS)
					
					// Handle Read Errors (Conflict or NotFound)
					if err1 != nil || err2 != nil {
						// Simple backoff on any error (Lock conflict is common)
						time.Sleep(time.Millisecond * time.Duration(r.Intn(5)+1))
						continue
					}
					
					fromBal, _ := strconv.Atoi(string(fromValBytes))
					toBal, _ := strconv.Atoi(string(toValBytes))
					
					if fromBal < amount {
						// Insufficient funds, stop this transaction attempt (but count as done)
						break 
					}
					
					newFrom := fromBal - amount
					newTo := toBal + amount
					
					// Step B: Prewrite
					mut1 := &titankvpb.Mutation{
						Op:    titankvpb.Mutation_Put,
						Key:   fromKey,
						Value: []byte(strconv.Itoa(newFrom)),
					}
					mut2 := &titankvpb.Mutation{
						Op:    titankvpb.Mutation_Put,
						Key:   toKey,
						Value: []byte(strconv.Itoa(newTo)),
					}
					
					// Use fromKey as Primary
					secondaries := [][]byte{toKey}
					err := s.PrewriteAsync([]*titankvpb.Mutation{mut1, mut2}, fromKey, startTS, 1000, 0, false, secondaries)
					if err != nil {
						// Write Conflict -> Retry
						time.Sleep(time.Millisecond * time.Duration(r.Intn(5)+1))
						continue
					}
					
					// Step C: Commit
					commitTS := atomic.AddUint64(&ts, 1)
					err = s.Commit([][]byte{fromKey, toKey}, startTS, commitTS)
					if err != nil {
						// Commit failed (e.g. TTL expired?) -> Retry
						time.Sleep(time.Millisecond * time.Duration(r.Intn(5)+1))
						continue
					}
					
					// Success
					break
				}
			}
		}(i)
	}
	
	wg.Wait()
	
	// 4. Verify Total Balance
	verifyTS := atomic.AddUint64(&ts, 1)
	total := 0
	t.Log("Verifying final balances...")
	for i := 0; i < numAccounts; i++ {
		key := []byte(accounts[i])
		valBytes, err := s.MvccGet(key, verifyTS)
		assert.NoError(t, err)
		bal, _ := strconv.Atoi(string(valBytes))
		total += bal
		t.Logf("Account %s: %d", accounts[i], bal)
	}
	
	assert.Equal(t, totalExpected, total, "Total balance must remain constant after concurrent transfers")
}
