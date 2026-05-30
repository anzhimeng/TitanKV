package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"titankv/pkg/client"
	"titankv/pkg/txn"
)

func main() {
	pdAddr := flag.String("pd", "127.0.0.1:9000", "pd address")
	concurrency := flag.Int("c", 50, "concurrency")
	reqPerWorker := flag.Int("n", 200, "requests per worker")
	keysPerTxn := flag.Int("k", 8, "keys per txn")
	flag.Parse()

	totalRequests := (*concurrency) * (*reqPerWorker)
	fmt.Printf("🔥 TitanKV 性能压测工具\n")
	fmt.Printf("配置: Concurrency=%d, ReqPerWorker=%d, Total=%d, KeysPerTxn=%d\n", *concurrency, *reqPerWorker, totalRequests, *keysPerTxn)
	fmt.Println("------------------------------------------------")
	runTxnBenchmark(*pdAddr, *concurrency, *reqPerWorker, *keysPerTxn)
}

func runTxnBenchmark(pdAddr string, concurrency, reqPerWorker, keysPerTxn int) {
	var wg sync.WaitGroup
	var successOps uint64
	var failOps uint64

	c, err := client.NewClient(pdAddr)
	if err != nil {
		log.Printf("client init fail: %v", err)
		return
	}

	startTime := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			baseID := workerID * reqPerWorker * keysPerTxn
			warmupKey := fmt.Sprintf("bench-txn-key-%d", baseID)
			_, _ = c.LocateLeader(context.Background(), []byte(warmupKey))
			for j := 0; j < reqPerWorker; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
				tx, err := txn.NewTransaction(ctx, c)
				if err != nil {
					cancel()
					atomic.AddUint64(&failOps, 1)
					continue
				}

				startID := baseID + j*keysPerTxn
				for k := 0; k < keysPerTxn; k++ {
					id := startID + k
					key := fmt.Sprintf("bench-txn-key-%d", id)
					val := fmt.Sprintf("bench-txn-val-%d", id)
					tx.Set([]byte(key), []byte(val))
				}

				err = tx.Commit(ctx)
				cancel()
				if err != nil {
					currFail := atomic.AddUint64(&failOps, 1)
					if currFail <= 5 {
						log.Printf("Txn commit failed: %v", err)
					}
				} else {
					atomic.AddUint64(&successOps, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)
	tps := float64(successOps) / duration.Seconds()

	fmt.Println("\n================ 压测结果 (Txn) ================")
	fmt.Printf("总耗时:     %v\n", duration)
	fmt.Printf("成功事务:   %d\n", successOps)
	fmt.Printf("失败事务:   %d\n", failOps)
	fmt.Printf("Txn TPS:    %.2f txn/s\n", tps)
	stats := c.GetConflictStats()
	fmt.Printf("冲突细分:   prewrite_key_locked=%d prewrite_key_locked_pri=%d prewrite_key_locked_sec=%d prewrite_write_conflict=%d prewrite_write_pri=%d prewrite_write_sec=%d prewrite_epoch_not_match=%d commit_key_locked=%d commit_lock_mismatch=%d commit_epoch_not_match=%d get_key_locked=%d get_key_locked_pri=%d get_key_locked_sec=%d get_epoch_not_match=%d resolve_no_action=%d resolve_rollback=%d resolve_commit=%d resolve_lock_not_exist=%d resolve_ttl_expire=%d resolve_error=%d\n",
		stats.PrewriteKeyLocked,
		stats.PrewriteKeyLockedPri,
		stats.PrewriteKeyLockedSec,
		stats.PrewriteWriteConflict,
		stats.PrewriteWritePri,
		stats.PrewriteWriteSec,
		stats.PrewriteEpochNotMatch,
		stats.CommitKeyLocked,
		stats.CommitLockMismatch,
		stats.CommitEpochNotMatch,
		stats.GetKeyLocked,
		stats.GetKeyLockedPri,
		stats.GetKeyLockedSec,
		stats.GetEpochNotMatch,
		stats.ResolveNoAction,
		stats.ResolveRollback,
		stats.ResolveCommit,
		stats.ResolveLockNotExist,
		stats.ResolveTtlExpire,
		stats.ResolveError,
	)
	fmt.Println("=================================================")
}
