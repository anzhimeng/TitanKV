package txn

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
	"titankv/pkg/client" // Week 8 实现的 Client

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// 批次结构
type batchKeys struct {
	region *pdpb.Region
	muts   []*titankvpb.Mutation
}

// Mutation 表示一次修改
type Mutation struct {
	Key   []byte
	Value []byte // 如果是 Delete，Value 为 nil
	Op    OpType
}

type OpType int

const (
	OpPut    OpType = 0
	OpDelete OpType = 1
)
const prewriteBatchSize = 8192

type ConflictError struct {
	Stage      string
	Kind       string
	Key        []byte
	Primary    []byte
	StartTS    uint64
	ConflictTS uint64
	LockTS     uint64
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("txn conflict stage=%s kind=%s key=%s primary=%s start_ts=%d conflict_ts=%d lock_ts=%d", e.Stage, e.Kind, string(e.Key), string(e.Primary), e.StartTS, e.ConflictTS, e.LockTS)
}

type Transaction struct {
	StartTS uint64
	client  *client.Client // 持有 KV Client 和 PD Client

	// 读写缓冲
	// key -> value (nil 表示 delete)
	buffer map[string][]byte
	// 记录修改顺序 (虽然 Percolator 不依赖顺序，但为了 Debug 方便)
	mutations     []Mutation
	mutationIndex map[string]int

	// 事务状态
	committed bool
	// Pessimistic Transaction fields
	isPessimistic    bool
	forUpdateTS      uint64
	pessimisticLocks map[string]bool
}

// 开启新事务
func NewTransaction(ctx context.Context, c *client.Client) (*Transaction, error) {
	ts, err := c.GetTS(ctx)
	if err != nil {
		return nil, err
	}

	return &Transaction{
		StartTS:       ts,
		client:        c,
		// 【必须初始化】
		buffer:           make(map[string][]byte),
		mutations:        make([]Mutation, 0),
		mutationIndex:    make(map[string]int),
		pessimisticLocks: make(map[string]bool),
	}, nil
}

// EnablePessimistic enables pessimistic transaction mode
func (txn *Transaction) EnablePessimistic(ctx context.Context) error {
	txn.isPessimistic = true
	// For Repeatable Read, for_update_ts is usually the same as start_ts
	// But in some implementations (like TiDB), it might be updated on retry.
	// Here we keep it simple.
	txn.forUpdateTS = txn.StartTS
	return nil
}

// LockKeys acquires pessimistic locks for the given keys
func (txn *Transaction) LockKeys(ctx context.Context, keys [][]byte) error {
	if !txn.isPessimistic {
		return fmt.Errorf("transaction is not pessimistic")
	}

	// 1. Group keys by region
	// We reuse the logic similar to groupMutations but for keys
	groups := make(map[uint64]*batchKeys)
	cache := txn.client.GetRegionCache()

	for _, key := range keys {
		region, _ := cache.Search(key)
		if region == nil {
			if _, err := txn.client.LocateLeader(ctx, key); err != nil {
				return err
			}
			region, _ = cache.Search(key)
			if region == nil {
				return fmt.Errorf("region not found for key %s", string(key))
			}
		}

		if _, ok := groups[region.Id]; !ok {
			groups[region.Id] = &batchKeys{region: region}
		}
		// We create dummy mutations for grouping, only Key matters for locking
		groups[region.Id].muts = append(groups[region.Id].muts, &titankvpb.Mutation{
			Key: key,
			Op:  titankvpb.Mutation_Lock,
		})
	}

	// 2. Send AcquirePessimisticLock requests in parallel
	g, gctx := errgroup.WithContext(ctx)
	for _, batch := range groups {
		batch := batch
		g.Go(func() error {
			req := &titankvpb.AcquirePessimisticLockRequest{
				Context: &titankvpb.RegionContext{
					RegionId:    batch.region.Id,
					RegionEpoch: toTitanEpoch(batch.region.RegionEpoch),
				},
				Mutations:   batch.muts,
				StartTs:     txn.StartTS,
				PrimaryKey:  keys[0], // Use first key as primary for now, or pass primary explicitly
				LockTtl:     3000,
				ForUpdateTs: txn.forUpdateTS,
			}
			
			resp, err := txn.client.AcquirePessimisticLock(gctx, req)
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("acquire pessimistic lock failed: %s", resp.Error)
			}
			if resp.KeyError != nil {
				return fmt.Errorf("key locked: %v", resp.KeyError)
			}
			
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	// 3. Mark keys as locked
	for _, key := range keys {
		txn.pessimisticLocks[string(key)] = true
	}

	return nil
}

func (txn *Transaction) Set(key []byte, value []byte) {
	if txn.buffer == nil {
		txn.buffer = make(map[string][]byte)
	}
	if txn.mutationIndex == nil {
		txn.mutationIndex = make(map[string]int)
	}
	k := string(key)
	txn.buffer[k] = value

	if idx, ok := txn.mutationIndex[k]; ok {
		txn.mutations[idx] = Mutation{
			Key:   key,
			Value: value,
			Op:    OpPut,
		}
		return
	}

	txn.mutations = append(txn.mutations, Mutation{
		Key:   key,
		Value: value,
		Op:    OpPut,
	})
	txn.mutationIndex[k] = len(txn.mutations) - 1
}

func (txn *Transaction) Delete(key []byte) {
	if txn.buffer == nil {
		txn.buffer = make(map[string][]byte)
	}
	if txn.mutationIndex == nil {
		txn.mutationIndex = make(map[string]int)
	}
	k := string(key)
	txn.buffer[k] = nil // nil 表示删除标记

	if idx, ok := txn.mutationIndex[k]; ok {
		txn.mutations[idx] = Mutation{
			Key: key,
			Op:  OpDelete,
		}
		return
	}

	txn.mutations = append(txn.mutations, Mutation{
		Key: key,
		Op:  OpDelete,
	})
	txn.mutationIndex[k] = len(txn.mutations) - 1
}

func (txn *Transaction) Get(ctx context.Context, key []byte) ([]byte, error) {
	k := string(key)

	// 1. 查 Buffer (Read-Your-Writes)
	// 必须先查自己的写缓冲，否则刚刚 Set 的数据读不到
	if val, ok := txn.buffer[k]; ok {
		// 如果 buffer 里是 nil，说明被删除了
		if val == nil {
			return nil, nil // 或者返回 ErrKeyNotFound
		}
		return val, nil
	}

	// 2. 查 Storage (Snapshot Read)
	// 调用 Client 的 SnapshotGet
	// 如果返回 nil, nil 说明数据库里也没这个 Key
	val, err := txn.client.SnapshotGet(ctx, key, txn.StartTS)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func (txn *Transaction) groupMutations(ctx context.Context, mutations []*titankvpb.Mutation) (map[uint64]*batchKeys, error) {
	groups := make(map[uint64]*batchKeys)
	cache := txn.client.GetRegionCache()

	if region, _ := cache.GetSingleRegion(); region != nil && len(region.StartKey) == 0 && len(region.EndKey) == 0 {
		groups[region.Id] = &batchKeys{region: region, muts: mutations}
		return groups, nil
	}

	for _, m := range mutations {
		// 1. 查找路由
		region, _ := cache.Search(m.Key)
		if region == nil {
			// 缓存未命中，强制刷新 (LocateLeader 会更新缓存)
			if _, err := txn.client.LocateLeader(ctx, m.Key); err != nil {
				return nil, err
			}
			region, _ = cache.Search(m.Key)
			if region == nil {
				return nil, fmt.Errorf("region not found for key %s", string(m.Key))
			}
		}

		// 2. 分组
		if _, ok := groups[region.Id]; !ok {
			groups[region.Id] = &batchKeys{region: region}
		}
		groups[region.Id].muts = append(groups[region.Id].muts, m)
	}
	return groups, nil
}

func (txn *Transaction) Commit(ctx context.Context) error {
	// 1. 转换 Mutations (Internal struct -> Proto)
	pbMutations := make([]*titankvpb.Mutation, len(txn.mutations))
	for i, m := range txn.mutations {
		op := titankvpb.Mutation_Put
		if m.Op == OpDelete {
			op = titankvpb.Mutation_Delete
		}
		pbMutations[i] = &titankvpb.Mutation{
			Op:    op,
			Key:   m.Key,
			Value: m.Value,
		}
	}

	if len(pbMutations) == 0 {
		return nil
	}

	// 2. 选择 Primary Key
	primary := pbMutations[0].Key

	var prewriteBatches map[uint64]*batchKeys
	// Async Commit variables
	useAsyncCommit := true // Enable Async Commit (Parallel Commit)
	minCommitTS := txn.StartTS + 1

	// 使用无限循环来处理 ResolveLock 和 Epoch 重试
	for {
		batches, err := txn.groupMutations(ctx, pbMutations)
		if err != nil {
			return err
		}
		if len(batches) == 1 {
			var single *batchKeys
			for _, b := range batches {
				single = b
				break
			}
			if single != nil && len(single.muts) <= prewriteBatchSize {
				// 1PC Optimization: Try to commit immediately if single region
				var onePcCommitTS uint64
				use1PC := false
				
				// Fetch CommitTS for 1PC
				ts, err := txn.client.GetTS(ctx)
				if err == nil && ts > txn.StartTS {
					onePcCommitTS = ts
					use1PC = true
				}


				task := single
				lockInfo, epochRetry, onePcDone, err := func() (*titankvpb.LockInfo, bool, bool, error) {
					req := &titankvpb.PrewriteRequest{
						Context: &titankvpb.RegionContext{
							RegionId:    task.region.Id,
							RegionEpoch: toTitanEpoch(task.region.RegionEpoch),
						},
						Mutations:  task.muts,
						PrimaryKey: primary,
						StartTs:    txn.StartTS,
						LockTtl:    3000,
						Use_1Pc:    use1PC,
						CommitTs:   onePcCommitTS,
					}
					var lastErr error
					for i := 0; i < 3; i++ {
						resp, err := txn.client.SendPrewrite(ctx, req)
						if err == nil {
							if resp.Error != "" {
								if resp.KeyError != nil {
									return resp.KeyError.LockInfo, false, false, nil
								}
								if resp.Conflict != nil {
									return nil, false, false, &ConflictError{
										Stage:      "prewrite",
										Kind:       "write_conflict",
										Key:        resp.Conflict.Key,
										Primary:    resp.Conflict.Primary,
										StartTS:    resp.Conflict.StartTs,
										ConflictTS: resp.Conflict.ConflictTs,
									}
								}
								if resp.Error == "WriteConflict" {
									return nil, false, false, &ConflictError{
										Stage:   "prewrite",
										Kind:    "write_conflict",
										Key:     task.muts[0].Key,
										Primary: primary,
										StartTS: txn.StartTS,
									}
								}
								return nil, false, false, fmt.Errorf("prewrite failed: %s", resp.Error)
							}
							// Check if 1PC succeeded
							if resp.OnePcCommitted {
								return nil, false, true, nil
							}
							return nil, false, false, nil
						}

						st, _ := status.FromError(err)
						if st.Code() == codes.Aborted && st.Message() == "EpochNotMatch" {
							for _, m := range task.muts {
								txn.client.GetRegionCache().Invalidate(m.Key)
							}
							return nil, true, false, err
						}

						lastErr = err
						time.Sleep(5 * time.Millisecond)
					}
					return nil, false, false, lastErr
				}()

				if err != nil {
					if epochRetry {
						time.Sleep(5 * time.Millisecond)
						continue
					}
					return err
				}

				if onePcDone {
					// 1PC Committed successfully!
					return nil
				}

				if lockInfo != nil {
				ok, err := txn.client.ResolveLocks(ctx, lockInfo)
				if err != nil {
					return err
				}

				if ok {
					time.Sleep(20 * time.Millisecond)
					continue
				}
				return &ConflictError{
					Stage:   "prewrite",
					Kind:    "key_locked",
					Key:     lockInfo.Key,
					Primary: lockInfo.PrimaryKey,
					LockTS:  lockInfo.LockVersion,
					StartTS: txn.StartTS,
				}
			}

			prewriteBatches = batches
			break
		}
		}

		g, groupCtx := errgroup.WithContext(ctx)

		// Async Commit Preparation
		var allSecondaries [][]byte
		if useAsyncCommit {
			for _, batch := range batches {
				for _, m := range batch.muts {
					if !bytes.Equal(m.Key, primary) {
						allSecondaries = append(allSecondaries, m.Key)
					}
				}
			}
		}

		type prewriteTask struct {
			region *pdpb.Region
			muts   []*titankvpb.Mutation
		}
		totalTasks := 0
		for _, batch := range batches {
			totalTasks += (len(batch.muts) + prewriteBatchSize - 1) / prewriteBatchSize
		}
		tasks := make([]prewriteTask, 0, totalTasks)
		for _, batch := range batches {
			for i := 0; i < len(batch.muts); i += prewriteBatchSize {
				end := i + prewriteBatchSize
				if end > len(batch.muts) {
					end = len(batch.muts)
				}
				tasks = append(tasks, prewriteTask{
					region: batch.region,
					muts:   batch.muts[i:end],
				})
			}
		}

		lockErrCh := make(chan *titankvpb.LockInfo, len(tasks))
		epochErrCh := make(chan bool, 1)

		// Async Commit TS tracking
		var commitTSMu sync.Mutex
		calculatedCommitTS := minCommitTS

		for _, task := range tasks {
			task := task
			g.Go(func() error {
				isPessimisticLocks := make([]bool, len(task.muts))
				if txn.isPessimistic {
					for i, m := range task.muts {
						if txn.pessimisticLocks[string(m.Key)] {
							isPessimisticLocks[i] = true
						}
					}
				}

				req := &titankvpb.PrewriteRequest{
					Context: &titankvpb.RegionContext{
						RegionId:    task.region.Id,
						RegionEpoch: toTitanEpoch(task.region.RegionEpoch),
					},
					Mutations:         task.muts,
					PrimaryKey:        primary,
					StartTs:           txn.StartTS,
					LockTtl:           3000,
					UseAsyncCommit:    useAsyncCommit,
					MinCommitTs:       minCommitTS,
					IsPessimisticLock: isPessimisticLocks,
					ForUpdateTs:       txn.forUpdateTS,
				}

				// Check if this task contains the Primary Key
				for _, m := range task.muts {
					if bytes.Equal(m.Key, primary) {
						req.Secondaries = allSecondaries
						break
					}
				}

				var lastErr error
				for i := 0; i < 3; i++ {
					resp, err := txn.client.SendPrewrite(groupCtx, req)
					if err == nil {
						if resp.Error != "" {
							if resp.KeyError != nil {
								lockErrCh <- resp.KeyError.LockInfo
								return nil
							}
							if resp.Conflict != nil {
								return &ConflictError{
									Stage:      "prewrite",
									Kind:       "write_conflict",
									Key:        resp.Conflict.Key,
									Primary:    resp.Conflict.Primary,
									StartTS:    resp.Conflict.StartTs,
									ConflictTS: resp.Conflict.ConflictTs,
								}
							}
							if resp.Error == "WriteConflict" {
								return &ConflictError{
									Stage:   "prewrite",
									Kind:    "write_conflict",
									Key:     task.muts[0].Key,
									Primary: primary,
									StartTS: txn.StartTS,
								}
							}
							return fmt.Errorf("prewrite failed: %s", resp.Error)
						}
						if useAsyncCommit {
							commitTSMu.Lock()
							if ts := resp.GetMinCommitTs(); ts > calculatedCommitTS {
								calculatedCommitTS = ts
							}
							commitTSMu.Unlock()
						}
						return nil
					}

					st, _ := status.FromError(err)
					if st.Code() == codes.Aborted && st.Message() == "EpochNotMatch" {
						for _, m := range task.muts {
							txn.client.GetRegionCache().Invalidate(m.Key)
						}
						select {
						case epochErrCh <- true:
						default:
						}
						return err
					}

					lastErr = err
					time.Sleep(20 * time.Millisecond)
				}
				return lastErr
			})
		}

		if err := g.Wait(); err != nil {
			// 检查是否有 Epoch 错误
			select {
			case <-epochErrCh:
				time.Sleep(5 * time.Millisecond)
				continue // 重新 Group，重新 Prewrite
			default:
				// 是真正的网络错误
				return err
			}
		}

		close(lockErrCh)

		// 检查是否遇到锁
		var lockInfo *titankvpb.LockInfo
		for l := range lockErrCh {
			lockInfo = l
			break
		}

		if lockInfo != nil {
			// 【核心】自动 Resolve
			ok, err := txn.client.ResolveLocks(ctx, lockInfo)
			if err != nil {
				return err
			}

			if ok {
				// 成功清理了锁，立即重试 Prewrite
				time.Sleep(20 * time.Millisecond)
				continue
			} else {
				// Resolve 说 "NoAction" (锁没过期)，我们只能失败
				return &ConflictError{
					Stage:   "prewrite",
					Kind:    "key_locked",
					Key:     lockInfo.Key,
					Primary: lockInfo.PrimaryKey,
					LockTS:  lockInfo.LockVersion,
					StartTS: txn.StartTS,
				}
			}
		}

		prewriteBatches = batches
		// Update minCommitTS for Async Commit
		if useAsyncCommit {
			minCommitTS = calculatedCommitTS
		}
		// 全部成功，无锁冲突，进入 Commit 阶段
		break
	}

	// 4. 获取 CommitTS
	var commitTS uint64
	if useAsyncCommit {
		commitTS = minCommitTS
	} else {
		var err error
		for i := 0; i < 3; i++ {
			commitTS, err = txn.client.GetTS(ctx)
			if err != nil {
				return err
			}
			if commitTS > txn.StartTS {
				break
			}
			time.Sleep(20 * time.Microsecond)
		}
		if commitTS <= txn.StartTS {
			return fmt.Errorf("invalid commit ts")
		}
	}

	// 如果是 Async Commit，立即返回成功，并在后台清理锁
		if useAsyncCommit {
			// fmt.Println("Async Commit path taken")
			txn.committed = true
			go func() {
			// Async Commit 清理锁逻辑：提交所有 Keys (Primary + Secondaries)
			bgCtx := context.Background()

			// Group all mutations
			allBatches, err := txn.groupMutations(bgCtx, pbMutations)
			if err != nil {
				return
			}

			reqs := make([]*titankvpb.CommitRequest, 0, len(allBatches))
			for _, batch := range allBatches {
				keys := make([][]byte, 0, len(batch.muts))
				for _, m := range batch.muts {
					keys = append(keys, m.Key)
				}
				reqs = append(reqs, &titankvpb.CommitRequest{
					Context: &titankvpb.RegionContext{
						RegionId:    batch.region.Id,
						RegionEpoch: toTitanEpoch(batch.region.RegionEpoch),
					},
					StartTs:  txn.StartTS,
					CommitTs: commitTS,
					Keys:     keys,
				})
			}

			// 发送 Commit 请求
			_ = txn.client.SendCommitBatch(bgCtx, reqs, 8)
		}()
		return nil
	}

	var secBatches map[uint64]*batchKeys
	if len(pbMutations) > 1 {
		if prewriteBatches != nil {
			secBatches = make(map[uint64]*batchKeys, len(prewriteBatches))
			for id, batch := range prewriteBatches {
				if len(batch.muts) == 0 {
					continue
				}
				sec := make([]*titankvpb.Mutation, 0, len(batch.muts))
				for _, m := range batch.muts {
					if bytes.Equal(m.Key, primary) {
						continue
					}
					sec = append(sec, m)
				}
				if len(sec) > 0 {
					secBatches[id] = &batchKeys{region: batch.region, muts: sec}
				}
			}
		} else {
			secMutations := make([]*titankvpb.Mutation, 0, len(pbMutations)-1)
			for i := 1; i < len(pbMutations); i++ {
				secMutations = append(secMutations, pbMutations[i])
			}
			var err error
			secBatches, err = txn.groupMutations(ctx, secMutations)
			if err != nil {
				return err
			}
		}
	}

	// 5. 第二阶段：Commit Primary
	// 只有 Primary Key 必须同步提交，决定事务状态
	region, _ := txn.client.GetRegionCache().Search(primary)
	if region == nil {
		if _, err := txn.client.LocateLeader(context.Background(), primary); err != nil {
			return err
		}
		region, _ = txn.client.GetRegionCache().Search(primary)
		if region == nil {
			return fmt.Errorf("region not found for primary %s", string(primary))
		}
	}

	keys := [][]byte{primary}
	if secBatches != nil {
		if batch, ok := secBatches[region.Id]; ok {
			keys = make([][]byte, 0, 1+len(batch.muts))
			keys = append(keys, primary)
			for _, m := range batch.muts {
				keys = append(keys, m.Key)
			}
			delete(secBatches, region.Id)
		}
	}

	commitReq := &titankvpb.CommitRequest{
		StartTs:  txn.StartTS,
		CommitTs: commitTS,
		Keys:     keys,
		Context: &titankvpb.RegionContext{
			RegionId:    region.Id,
			RegionEpoch: toTitanEpoch(region.RegionEpoch),
		},
	}

	// 增加重试循环处理 EpochNotMatch
	var cResp *titankvpb.CommitResponse
	var commitErr error

	for i := 0; i < 3; i++ {
		cResp, commitErr = txn.client.SendCommit(ctx, commitReq)

		// 检查 RPC 错误中的 EpochNotMatch
		if commitErr != nil {
			st, _ := status.FromError(commitErr)
			if st.Code() == codes.Aborted && st.Message() == "EpochNotMatch" {
				txn.client.GetRegionCache().Invalidate(primary)
				time.Sleep(20 * time.Millisecond)
				continue
			}
			break
		}

		// 检查业务错误
		if cResp.Error != "" {
			if cResp.Error == "EpochNotMatch" {
				txn.client.GetRegionCache().Invalidate(primary)
				time.Sleep(20 * time.Millisecond)
				continue
			}
			break
		}

		break // 成功
	}

	if commitErr != nil {
		return commitErr
	}
	if cResp.Error != "" {
		errText := cResp.Error
		if strings.Contains(errText, "Lock mismatch") {
			return &ConflictError{
				Stage:   "commit_primary",
				Kind:    "lock_mismatch",
				Key:     primary,
				Primary: primary,
				StartTS: txn.StartTS,
			}
		}
		if strings.Contains(errText, "Key is locked") {
			return &ConflictError{
				Stage:   "commit_primary",
				Kind:    "key_locked",
				Key:     primary,
				Primary: primary,
				StartTS: txn.StartTS,
			}
		}
		return fmt.Errorf("commit primary failed: %s", cResp.Error)
	}

	if len(pbMutations) > 1 && len(secBatches) > 0 {
		secReqs := make([]*titankvpb.CommitRequest, 0, len(secBatches))
		for _, batch := range secBatches {
			keys := make([][]byte, 0, len(batch.muts))
			for _, m := range batch.muts {
				keys = append(keys, m.Key)
			}
			secReqs = append(secReqs, &titankvpb.CommitRequest{
				Context: &titankvpb.RegionContext{
					RegionId:    batch.region.Id,
					RegionEpoch: toTitanEpoch(batch.region.RegionEpoch),
				},
				StartTs:  txn.StartTS,
				CommitTs: commitTS,
				Keys:     keys,
			})
		}
		go func(reqs []*titankvpb.CommitRequest) {
			workers := len(reqs)
			maxWorkers := runtime.GOMAXPROCS(0) * 4
			if maxWorkers < 4 {
				maxWorkers = 4
			}
			if workers > maxWorkers {
				workers = maxWorkers
			}
			_ = txn.client.SendCommitBatch(context.Background(), reqs, workers)
		}(secReqs)
	}

	txn.committed = true
	return nil
}
func toTitanEpoch(e *pdpb.RegionEpoch) *titankvpb.RegionEpoch {
	if e == nil {
		return nil
	}
	return &titankvpb.RegionEpoch{
		ConfVer: e.ConfVer,
		Version: e.Version,
	}
}

func BatchCommit(ctx context.Context, c *client.Client, mutations []Mutation) error {
	if len(mutations) == 0 {
		return nil
	}
	txn, err := NewTransaction(ctx, c)
	if err != nil {
		return err
	}
	for _, m := range mutations {
		if m.Op == OpDelete {
			txn.Delete(m.Key)
		} else {
			txn.Set(m.Key, m.Value)
		}
	}
	return txn.Commit(ctx)
}
