package txn

import (
	"context"
	"fmt"
	"time"
	"log"

	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
	"titankv/pkg/client" // Week 8 实现的 Client

	"google.golang.org/grpc/codes"
     "google.golang.org/grpc/status"
	"golang.org/x/sync/errgroup" 
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

type Transaction struct {
	StartTS uint64
	client  *client.Client // 持有 KV Client 和 PD Client
	
	// 读写缓冲
	// key -> value (nil 表示 delete)
	buffer map[string][]byte
	// 记录修改顺序 (虽然 Percolator 不依赖顺序，但为了 Debug 方便)
	mutations []Mutation 
	
	// 事务状态
	committed bool
}

// 开启新事务
func NewTransaction(ctx context.Context, c *client.Client) (*Transaction, error) {
    ts, err := c.GetTS(ctx)
    if err != nil {
        return nil, err
    }

    return &Transaction{
        StartTS:   ts,
        client:    c,
        // 【必须初始化】
        buffer:    make(map[string][]byte), 
        mutations: make([]Mutation, 0),
    }, nil
}

func (txn *Transaction) Set(key []byte, value []byte) {
	k := string(key)
	txn.buffer[k] = value
	
	// 记录 Mutation (为了 Commit 阶段使用)
	// 优化：如果 key 已存在 mutations 中，更新它？
	// 简单起见，直接 append，Commit 时再 deduplicate 或者以 buffer 为准
	txn.mutations = append(txn.mutations, Mutation{
		Key:   key,
		Value: value,
		Op:    OpPut,
	})
}

func (txn *Transaction) Delete(key []byte) {
	k := string(key)
	txn.buffer[k] = nil // nil 表示删除标记
	
	txn.mutations = append(txn.mutations, Mutation{
		Key: key,
		Op:  OpDelete,
	})
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

func (txn *Transaction) groupMutations(mutations []*titankvpb.Mutation) (map[uint64]*batchKeys, error) {
	groups := make(map[uint64]*batchKeys)
	cache := txn.client.GetRegionCache()

	for _, m := range mutations {
		// 1. 查找路由
		region, _ := cache.Search(m.Key)
		if region == nil {
			// 缓存未命中，强制刷新 (LocateLeader 会更新缓存)
			if _, err := txn.client.LocateLeader(context.Background(), m.Key); err != nil {
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
	var pbMutations []*titankvpb.Mutation
	for _, m := range txn.mutations {
		op := titankvpb.Mutation_Put
		if m.Op == OpDelete {
			op = titankvpb.Mutation_Delete
		}
		pbMutations = append(pbMutations, &titankvpb.Mutation{
			Op:    op,
			Key:   m.Key,
			Value: m.Value,
		})
	}

	if len(pbMutations) == 0 {
		return nil
	}

	// 2. 选择 Primary Key
	primary := pbMutations[0].Key

	// 3. 第一阶段：Prewrite (带重试、锁清理和 Epoch 刷新)
	// 使用无限循环来处理 ResolveLock 和 Epoch 重试
	log.Printf("[Txn] Start Commit. Mutations: %d", len(pbMutations))
	for {
		batches, err := txn.groupMutations(pbMutations)
		if err != nil { return err }

		g, groupCtx := errgroup.WithContext(ctx)
		
		// 错误通道：用于收集 KeyLocked 错误
		lockErrCh := make(chan *titankvpb.LockInfo, len(batches))
		// 错误通道：用于收集 Epoch 错误信号
		epochErrCh := make(chan bool, 1)

		for _, batch := range batches {
			batch := batch
			g.Go(func() error {
				req := &titankvpb.PrewriteRequest{
					Context: &titankvpb.RegionContext{
						RegionId:    batch.region.Id,
						RegionEpoch: toTitanEpoch(batch.region.RegionEpoch),
					},
					Mutations:  batch.muts,
					PrimaryKey: primary,
					StartTs:    txn.StartTS,
					LockTtl:    3000,
				}
				log.Printf("[Txn] Sending Prewrite to Region %d. Epoch: %v", 
                    batch.region.Id, batch.region.RegionEpoch)
				// 网络重试循环
				var lastErr error
				for i := 0; i < 3; i++ {
					resp, err := txn.client.SendPrewrite(groupCtx, req)
					if err == nil {
						// 检查业务错误
						if resp.Error != "" {
							// A. 如果是 KeyLocked，收集 LockInfo 并返回 nil (非 Fatal)
							if resp.KeyError != nil {
								lockErrCh <- resp.KeyError.LockInfo
								return nil 
							}
							return fmt.Errorf("prewrite failed: %s", resp.Error)
						}
						return nil // 成功
					}
					
					// B. 检查 gRPC 错误中的 EpochNotMatch (Aborted)
					st, _ := status.FromError(err)
					if st.Code() == codes.Aborted && st.Message() == "EpochNotMatch" {
					log.Printf("[Txn] Prewrite Err: %v. Code: %s, Msg: %s", err, st.Code(), st.Message())
						log.Printf("[Txn] Caught EpochNotMatch! Invalidating cache...")
						// 1. 刷新该批次所有 Key 的缓存
						for _, m := range batch.muts {
							txn.client.GetRegionCache().Invalidate(m.Key)
						}
						// 2. 发送信号
						select {
						case epochErrCh <- true:
						log.Printf("[Txn] Sent epoch signal")
						default:
						}
						// 3. 返回错误中断 g.Wait
						return err
					}

					lastErr = err
					time.Sleep(50 * time.Millisecond)
				}
				return lastErr
			})
		}

		if err := g.Wait(); err != nil {
			// 检查是否有 Epoch 错误
			select {
			case <-epochErrCh:
				// log.Printf("[Txn] Prewrite EpochNotMatch. Retrying...")
				time.Sleep(50 * time.Millisecond)
				continue // 重新 Group，重新 Prewrite
			default:
			log.Printf("[Txn] Prewrite failed permanently: %v", err)
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
			log.Printf("[Txn] Conflict on key %s (Primary: %s). Resolving...", lockInfo.Key, lockInfo.PrimaryKey)
			
			ok, err := txn.client.ResolveLocks(ctx, lockInfo)
			if err != nil { return err }
			
			if ok {
				// 成功清理了锁，立即重试 Prewrite
				time.Sleep(50 * time.Millisecond)
				continue
			} else {
				// Resolve 说 "NoAction" (锁没过期)，我们只能失败
				return fmt.Errorf("txn conflict: key locked by live txn")
			}
		}

		// 全部成功，无锁冲突，进入 Commit 阶段
		break
	}

	// 4. 获取 CommitTS
	commitTS, err := txn.client.GetTS(ctx)
	if err != nil { return err }
	if commitTS <= txn.StartTS {
		return fmt.Errorf("invalid commit ts")
	}

	// 5. 第二阶段：Commit Primary
	// 只有 Primary Key 必须同步提交，决定事务状态
	commitReq := &titankvpb.CommitRequest{
		StartTs:    txn.StartTS,
		CommitTs:   commitTS,
		Keys:       [][]byte{primary},
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
				time.Sleep(100 * time.Millisecond)
				continue
			}
			break 
		}
        
		// 检查业务错误
		if cResp.Error != "" {
			if cResp.Error == "EpochNotMatch" {
				txn.client.GetRegionCache().Invalidate(primary)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			break 
		}
        
		break // 成功
	}
    
	if commitErr != nil { return commitErr }
	if cResp.Error != "" { return fmt.Errorf("commit primary failed: %s", cResp.Error) }

	// 6. 异步 Commit Secondaries
	if len(pbMutations) > 1 {
		go func() {
            var secMutations []*titankvpb.Mutation
            for i := 1; i < len(pbMutations); i++ {
                secMutations = append(secMutations, pbMutations[i])
            }
            
            // 重新 Group，防止 Secondary Keys 分布在不同 Region
            secBatches, _ := txn.groupMutations(secMutations)
            
            for _, batch := range secBatches {
                var keys [][]byte
                for _, m := range batch.muts { keys = append(keys, m.Key) }
                
                req := &titankvpb.CommitRequest{
                    Context: &titankvpb.RegionContext{
                        RegionId:    batch.region.Id,
                        RegionEpoch: toTitanEpoch(batch.region.RegionEpoch),
                    },
                    StartTs:  txn.StartTS,
                    CommitTs: commitTS,
                    Keys:     keys,
                }
                // 异步提交，忽略错误 (如果失败，后续读请求会通过 ResolveLock 解决)
                txn.client.SendCommit(context.Background(), req)
            }
		}()
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
