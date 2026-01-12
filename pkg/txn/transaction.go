package txn

import (
	"context"
	"errors"
	
	"titankv/pkg/client" // Week 8 实现的 Client

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
	// 1. 从 PD 获取 StartTS
	// 假设 Client 暴露了 GetTS 接口 (Week 8 Day 2)
	// 如果没有，需要去补一个
	ts, err := c.GetTS(ctx)
	if err != nil {
		return nil, err
	}

	return &Transaction{
		StartTS:   ts,
		client:    c,
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
	if val, ok := txn.buffer[k]; ok {
		if val == nil {
			return nil, errors.New("key not found (deleted in txn)")
		}
		return val, nil
	}

	// 2. 查 Storage (Snapshot Read)
	// 我们需要调用 Server 的 SnapshotRead 接口
	// (Server 端接口将在 Week 14 实现，这里先定义 Client 逻辑)
	
	// 假设 Client 有一个 SnapshotGet 方法，传入 Key 和 StartTS
	val, err := txn.client.SnapshotGet(ctx, key, txn.StartTS)
	if err != nil {
		return nil, err
	}
	
	// 3. 将读到的数据缓存到 Buffer 吗？
	// 通常不需要，除非是为了 Repeatable Read 的校验。
	// Percolator 模型在 Prewrite 阶段会检查 Write Conflict，所以这里不需要缓存读集。
	
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

	// 3. 第一阶段：Prewrite (GroupByRegion + Concurrent)
	batches, err := txn.groupMutations(pbMutations)
	if err != nil { return err }

	g, ctx := errgroup.WithContext(ctx)
	
	// 遍历每个 Region 的批次
	for _, batch := range batches {
		batch := batch // capture loop var
		g.Go(func() error {
			req := &titankvpb.PrewriteRequest{
				Context: &titankvpb.RegionContext{
					RegionId:    batch.region.Id,
					RegionEpoch: batch.region.RegionEpoch,
					// Peer: 这里可以留空，由 Client.SendPrewrite 内部填充或忽略
				},
				Mutations:  batch.muts,
				PrimaryKey: primary,
				StartTs:    txn.StartTS,
				LockTtl:    3000,
			}
			
			// 调用 Client 的定向发送接口
			resp, err := txn.client.SendPrewrite(ctx, req)
			if err != nil { return err }
			if resp.Error != "" { return fmt.Errorf("prewrite failed: %s", resp.Error) }
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err // 任意一个 Prewrite 失败，事务回滚 (Client 端 Cleanup 暂略)
	}

	// 4. 获取 CommitTS
	commitTS, err := txn.client.GetTS(ctx)
	if err != nil { return err }
	if commitTS <= txn.StartTS {
		return fmt.Errorf("invalid commit ts")
	}

	// 5. 第二阶段：Commit Primary
	// Primary Key 必须先提交，决定事务状态
	// 我们需要找到 Primary Key 所在的 Region
	// 为了复用逻辑，我们重新 Group 一次，但只包含 Primary Key
    // 或者简单点：直接构造请求，利用 SendCommit 内部的 Locate 逻辑
    
	commitReq := &titankvpb.CommitRequest{
        // Context 会由 SendCommit 自动填充 (如果需要)
		StartTs:    txn.StartTS,
		CommitTs:   commitTS,
		Keys:       [][]byte{primary},
	}
    
    // Primary Commit 必须同步等待成功
    // 我们复用 SendCommit，它会根据 Key 路由
	cResp, err := txn.client.SendCommit(ctx, commitReq)
	if err != nil { return err }
	if cResp.Error != "" { return fmt.Errorf("commit primary failed: %s", cResp.Error) }

	// 6. 异步 Commit Secondaries
	// Primary 成功后，事务已经成功。剩下的可以异步做。
	if len(pbMutations) > 1 {
		go func() {
            // 这里应该重新 GroupByRegion，但为了简单，我们还是利用 Client 的自动路由能力
            // 构造包含所有 Secondary Keys 的请求
            // 但这样会导致请求发给所有 Region 吗？
            // 不，SendCommit 如果只支持单 Region 路由，这里就会出问题。
            // 正确做法：对 Secondary Keys 也做 GroupByRegion。
            
            // 为了 Week 14 Day 1 不过于复杂，我们假设 Client.SendCommit 内部不做拆分，
            // 而是要求我们传入已经拆分好的。
            
            // 重新 Group 剩余的 Keys
            var secMutations []*titankvpb.Mutation
            for i := 1; i < len(pbMutations); i++ {
                secMutations = append(secMutations, pbMutations[i])
            }
            
            secBatches, _ := txn.groupMutations(secMutations)
            
            for _, batch := range secBatches {
                var keys [][]byte
                for _, m := range batch.muts { keys = append(keys, m.Key) }
                
                req := &titankvpb.CommitRequest{
                    Context: &titankvpb.RegionContext{
                        RegionId:    batch.region.Id,
                        RegionEpoch: batch.region.RegionEpoch,
                    },
                    StartTs:  txn.StartTS,
                    CommitTs: commitTS,
                    Keys:     keys,
                }
                // 背景执行，忽略错误
                txn.client.SendCommit(context.Background(), req)
            }
		}()
	}

	txn.committed = true
	return nil
}