package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"titankv/pd/api/pdpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	storePathPrefix = "/pd/stores"
	regionPathPrefix = "/pd/regions"
)

type RaftCluster struct {
	client *clientv3.Client
	mu     sync.RWMutex
	stores map[uint64]*StoreInfo
	regions *RegionTree
	// 【新增】缓存每个 Region 的 Leader
	// Key: RegionID, Value: Leader Peer
	leaders map[uint64]*pdpb.Peer
}

func NewRaftCluster(client *clientv3.Client) *RaftCluster {
	return &RaftCluster{
		client: client,
		stores: make(map[uint64]*StoreInfo),
		regions: NewRegionTree(),
		leaders: make(map[uint64]*pdpb.Peer),
	}
}

// 启动时加载 Etcd 中的元数据
func (c *RaftCluster) Load(ctx context.Context) error {
	resp, err := c.client.Get(ctx, storePathPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, kv := range resp.Kvs {
		var meta pdpb.MetaStore
		if err := json.Unmarshal(kv.Value, &meta); err != nil {
			continue
		}
		c.stores[meta.Id] = NewStoreInfo(&meta)
	}
	log.Printf("Loaded %d stores from Etcd", len(c.stores))
	return nil
}

// 处理 PutStore (注册/更新静态信息)
func (c *RaftCluster) PutStore(ctx context.Context, meta *pdpb.MetaStore) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.stores[meta.Id]; ok {
		// 已存在，可能只是更新地址，暂不处理
		return nil
	}

	// 1. 持久化到 Etcd
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s/%d", storePathPrefix, meta.Id)
	if _, err := c.client.Put(ctx, key, string(data)); err != nil {
		return err
	}

	// 2. 更新内存
	c.stores[meta.Id] = NewStoreInfo(meta)
	log.Printf("New store registered: ID=%d, Addr=%s", meta.Id, meta.Address)
	return nil
}


// 获取所有 Store
func (c *RaftCluster) GetStores() []*StoreInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	var stores []*StoreInfo
	for _, s := range c.stores {
		stores = append(stores, s.Clone())
	}
	return stores
}


// 【新增】启动时加载所有 Region
func (c *RaftCluster) LoadRegions(ctx context.Context) error {
    // 扫描 /pd/regions 前缀的所有 key
    resp, err := c.client.Get(ctx, regionPathPrefix, clientv3.WithPrefix())
    if err != nil {
        return err
    }

    for _, kv := range resp.Kvs {
        var region pdpb.Region
        if err := json.Unmarshal(kv.Value, &region); err != nil {
            continue
        }
        // 更新内存树
        c.regions.Update(&region)
    }
    log.Printf("Loaded %d regions from Etcd", len(resp.Kvs))
    return nil
}

// HandleRegionHeartbeat 处理 Region 心跳
func (c *RaftCluster) HandleRegionHeartbeat(ctx context.Context, req *pdpb.RegionHeartbeatRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	region := req.Region
	leader := req.Leader

	// 1. 检查 Region 有效性 (简单的 Epoch 检查)
	// 在工业级实现中，如果 PD 发现心跳的 Epoch 小于 PD 缓存的 Epoch，
	// 说明是过期的心跳，应该忽略或返回错误让 TiKV 更新。
	// 这里 Day 4 简化：总是信任最新的心跳。

	// 2. 更新内存 B-Tree (RegionTree)
	// 这会处理 Region 的 StartKey/EndKey 变化（如分裂）
	c.regions.Update(region)

	// 3. 更新 Leader 缓存
	if leader != nil {
		c.leaders[region.Id] = leader
	}

	// 4. (可选) 更新统计信息
	// c.regionStats[region.Id] = req.ApproximateSize ...

	// 5. 持久化 Region 元数据到 Etcd
	// 注意：频繁写 Etcd 会有性能问题。
	// 优化策略：只有当 Region 的 Meta (Range, Epoch, Peers) 发生变化时才写。
	// 如果只是 Leader 变了或者 Size 变了，只更新内存，不写 Etcd。
	
	// TODO: 比较新旧 Region，只有变化才 save。为了简单，Day 4 每次都 save。
	return c.saveRegion(ctx, region)
}

// 内部辅助：保存 Region 到 Etcd
func (c *RaftCluster) saveRegion(ctx context.Context, region *pdpb.Region) error {
	data, err := json.Marshal(region)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s/%d", regionPathPrefix, region.Id)

	// 使用独立的 Context 避免主请求超时导致持久化中断
	// 但在这个简单的同步实现中，直接用 ctx 也可以，或者用 Background
	// 为了不阻塞 Heartbeat 的 RPC 返回，最好是异步写，或者用 KV 系统的 Batch Put
	
	// 这里演示异步写 (Fire and Forget)，生产环境需要更严谨的错误处理
	go func() {
		// 这里的 Timeout 设短一点
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := c.client.Put(ctx, key, string(data)); err != nil {
			log.Printf("[PD] Failed to persist region %d: %v", region.Id, err)
		}
	}()
	
	return nil
}

// GetRegion 根据 Key 查找 Region 和 Leader
func (c *RaftCluster) GetRegion(key []byte) (*pdpb.Region, *pdpb.Peer) {
	// 读锁
	c.mu.RLock()
	defer c.mu.RUnlock()

	// 1. 从 B-Tree 查找 Region
	r := c.regions.Search(key)
	if r == nil {
		return nil, nil
	}

	// 2. 【新增】从 Map 查找 Leader
	// 注意：如果刚启动还没收到心跳，leader 可能是 nil，Client 端需要处理这种情况（重试）
	leader := c.leaders[r.Id]

	// 返回副本以防止外部修改内部状态 (虽然 Protobuf 生成的结构体是指针)
	// 简单的浅拷贝返回即可，因为我们只读
	return r, leader
}