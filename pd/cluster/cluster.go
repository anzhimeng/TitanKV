package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
	"bytes"
	"titankv/pd/api/pdpb"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	storePathPrefix  = "/pd/stores"
	regionPathPrefix = "/pd/regions"
)

type RaftCluster struct {
	client *clientv3.Client
	mu     sync.RWMutex

	stores      map[uint64]*StoreInfo
	regions     map[uint64]*RegionInfo // ID -> Info
	regionsTree *RegionTree            // KeyRange -> Info
	storeRegions map[uint64]map[uint64]struct{}
}

func NewRaftCluster(client *clientv3.Client) *RaftCluster {
	return &RaftCluster{
		client:      client,
		stores:      make(map[uint64]*StoreInfo),
		regions:     make(map[uint64]*RegionInfo),
		regionsTree: NewRegionTree(),
		storeRegions: make(map[uint64]map[uint64]struct{}),
	}
}

// 启动时加载 Etcd 中的 Store 元数据
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
	
	// 顺便加载 Regions
	return c.loadRegionsLocked(ctx)
}

// 内部辅助：加载所有 Region (需要在锁内调用，或者由 Load 调用)
func (c *RaftCluster) loadRegionsLocked(ctx context.Context) error {
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
		
		// 构造 RegionInfo (启动时 Leader 未知，设为 nil)
		info := NewRegionInfo(&region, nil)
		
		// 更新内存 Map
		c.regions[region.Id] = info
		// 更新内存 B-Tree
		c.regionsTree.Update(info)       
		// 【关键修复】重建倒排索引 (Store -> Regions)
         c.updateStoreRegionIndex(region.Id, nil, region.Peers)
	}
	log.Printf("Loaded %d regions from Etcd", len(resp.Kvs))
	return nil
}

// 处理 PutStore (注册/更新静态信息)
func (c *RaftCluster) PutStore(ctx context.Context, meta *pdpb.MetaStore) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.stores[meta.Id]; ok {
		return nil
	}

	// 1. 持久化到 Etcd
	//data, err := json.Marshal(meta)
	//if err != nil {
		//return err
	//}
	//key := fmt.Sprintf("%s/%d", storePathPrefix, meta.Id)
	//if _, err := c.client.Put(ctx, key, string(data)); err != nil {
	//	return err
	//}
	// 测试模式 (client == nil)，跳过持久化
	if c.client != nil {
		data, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		key := fmt.Sprintf("%s/%d", storePathPrefix, meta.Id)
		if _, err := c.client.Put(ctx, key, string(data)); err != nil {
			return err
		}
	}

	// 2. 更新内存
	c.stores[meta.Id] = NewStoreInfo(meta)
	log.Printf("New store registered: ID=%d, Addr=%s", meta.Id, meta.Address)
	return nil
}

// 处理 Store 心跳 (Day 3 内容)
func (c *RaftCluster) HandleStoreHeartbeat(req *pdpb.StoreHeartbeatRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	store, ok := c.stores[req.StoreId]
	if !ok {
		return fmt.Errorf("store %d not found", req.StoreId)
	}

	store.LastHeartbeat = time.Now()
	store.Stats = req.Stats
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

// 辅助：更新倒排索引 (Store -> Regions)
func (c *RaftCluster) updateStoreRegionIndex(regionID uint64, oldPeers []*pdpb.Peer, newPeers []*pdpb.Peer) {
	// 1. 从旧 Store 中移除
	for _, p := range oldPeers {
		storeID := p.StoreId
		if regionSet, ok := c.storeRegions[storeID]; ok {
			delete(regionSet, regionID)
			// 如果空了可以删掉 map，但在频繁变动下保留也无妨
		}
	}

	// 2. 添加到新 Store
	for _, p := range newPeers {
		storeID := p.StoreId
		regionSet, ok := c.storeRegions[storeID]
		if !ok {
			regionSet = make(map[uint64]struct{})
			c.storeRegions[storeID] = regionSet
		}
		regionSet[regionID] = struct{}{}
	}
}


// HandleRegionHeartbeat 处理 Region 心跳
func (c *RaftCluster) HandleRegionHeartbeat(ctx context.Context, req *pdpb.RegionHeartbeatRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	region := req.Region
	peer := req.Leader

	origin, exists := c.regions[region.Id]

	if exists {
		if region.RegionEpoch != nil && origin.Meta.RegionEpoch != nil {
			if region.RegionEpoch.Version < origin.Meta.RegionEpoch.Version ||
				region.RegionEpoch.ConfVer < origin.Meta.RegionEpoch.ConfVer {
				return nil
			}
		}
	}

	if peer == nil && exists && origin.Leader != nil {
		peer = origin.Leader
	}

	// 更新倒排索引 (如果 Peer 列表发生了变化，或者这是新 Region)
	// 简化判断：直接全量更新索引（先删旧的，再加新的），虽然有少许性能损耗但逻辑绝对正确
	var oldPeers []*pdpb.Peer
	if exists {
		oldPeers = origin.Meta.Peers
	}
	// 【新增】调用辅助函数更新索引
	c.updateStoreRegionIndex(region.Id, oldPeers, region.Peers)

	// 更新 Store 的 LeaderCount / RegionCount (Day 3 的逻辑)
	if exists {
		if origin.Leader != nil {
			if store, ok := c.stores[origin.Leader.StoreId]; ok {
				store.LeaderCount--
			}
		}
		for _, p := range origin.Meta.Peers {
			if store, ok := c.stores[p.StoreId]; ok {
				store.RegionCount--
			}
		}
	}

	if peer != nil {
		if store, ok := c.stores[peer.StoreId]; ok {
			store.LeaderCount++
		}
	}
	for _, p := range region.Peers {
		if store, ok := c.stores[p.StoreId]; ok {
			store.RegionCount++
		}
	}

	// 构建新对象
	newRegionInfo := NewRegionInfo(region, peer)
	newRegionInfo.ApproximateSize = req.ApproximateSize
	newRegionInfo.ApproximateKeys = req.ApproximateKeys

	c.regions[region.Id] = newRegionInfo

	if exists && !bytes.Equal(origin.Meta.StartKey, region.StartKey) {
		c.regionsTree.Remove(origin)
	}
	c.regionsTree.Update(newRegionInfo)

	// 持久化
	needPersist := !exists ||
		(origin.Meta.RegionEpoch.Version != region.RegionEpoch.Version) ||
		(origin.Meta.RegionEpoch.ConfVer != region.RegionEpoch.ConfVer)

	if needPersist {
		if err := c.saveRegion(ctx, region); err != nil {
			// log.Printf(...)
		}
	}

	return nil
}

// 【工业级实现】RandRegionOnStore
// 利用倒排索引，复杂度从 O(TotalRegions) 降低到 O(StoreRegions)
func (c *RaftCluster) RandRegionOnStore(storeID uint64, excludeStoreID uint64) *RegionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// 1. 直接获取该 Store 上的 Region 集合
	regionSet, ok := c.storeRegions[storeID]
    if !ok || len(regionSet) == 0 {
        // 【调试】
        // log.Printf("Store %d has no regions in index", storeID)
        return nil
    }

	// 2. 利用 Go map 遍历的随机性来选取
	// 我们遍历这个集合，直到找到一个符合条件的
	for regionID := range regionSet {
		region := c.regions[regionID]
		if region == nil {
			continue // Should not happen
		}

		// 3. 检查 excludeStoreID
		// 我们已经确信该 Region 在 storeID 上（因为是从 storeRegions[storeID] 拿的）
		// 只需要检查它是否同时在 excludeStoreID 上
		onTarget := false
		for _, p := range region.Meta.Peers {
			if p.StoreId == excludeStoreID {
				onTarget = true
				break
			}
		}

		if !onTarget {
			return region.Clone()
		}
	}

	return nil
}

// SearchRegion 查找 Key 所在的 Region
func (c *RaftCluster) SearchRegion(key []byte) *RegionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.regionsTree.Search(key)
}

// GetRegion 根据 Key 查找 Region 和 Leader (Client API 用)
func (c *RaftCluster) GetRegion(key []byte) (*pdpb.Region, *pdpb.Peer) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// 1. 从 B-Tree 查找 Region
	r := c.regionsTree.Search(key)
	if r != nil {
		log.Printf("[PD] GetRegion Key=%s -> Region %d, Epoch: %v", string(key), r.Meta.Id, r.Meta.RegionEpoch)
		return r.Meta, r.Leader
	}
	for _, info := range c.regions {
		start := info.Meta.GetStartKey()
		end := info.Meta.GetEndKey()
		if len(start) > 0 && bytes.Compare(key, start) < 0 {
			continue
		}
		if len(end) > 0 && bytes.Compare(key, end) >= 0 {
			continue
		}
		log.Printf("[PD] GetRegion Key=%s -> Region %d, Epoch: %v", string(key), info.Meta.Id, info.Meta.RegionEpoch)
		return info.Meta, info.Leader
	}
	log.Printf("[PD] GetRegion Key=%s -> Not Found", string(key))
	return nil, nil
}

// GetRegionByID
func (c *RaftCluster) GetRegionByID(id uint64) *RegionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.regions[id]
}

// 内部辅助：保存 Region 到 Etcd
func (c *RaftCluster) saveRegion(ctx context.Context, region *pdpb.Region) error {
	// 【修复 Panic】如果 client 为空，直接返回（测试模式）
	if c.client == nil {
		return nil
	}
	data, err := json.Marshal(region)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s/%d", regionPathPrefix, region.Id)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := c.client.Put(ctx, key, string(data)); err != nil {
			log.Printf("[PD] Failed to persist region %d: %v", region.Id, err)
		}
	}()
	return nil
}

// 获取所有 Region
func (c *RaftCluster) GetRegions() []*RegionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res := make([]*RegionInfo, 0, len(c.regions))
	for _, r := range c.regions {
		res = append(res, r.Clone())
	}
	return res
}

// 仅供测试使用
func (c *RaftCluster) SetStoreLeaderCountForTest(id uint64, count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.stores[id]; ok {
		s.LeaderCount = count
	}
}

func (c *RaftCluster) GetStoreInfoByID(id uint64) *StoreInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
    if store, ok := c.stores[id]; ok {
        return store.Clone() // 返回副本，线程安全
    }
	return nil
}
