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
}

func NewRaftCluster(client *clientv3.Client) *RaftCluster {
	return &RaftCluster{
		client:      client,
		stores:      make(map[uint64]*StoreInfo),
		regions:     make(map[uint64]*RegionInfo),
		regionsTree: NewRegionTree(),
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

// HandleRegionHeartbeat 处理 Region 心跳
func (c *RaftCluster) HandleRegionHeartbeat(ctx context.Context, req *pdpb.RegionHeartbeatRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	region := req.Region
	peer := req.Leader

	// 1. 获取旧信息
	origin, exists := c.regions[region.Id]

	// 2. 检查 Epoch
	if exists {
		if region.RegionEpoch != nil && origin.Meta.RegionEpoch != nil {
			if region.RegionEpoch.Version < origin.Meta.RegionEpoch.Version ||
				region.RegionEpoch.ConfVer < origin.Meta.RegionEpoch.ConfVer {
				return nil // 过期消息
			}
		}
	}
	log.Printf("current leader: %v", peer)
	// 3. 构建新 RegionInfo
	newRegionInfo := NewRegionInfo(region, peer)
	newRegionInfo.ApproximateSize = req.ApproximateSize
	newRegionInfo.ApproximateKeys = req.ApproximateKeys

	// 4. 更新内存索引 (Map)
	c.regions[region.Id] = newRegionInfo

	// 5. 更新内存索引 (B-Tree)
	if exists && !bytes.Equal(origin.Meta.StartKey, region.StartKey) {
		c.regionsTree.Remove(origin)
	}
	c.regionsTree.Update(newRegionInfo)

	// 6. 持久化
	needPersist := !exists ||
		(origin.Meta.RegionEpoch.Version != region.RegionEpoch.Version) ||
		(origin.Meta.RegionEpoch.ConfVer != region.RegionEpoch.ConfVer)

	if needPersist {
		if err := c.saveRegion(ctx, region); err != nil {
			log.Printf("[PD] Async save failed: %v", err)
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
	r := c.regionsTree.Search(key) // 【修复】使用 regionsTree
	if r == nil {
		return nil, nil
	}

	// 2. 获取 Leader (直接从 RegionInfo 获取)
	// 【修复】leaders map 已被移除，直接用 r.Leader
	return r.Meta, r.Leader
}

// GetRegionByID
func (c *RaftCluster) GetRegionByID(id uint64) *RegionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.regions[id]
}

// 内部辅助：保存 Region 到 Etcd
func (c *RaftCluster) saveRegion(ctx context.Context, region *pdpb.Region) error {
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

// 供 Week 9 Day 1 调度器使用：获取所有 Region
func (c *RaftCluster) GetRegions() []*RegionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res := make([]*RegionInfo, 0, len(c.regions))
	for _, r := range c.regions {
		res = append(res, r.Clone())
	}
	return res
}