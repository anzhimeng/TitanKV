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
)

type RaftCluster struct {
	client *clientv3.Client
	mu     sync.RWMutex
	stores map[uint64]*StoreInfo
}

func NewRaftCluster(client *clientv3.Client) *RaftCluster {
	return &RaftCluster{
		client: client,
		stores: make(map[uint64]*StoreInfo),
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

// 处理心跳
func (c *RaftCluster) HandleStoreHeartbeat(req *pdpb.StoreHeartbeatRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	store, ok := c.stores[req.StoreId]
	if !ok {
		return fmt.Errorf("store %d not found", req.StoreId)
	}

	// 更新动态状态 (纯内存操作，不写 Etcd，因为心跳太频繁)
	store.LastHeartbeat = time.Now()
	store.Stats = req.Stats
	
	// log.Printf("Heartbeat from Store %d", req.StoreId)
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