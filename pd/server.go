package pd

import (
	"context"
	"fmt" // 【新增】修复 undefined: fmt
	"log"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency" // 确保包含这个
	"go.etcd.io/etcd/server/v3/embed"
)
const (
	etcdTimeout = 3 * time.Second
)

type Server struct {
	cfg      *Config
	etcd     *embed.Etcd
	client   *clientv3.Client
	
	// 原子变量：1 表示我是 Leader，0 表示 Follower
	isLeader int64 
	
	// 用于通知退出的 Context
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewServer(cfg *Config) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *Server) Run() error {
	// 1. 转换配置
	etcdCfg, err := s.cfg.GenEmbedEtcdConfig()
	if err != nil {
		return err
	}

	// 2. 启动嵌入式 Etcd
	log.Printf("Starting embedded etcd...")
	e, err := embed.StartEtcd(etcdCfg)
	if err != nil {
		return err
	}
	s.etcd = e

	// 3. 等待 Etcd 就绪
	select {
	case <-e.Server.ReadyNotify():
		log.Printf("Etcd is ready!")
	case <-time.After(60 * time.Second):
		e.Close()
		return fmt.Errorf("server took too long to start")
	}

	// 4. 创建连接自己的 Client (用于后续选主和元数据存取)
	// 因为是连接本地，Endpoints 填 ClientUrls 即可
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   s.cfg.ClientUrls,
		DialTimeout: etcdTimeout,
	})
	if err != nil {
		return err
	}
	s.client = client

	// 5. 启动竞选 Loop (异步)
	go s.campaignLoop()

	return nil
}

func (s *Server) Close() {
	s.cancel()
	if s.client != nil {
		s.client.Close()
	}
	if s.etcd != nil {
		s.etcd.Close()
	}
}

// 检查自己是否是 Leader
func (s *Server) IsLeader() bool {
	return atomic.LoadInt64(&s.isLeader) == 1
}

func (s *Server) campaignLoop() {
	for {
		if s.ctx.Err() != nil {
			return
		}

		// 1. 创建 Session (带 TTL 的会话)
		// 如果 PD 挂了，TTL 过期，Lease 自动释放，其他节点可以抢
		session, err := concurrency.NewSession(s.client, concurrency.WithTTL(5)) // 5秒 TTL
		if err != nil {
			log.Printf("Failed to create session: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// 2. 创建 Election 对象
		election := concurrency.NewElection(session, "/pd/leader")

		// 3. 开始竞选 (阻塞调用，直到当选)
		log.Println("Campaigning for leader...")
		if err := election.Campaign(s.ctx, s.cfg.Name); err != nil {
			log.Printf("Campaign failed: %v", err)
			session.Close()
			continue
		}

		// 4. 当选成功！
		log.Println("I am the Leader!")
		atomic.StoreInt64(&s.isLeader, 1)

		// 5. 阻塞直到 Session 过期或被取消 (Resign)
		// 只要 Session 还在，我就一直是 Leader
		select {
		case <-session.Done():
			log.Println("Session expired, stepping down")
		case <-s.ctx.Done():
			log.Println("Server stopping, stepping down")
		}

		// 6. 退位
		atomic.StoreInt64(&s.isLeader, 0)
		session.Close()
	}
}