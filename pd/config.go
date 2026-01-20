package pd

import (
	"net/url"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
)

type Config struct {
	Name       string
	DataDir    string
	ClientUrls []string
	PeerUrls   []string
	
	InitialCluster string 
	GCInterval     time.Duration
	GCSafePointLag time.Duration
}

func (c *Config) GenEmbedEtcdConfig() (*embed.Config, error) {
	cfg := embed.NewConfig()
	cfg.Name = c.Name
	cfg.Dir = c.DataDir
	cfg.InitialCluster = c.InitialCluster
	cfg.ClusterState = embed.ClusterStateFlagNew

	var err error
	cfg.ListenClientUrls, err = parseUrls(c.ClientUrls)
	if err != nil { return nil, err }
	cfg.AdvertiseClientUrls, err = parseUrls(c.ClientUrls)
	if err != nil { return nil, err }
	cfg.ListenPeerUrls, err = parseUrls(c.PeerUrls)
	if err != nil { return nil, err }
	cfg.AdvertisePeerUrls, err = parseUrls(c.PeerUrls)
	if err != nil { return nil, err }

	cfg.Logger = "zap"
	return cfg, nil
}

func parseUrls(urls []string) ([]url.URL, error) {
	var us []url.URL
	for _, u := range urls {
		p, err := url.Parse(u)
		if err != nil {
			return nil, err
		}
		us = append(us, *p)
	}
	return us, nil
}
