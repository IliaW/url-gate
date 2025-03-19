package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	netUrl "net/url"
	"os"
	"sync"

	"github.com/IliaW/url-gate/config"
	"github.com/bradfitz/gomemcache/memcache"
)

var (
	ThresholdReachedError = errors.New("threshold reached")
)

type CachedClient interface {
	CheckIfCrawled(string) bool
	IncrementThreshold(string) error
	Close()
}

type MemcachedClient struct {
	client *memcache.Client
	cfg    *config.CacheConfig
	mu     sync.Mutex
}

func NewMemcachedClient(cacheConfig *config.CacheConfig) *MemcachedClient {
	slog.Info("connecting to memcached...")
	ss := new(memcache.ServerList)
	err := ss.SetServers(cacheConfig.Servers...)
	if err != nil {
		slog.Error("failed to set memcached servers.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	c := &MemcachedClient{
		client: memcache.NewFromSelector(ss),
		cfg:    cacheConfig,
	}
	slog.Info("pinging the memcached.")
	err = c.client.Ping()
	if err != nil {
		slog.Error("connection to the memcached is failed.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	slog.Info("connected to memcached!")

	return c
}

func (mc *MemcachedClient) CheckIfCrawled(url string) bool {
	key := hashURL(url)
	it, err := mc.client.Get(key)
	if err != nil {
		if errors.Is(err, memcache.ErrCacheMiss) {
			slog.Debug("cache not found.", slog.String("key", key))
			return false
		} else {
			slog.Error("failed to check if crawled.", slog.String("key", key),
				slog.String("err", err.Error()))
			return false
		}
	}
	if string(it.Value) == "" {
		slog.Warn("cache found but the value is empty.", slog.String("key", key),
			slog.String("value", string(it.Value)))
		return false
	}

	return true
}

func (mc *MemcachedClient) IncrementThreshold(url string) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	slog.Debug("increment the threshold.")
	key := mc.generateDomainHash(url)
	value, err := mc.client.Increment(key, 0) // returns current value
	if err != nil {
		if errors.Is(err, memcache.ErrCacheMiss) {
			slog.Debug("cache not found. Creating new threshold.", slog.String("url", url))
			err = mc.set(key, 1, int32((mc.cfg.TtlForThreshold).Seconds()))
			if err != nil {
				slog.Error("failed to create new counter for the domain.", slog.String("url", url),
					slog.String("err", err.Error()))
				return err
			}
			slog.Debug("new counter is created.", slog.String("url", url), slog.String("key", key),
				slog.Uint64("value", 1))
			return nil
		} else {
			slog.Error("failed to increment the threshold.", slog.String("url", url),
				slog.String("key", key), slog.String("err", err.Error()))
			return err
		}
	}
	if value > mc.cfg.Threshold {
		slog.Info("threshold reached.", slog.String("url", url))
		return ThresholdReachedError
	}
	value, err = mc.client.Increment(key, 1)
	slog.Debug("new value is set.", slog.String("key", key), slog.Uint64("value", value))
	return nil
}

func (mc *MemcachedClient) Close() {
	slog.Info("closing memcached connection.")
	err := mc.client.Close()
	if err != nil {
		slog.Error("failed to close memcached connection.", slog.String("err", err.Error()))
	}
}

func (mc *MemcachedClient) set(key string, value any, expiration int32) error {
	byteValue, err := json.Marshal(value)
	if err != nil {
		slog.Error("failed to marshal value.", slog.String("err", err.Error()))
		return err
	}
	item := &memcache.Item{
		Key:        key,
		Value:      byteValue,
		Expiration: expiration,
	}

	return mc.client.Set(item)
}

func (mc *MemcachedClient) generateDomainHash(url string) string {
	u, err := netUrl.Parse(url)
	var key string
	if err != nil {
		slog.Error("failed to parse url. Use full url as a key.", slog.String("url", url),
			slog.String("err", err.Error()))
		key = fmt.Sprintf("%s-1m-crawl", hashURL(url))
	} else {
		key = fmt.Sprintf("%s-1m-crawl", hashURL(u.Host))
		slog.Debug("", slog.String("key:", key))
	}

	return key
}

func hashURL(url string) string {
	hash := sha256.New()
	hash.Write([]byte(url))
	return hex.EncodeToString(hash.Sum(nil))
}
