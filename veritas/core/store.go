package core

import (
	"context"
	"sync"

	"github.com/go-redis/redis/v8"
)

// KVStore is the replica's key/value state. Values are veritas.Encode'd
// entries (value + version).
type KVStore interface {
	// Get returns ok=false (and err=nil) when the key does not exist.
	Get(ctx context.Context, key string) (val string, ok bool, err error)
	Set(ctx context.Context, key, val string) error
}

type redisStore struct {
	cli *redis.Client
}

// NewRedisStore wraps a go-redis client. Keys are written without expiry,
// as in the original veritas server.
func NewRedisStore(cli *redis.Client) KVStore {
	return &redisStore{cli: cli}
}

func (s *redisStore) Get(ctx context.Context, key string) (string, bool, error) {
	v, err := s.cli.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

func (s *redisStore) Set(ctx context.Context, key, val string) error {
	return s.cli.Set(ctx, key, val, 0).Err()
}

type mapStore struct {
	mu sync.RWMutex
	m  map[string]string
}

// NewMapStore returns an in-memory, mutex-guarded KVStore for tests.
func NewMapStore() KVStore {
	return &mapStore{m: make(map[string]string)}
}

func (s *mapStore) Get(ctx context.Context, key string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	return v, ok, nil
}

func (s *mapStore) Set(ctx context.Context, key, val string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = val
	return nil
}
