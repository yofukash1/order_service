package caching

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cacher interface {
	Set(ctx context.Context, key string, value interface{}, duration time.Duration) error
	Get(ctx context.Context, key string) error
	Invalidate(ctx context.Context, key string) error
}

type redisCache struct {
	client *redis.Client
}

func (r *redisCache) Set(ctx context.Context, key string, value interface{}, duration time.Duration) error {
	err := r.client.Set(ctx, key, value, duration)
	return err.Err()
}

func (r *redisCache) Get(ctx context.Context, key string) error {
	err := r.client.Get(ctx, key)
	return err.Err()
}
func (r *redisCache) Invalidate(ctx context.Context, key string) error {
	return r.Invalidate(ctx, key)
}

func NewCacher() Cacher {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // No password set
		DB:       0,  // Use default DB
		Protocol: 2,  // Connection protocol
	})
	return &redisCache{client}
}
