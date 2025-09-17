package cacher

import (
	"context"
	"time"
	"wb_tech/L0/pkg/redis"
)

type CacherSystem struct {
	cacher redis.RedisClient
}

type Cacher interface {
	Set(ctx context.Context, key string, value interface{}, duration time.Duration) error
	Get(ctx context.Context, key string) (string, error)
	Invalidate(ctx context.Context, key string) error
}

func (cs *CacherSystem) Set(ctx context.Context, key string, value interface{}, duration time.Duration) error {
	return cs.cacher.Set(ctx, key, value, duration)
}

func (cs *CacherSystem) Get(ctx context.Context, key string) (string, error) {
	res, err := cs.cacher.Get(ctx, key)
	return res.Val(), err
}

func (cs *CacherSystem) Invalidate(ctx context.Context, key string) error {
	return cs.cacher.Invalidate(ctx, key)
}

func NewCacher(redisClient redis.RedisClient) *CacherSystem {
	return &CacherSystem{
		cacher: redisClient,
	}
}
