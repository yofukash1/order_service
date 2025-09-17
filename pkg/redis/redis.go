package redis

import (
	"context"
	"fmt"
	"log"
	"time"

	r "github.com/redis/go-redis/v9"
)

type RedisClient interface {
	Set(ctx context.Context, key string, value interface{}, duration time.Duration) error
	Get(ctx context.Context, key string) (*r.StringCmd, error)
	Invalidate(ctx context.Context, key string) error
	Close() error
}

type redisClient struct {
	client *r.Client
}

func (rc *redisClient) Set(ctx context.Context, key string, value interface{}, duration time.Duration) error {
	return rc.client.Set(ctx, key, value, duration).Err()
}

func (rc *redisClient) Get(ctx context.Context, key string) (*r.StringCmd, error) {
	result := rc.client.Get(ctx, key)
	return result, result.Err()
}

func (rc *redisClient) Invalidate(ctx context.Context, key string) error {
	return rc.client.Del(ctx, key).Err()
}

func (rc *redisClient) Ping(ctx context.Context) error {
	return rc.client.Ping(ctx).Err()
}

func (rc *redisClient) Close() error {
	return rc.client.Close()
}

func NewClient() (RedisClient, error) {
	client := r.NewClient(&r.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	log.Println("Successfully connected to Redis")
	return &redisClient{client: client}, nil
}
