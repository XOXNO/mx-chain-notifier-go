package redis

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	pongValue = "PONG"
)

type redisClientWrapper struct {
	redis *redis.Client
}

// NewRedisClientWrapper creates a wrapper over redis client instance. This is an abstraction
// over different redis connections: single instance, sentinel, cluster.
func NewRedisClientWrapper(redis *redis.Client) *redisClientWrapper {
	return &redisClientWrapper{
		redis: redis,
	}
}

// SetEntry will try to update a key value entry in redis database
func (rc *redisClientWrapper) SetEntry(ctx context.Context, key string, value bool, ttl time.Duration) (bool, error) {
	return rc.redis.SetNX(ctx, key, value, ttl).Result()
}

// Ping will check if Redis instance is reponding
func (rc *redisClientWrapper) Ping(ctx context.Context) (string, error) {
	return rc.redis.Ping(ctx).Result()
}

func (rc *redisClientWrapper) AddEventToList(ctx context.Context, key string, value string, ttl time.Duration) (int64, error) {
	index, err := rc.redis.SAdd(ctx, key, value).Result()
	if err != nil {
		log.Error("could not add event to list", "key", key, "err", err.Error())
		return 0, err
	}

	_, err = rc.redis.Expire(ctx, key, ttl).Result()
	if err != nil {
		log.Error("could not set expiration for key", "key", key, "err", err.Error())
		return 0, err
	}
	return index, nil
}

// GetEventList will try to get the list of events from redis database
func (rc *redisClientWrapper) HasEvent(ctx context.Context, key string, value string) (bool, error) {
	return rc.redis.SIsMember(ctx, key, value).Result()
}

// IsConnected will check if Redis is connected
func (rc *redisClientWrapper) IsConnected(ctx context.Context) bool {
	pong, err := rc.Ping(context.Background())
	return err == nil && pong == pongValue
}

// IsInterfaceNil returns true if there is no value under the interface
func (rc *redisClientWrapper) IsInterfaceNil() bool {
	return rc == nil
}
