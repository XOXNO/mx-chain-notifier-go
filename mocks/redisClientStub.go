package mocks

import (
	"context"
	"time"
)

// RedisClientStub -
type RedisClientStub struct {
	SetEntryCalled       func(key string, value bool, ttl time.Duration) (bool, error)
	DeleteEntryCalled    func(key string) error
	AddEventToListCalled func(key string, value string, ttl time.Duration) (int64, error)
	HasEventCalled       func(key string, value string) (bool, error)
	SetTimestampCalled   func(key string, timestamp uint64, ttl time.Duration) error
	PingCalled           func() (string, error)
	IsConnectedCalled    func() bool
}

// SetEntry -
func (rc *RedisClientStub) SetEntry(_ context.Context, key string, value bool, ttl time.Duration) (bool, error) {
	if rc.SetEntryCalled != nil {
		return rc.SetEntryCalled(key, value, ttl)
	}

	return false, nil
}

// DeleteEntry -
func (rc *RedisClientStub) DeleteEntry(_ context.Context, key string) error {
	if rc.DeleteEntryCalled != nil {
		return rc.DeleteEntryCalled(key)
	}

	return nil
}

// Ping -
func (rc *RedisClientStub) Ping(_ context.Context) (string, error) {
	if rc.PingCalled != nil {
		return rc.PingCalled()
	}

	return "", nil
}

// IsConnected -
func (rc *RedisClientStub) IsConnected(_ context.Context) bool {
	if rc.IsConnectedCalled != nil {
		return rc.IsConnectedCalled()
	}

	return false
}

// AddEventToList -
func (rc *RedisClientStub) AddEventToList(_ context.Context, key string, value string, ttl time.Duration) (int64, error) {
	if rc.AddEventToListCalled != nil {
		return rc.AddEventToListCalled(key, value, ttl)
	}
	return 0, nil
}

// HasEvent -
func (rc *RedisClientStub) HasEvent(_ context.Context, key string, value string) (bool, error) {
	if rc.HasEventCalled != nil {
		return rc.HasEventCalled(key, value)
	}
	return false, nil
}

// SetTimestamp -
func (rc *RedisClientStub) SetTimestamp(_ context.Context, key string, timestamp uint64, ttl time.Duration) error {
	if rc.SetTimestampCalled != nil {
		return rc.SetTimestampCalled(key, timestamp, ttl)
	}
	return nil
}

// IsInterfaceNil -
func (rc *RedisClientStub) IsInterfaceNil() bool {
	return false
}
