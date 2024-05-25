package redis

import (
	"context"
	"time"

	"github.com/multiversx/mx-chain-notifier-go/data"
)

// LockService defines the behaviour of a lock service component.
// It makes sure that a duplicated entry is not processed multiple times.
type LockService interface {
	IsEventProcessed(ctx context.Context, blockHash string) (bool, error)
	IsCrossShardConfirmation(ctx context.Context, originalTxHash string, event data.EventDuplicateCheck) (bool, error)
	HasConnection(ctx context.Context) bool
	IsInterfaceNil() bool
}

// RedLockClient defines the behaviour of a cache handler component
type RedLockClient interface {
	SetEntry(ctx context.Context, key string, value bool, ttl time.Duration) (bool, error)
	AddEventToList(ctx context.Context, key string, value string, ttl time.Duration) (int64, error)
	HasEvent(ctx context.Context, key string, value string) (bool, error)
	Ping(ctx context.Context) (string, error)
	IsConnected(ctx context.Context) bool
	IsInterfaceNil() bool
}
