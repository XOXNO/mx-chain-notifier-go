package redis

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/multiversx/mx-chain-core-go/core/check"
	"github.com/multiversx/mx-chain-notifier-go/data"
)

type ArgsRedlockWrapper struct {
	Client       RedLockClient
	TTLInMinutes uint32
}

type redlockWrapper struct {
	client RedLockClient
	ttl    time.Duration
}

// NewRedlockWrapper create a new redLock based on a cache instance
func NewRedlockWrapper(args ArgsRedlockWrapper) (*redlockWrapper, error) {
	if check.IfNil(args.Client) {
		return nil, ErrNilRedlockClient
	}
	if args.TTLInMinutes == 0 {
		return nil, fmt.Errorf("%w for TTL in minutes", ErrZeroValueReceived)
	}

	ttl := time.Minute * time.Duration(args.TTLInMinutes)

	return &redlockWrapper{
		client: args.Client,
		ttl:    ttl,
	}, nil
}

// IsEventProcessed returns wether the item is already locked
func (r *redlockWrapper) IsEventProcessed(ctx context.Context, blockHash string) (bool, error) {
	return r.client.SetEntry(ctx, blockHash, true, r.ttl)
}

// IsCrossShardConfirmation returns true if the very same event was already seen for the given
// original tx hash. Cross-shard execution can surface the same logical event more than once
// (once per involved shard), so events are deduplicated by their full marshalled content.
func (r *redlockWrapper) IsCrossShardConfirmation(ctx context.Context, originalTxHash string, event data.EventDuplicateCheck) (bool, error) {
	jsonData, err := json.Marshal(event)
	if err != nil {
		log.Error("could not marshal event", "err", err.Error())
		return false, err
	}
	hexData := hex.EncodeToString(jsonData)
	key := fmt.Sprintf("block:cross-shard-confirmation:%s", originalTxHash)
	eventExists, err := r.client.HasEvent(ctx, key, hexData)

	if err != nil {
		log.Error("could not check if event exists", "err", err.Error())
		return false, err
	}

	if eventExists {
		return true, nil
	}

	_, err = r.client.AddEventToList(ctx, key, hexData, time.Minute*5)
	if err != nil {
		log.Error("could not add event to list", "err", err.Error())
		return false, err
	}
	return false, nil
}

// SetBlockTimestamp stores the timestamp for a given block hash
func (r *redlockWrapper) SetBlockTimestamp(ctx context.Context, blockHash string, timestamp uint64) error {
	key := fmt.Sprintf("block:timestamp:%s", blockHash)
	timestampTTL := time.Hour // 1 hour TTL for block timestamps
	return r.client.SetTimestamp(ctx, key, timestamp, timestampTTL)
}

// TryLock attempts to acquire a mutual-exclusion lock for the given key,
// returning true if it was acquired. Unlike IsEventProcessed, a lock
// acquired here is meant to be released with Unlock once the caller is done;
// the TTL only bounds how long the lock can be held if the caller crashes
// before releasing it.
func (r *redlockWrapper) TryLock(ctx context.Context, key string) (bool, error) {
	return r.client.SetEntry(ctx, key, true, r.ttl)
}

// Unlock releases a lock previously acquired with TryLock.
func (r *redlockWrapper) Unlock(ctx context.Context, key string) error {
	return r.client.DeleteEntry(ctx, key)
}

// HasConnection returns true if the redis client is connected
func (r *redlockWrapper) HasConnection(ctx context.Context) bool {
	return r.client.IsConnected(ctx)
}

// IsInterfaceNil returns true if there is no value under the interface
func (r *redlockWrapper) IsInterfaceNil() bool {
	return r == nil
}
