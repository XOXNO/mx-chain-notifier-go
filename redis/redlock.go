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

func (r *redlockWrapper) IsCrossShardConfirmation(ctx context.Context, originalTxHash string, event data.EventDuplicateCheck) (bool, error) {
	jsonData, err := json.Marshal(event)
	if err != nil {
		log.Error("could not marshal event", "err", err.Error())
		return false, err
	}
	hexData := hex.EncodeToString(jsonData)
	eventExists, err := r.client.HasEvent(ctx, originalTxHash, hexData)

	if err != nil {
		log.Error("could not check if event exists", "err", err.Error())
		return false, err
	}

	if eventExists {
		return true, nil
	}

	_, err = r.client.AddEventToList(ctx, originalTxHash, hexData, time.Minute*5)
	if err != nil {
		log.Error("could not add event to list", "err", err.Error())
		return false, err
	}
	return false, nil
}

// HasConnection returns true if the redis client is connected
func (r *redlockWrapper) HasConnection(ctx context.Context) bool {
	return r.client.IsConnected(ctx)
}

// IsInterfaceNil returns true if there is no value under the interface
func (r *redlockWrapper) IsInterfaceNil() bool {
	return r == nil
}
