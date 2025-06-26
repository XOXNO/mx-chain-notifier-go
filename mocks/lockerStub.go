package mocks

import (
	"context"

	"github.com/multiversx/mx-chain-notifier-go/data"
)

// LockerStub implements LockService interface
type LockerStub struct {
	IsEventProcessedCalled    func(ctx context.Context, blockHash string) (bool, error)
	SetBlockTimestampCalled   func(ctx context.Context, blockHash string, timestamp uint64) error
	HasConnectionCalled       func(ctx context.Context) bool
}

// IsEventProcessed -
func (ls *LockerStub) IsEventProcessed(ctx context.Context, blockHash string) (bool, error) {
	if ls.IsEventProcessedCalled != nil {
		return ls.IsEventProcessedCalled(ctx, blockHash)
	}

	return false, nil
}

func (ls *LockerStub) IsCrossShardConfirmation(ctx context.Context, originalTxHash string, event data.EventDuplicateCheck) (bool, error) {
	return false, nil
}

// SetBlockTimestamp -
func (ls *LockerStub) SetBlockTimestamp(ctx context.Context, blockHash string, timestamp uint64) error {
	if ls.SetBlockTimestampCalled != nil {
		return ls.SetBlockTimestampCalled(ctx, blockHash, timestamp)
	}
	return nil
}

// HasConnection -
func (ls *LockerStub) HasConnection(ctx context.Context) bool {
	if ls.HasConnectionCalled != nil {
		return ls.HasConnectionCalled(ctx)
	}

	return false
}

// IsInterfaceNil -
func (ls *LockerStub) IsInterfaceNil() bool {
	return ls == nil
}
