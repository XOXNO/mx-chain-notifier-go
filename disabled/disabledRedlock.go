package disabled

import (
	"context"

	"github.com/multiversx/mx-chain-notifier-go/data"
)

type disabledRedlockWrapper struct {
}

// NewDisabledRedlockWrapper creates a new disabled Redlock wrapper
func NewDisabledRedlockWrapper() *disabledRedlockWrapper {
	return &disabledRedlockWrapper{}
}

// IsEventProcessed returns true and nil
func (drw *disabledRedlockWrapper) IsEventProcessed(_ context.Context, _ string) (bool, error) {
	return true, nil
}

func (drw *disabledRedlockWrapper) IsCrossShardConfirmation(ctx context.Context, originalTxHash string, event data.EventDuplicateCheck) (bool, error) {
	return true, nil
}

// HasConnection returns true
func (drw *disabledRedlockWrapper) HasConnection(_ context.Context) bool {
	return true
}

// IsInterfaceNil returns true if there is no value under the interface
func (drw *disabledRedlockWrapper) IsInterfaceNil() bool {
	return drw == nil
}
