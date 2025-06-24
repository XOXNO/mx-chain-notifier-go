package dispatcher

import (
	"strings"
	"sync"

	"github.com/google/uuid"
	logger "github.com/multiversx/mx-chain-logger-go"
	"github.com/multiversx/mx-chain-notifier-go/common"
	"github.com/multiversx/mx-chain-notifier-go/data"
)

var log = logger.GetOrCreate("subscription")

const (
	// MatchAll signals that all events will be matched
	MatchAll = "*"

	// MatchAddress signals that events will be filtered by (address)
	MatchAddress = "match:address"

	// MatchAddressIdentifier signals that events will be filtered by (address,identifier)
	MatchAddressIdentifier = "match:addressIdentifier"

	// MatchIdentifier signals that events will be filtered by (identifier)
	MatchIdentifier = "match:identifier"

	// MatchTopics signals that events will be filtered by (address,identifier,[topics_pattern])
	MatchTopics = "match:topics"
)

const (
	erdTag = "erd"
)

// SubscriptionMapper defines a subscriptions manager component
type SubscriptionMapper struct {
	rwMut         sync.RWMutex
	subscriptions map[uuid.UUID][]data.Subscription

	// Cache for subscription map to avoid recreation on every call
	cacheMutex          sync.RWMutex
	cachedSubscriptions map[string][]data.Subscription
	cacheValid          bool
}

// NewSubscriptionMapper initializes an empty map for subscriptions
func NewSubscriptionMapper() *SubscriptionMapper {
	return &SubscriptionMapper{
		rwMut:               sync.RWMutex{},
		subscriptions:       make(map[uuid.UUID][]data.Subscription),
		cacheMutex:          sync.RWMutex{},
		cachedSubscriptions: make(map[string][]data.Subscription),
		cacheValid:          false,
	}
}

// MatchSubscribeEvent creates a subscription entry in the subscriptions map
// It assigns each SubscribeEvent a match level from the input provided
func (sm *SubscriptionMapper) MatchSubscribeEvent(event data.SubscribeEvent) {
	if event.SubscriptionEntries == nil || len(event.SubscriptionEntries) == 0 {
		sm.appendSubscription(data.Subscription{
			DispatcherID: event.DispatcherID,
			MatchLevel:   MatchAll,
			EventType:    common.PushLogsAndEvents,
		})
		log.Info("subscribed dispatcher",
			"dispatcherID", event.DispatcherID,
			"match level", MatchAll,
		)
		return
	}

	for _, subEntry := range event.SubscriptionEntries {
		matchLevel := sm.matchLevelFromInput(subEntry)
		eventType := getEventType(subEntry)
		subscription := data.Subscription{
			Address:      subEntry.Address,
			Identifier:   subEntry.Identifier,
			Topics:       subEntry.Topics,
			DispatcherID: event.DispatcherID,
			MatchLevel:   matchLevel,
			EventType:    eventType,
		}
		sm.appendSubscription(subscription)

		log.Info("added new subscription for dispatcher",
			"dispatcherID", event.DispatcherID,
			"match level", matchLevel,
		)
	}

	log.Info("subscribed dispatcher", "dispatcherID", event.DispatcherID)
}

// RemoveSubscriptions removes all subscriptions registered by a dispatcher
func (sm *SubscriptionMapper) RemoveSubscriptions(dispatcherID uuid.UUID) {
	sm.rwMut.Lock()
	defer sm.rwMut.Unlock()

	if _, ok := sm.subscriptions[dispatcherID]; ok {
		delete(sm.subscriptions, dispatcherID)
		sm.invalidateCache() // Invalidate cache when subscriptions change
	}

	log.Info("unsubscribed dispatcher", "dispatcherID", dispatcherID)
}

// Subscriptions returns a slice reflecting the subscriptions present in the map
func (sm *SubscriptionMapper) Subscriptions() map[string][]data.Subscription {
	// Check if cache is valid first
	sm.cacheMutex.RLock()
	if sm.cacheValid {
		result := sm.cachedSubscriptions
		sm.cacheMutex.RUnlock()
		return result
	}
	sm.cacheMutex.RUnlock()

	// Cache invalid, need to rebuild
	sm.cacheMutex.Lock()
	defer sm.cacheMutex.Unlock()

	// Double-check pattern in case another goroutine rebuilt it
	if sm.cacheValid {
		return sm.cachedSubscriptions
	}

	// Rebuild cache
	sm.rwMut.RLock()
	defer sm.rwMut.RUnlock()

	// Estimate capacity based on current subscription count
	estimatedTypes := 10 // Common event types count estimate
	sm.cachedSubscriptions = make(map[string][]data.Subscription, estimatedTypes)
	for _, sub := range sm.subscriptions {
		for _, s := range sub {
			sm.cachedSubscriptions[s.EventType] = append(sm.cachedSubscriptions[s.EventType], s)
		}
	}
	sm.cacheValid = true

	return sm.cachedSubscriptions
}

// invalidateCache marks the cache as invalid
func (sm *SubscriptionMapper) invalidateCache() {
	sm.cacheMutex.Lock()
	sm.cacheValid = false
	sm.cacheMutex.Unlock()
}

func (sm *SubscriptionMapper) matchLevelFromInput(subEntry data.SubscriptionEntry) string {
	hasAddress := subEntry.Address != "" && strings.Contains(subEntry.Address, erdTag)
	hasIdentifier := subEntry.Identifier != ""
	hasTopics := len(subEntry.Topics) > 0

	if hasAddress && hasIdentifier && hasTopics {
		return MatchTopics
	}
	if hasAddress && hasIdentifier {
		return MatchAddressIdentifier
	}
	if hasIdentifier {
		return MatchIdentifier
	}
	if hasAddress {
		return MatchAddress
	}

	return MatchAll
}

func (sm *SubscriptionMapper) appendSubscription(sub data.Subscription) {
	sm.rwMut.Lock()
	defer sm.rwMut.Unlock()

	sm.subscriptions[sub.DispatcherID] = append(sm.subscriptions[sub.DispatcherID], sub)
	sm.invalidateCache() // Invalidate cache when subscriptions change
}

func getEventType(subEntry data.SubscriptionEntry) string {
	if subEntry.EventType == common.FinalizedBlockEvents ||
		subEntry.EventType == common.RevertBlockEvents ||
		subEntry.EventType == common.BlockTxs ||
		subEntry.EventType == common.BlockScrs ||
		subEntry.EventType == common.AlteredAccountsEvent ||
		subEntry.EventType == common.BlockEvents {
		return subEntry.EventType
	}

	return common.PushLogsAndEvents
}

// IsInterfaceNil returns true if there is no value under the interface
func (sm *SubscriptionMapper) IsInterfaceNil() bool {
	return sm == nil
}
