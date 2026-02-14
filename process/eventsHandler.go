package process

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/multiversx/mx-chain-core-go/core/check"
	logger "github.com/multiversx/mx-chain-logger-go"
	"github.com/multiversx/mx-chain-notifier-go/common"
	"github.com/multiversx/mx-chain-notifier-go/data"
)

var log = logger.GetOrCreate("process")

const (
	setRetryDuration       = time.Millisecond * 500
	reconnectRetryDuration = time.Second * 2
	redisOperationTimeout  = time.Second * 5 // Timeout for Redis operations
	maxRedisRetries        = 10              // Maximum retry attempts
	minRetries             = 1
	revertKeyPrefix        = "revert_"
	finalizedKeyPrefix     = "finalized_"

	rabbitmqMetricPrefix = "RabbitMQ"
	redisMetricPrefix    = "Redis"
)

// ArgsEventsHandler defines the arguments needed for an events handler
type ArgsEventsHandler struct {
	Locker               LockService
	Publisher            Publisher
	Publishers           []Publisher
	StatusMetricsHandler common.StatusMetricsHandler
	EventsInterceptor    EventsInterceptor
	CheckDuplicates      bool
}

type eventsHandler struct {
	locker            LockService
	publishers        []Publisher
	metricsHandler    common.StatusMetricsHandler
	eventsInterceptor EventsInterceptor
	checkDuplicates   bool
}

// NewEventsHandler creates a new events handler component
func NewEventsHandler(args ArgsEventsHandler) (*eventsHandler, error) {
	err := checkArgs(args)
	if err != nil {
		return nil, err
	}

	publishers := mergePublishers(args.Publisher, args.Publishers)

	return &eventsHandler{
		locker:            args.Locker,
		publishers:        publishers,
		metricsHandler:    args.StatusMetricsHandler,
		eventsInterceptor: args.EventsInterceptor,
		checkDuplicates:   args.CheckDuplicates,
	}, nil
}

func checkArgs(args ArgsEventsHandler) error {
	if check.IfNil(args.Locker) {
		return ErrNilLockService
	}

	hasValidPublisher := false
	if !check.IfNil(args.Publisher) {
		hasValidPublisher = true
	}
	if !hasValidPublisher {
		for _, publisher := range args.Publishers {
			if !check.IfNil(publisher) {
				hasValidPublisher = true
				break
			}
		}
	}
	if !hasValidPublisher {
		return ErrNilPublisherService
	}

	if check.IfNil(args.StatusMetricsHandler) {
		return common.ErrNilStatusMetricsHandler
	}
	if check.IfNil(args.EventsInterceptor) {
		return ErrNilEventsInterceptor
	}

	return nil
}

func mergePublishers(single Publisher, publishers []Publisher) []Publisher {
	allPublishers := make([]Publisher, 0, len(publishers)+1)

	if !check.IfNil(single) {
		allPublishers = append(allPublishers, single)
	}

	for _, publisher := range publishers {
		if check.IfNil(publisher) {
			continue
		}

		allPublishers = append(allPublishers, publisher)
	}

	return allPublishers
}

// HandleSaveBlockEvents will handle save block events received from observer
func (eh *eventsHandler) HandleSaveBlockEvents(allEvents data.ArgsSaveBlockData) error {
	blockHash := hex.EncodeToString(allEvents.HeaderHash)
	shouldProcessPushEvents := eh.shouldProcessSaveBlockEvents(blockHash)
	if !shouldProcessPushEvents {
		return nil
	}

	if check.IfNil(allEvents.Header) {
		return ErrNilBlockHeader
	}

	if allEvents.Header.IsHeaderV3() {
		return eh.handleSaveBlockEventsV3(allEvents)
	}

	return eh.handleSaveBlockEventsLegacy(allEvents, blockHash)
}

func (eh *eventsHandler) handleSaveBlockEventsLegacy(allEvents data.ArgsSaveBlockData, blockHash string) error {
	eventsData, err := eh.eventsInterceptor.ProcessBlockEvents(&allEvents)
	if err != nil {
		log.Error("eventsHandler: failed to process block events", "blockHash", blockHash, "error", err)
		return err
	}

	if eventsData == nil {
		return ErrNilEventsInterceptor
	}
	if check.IfNil(eventsData.Header) {
		return ErrNilBlockHeader
	}

	// Store block timestamp in Redis
	err = eh.locker.SetBlockTimestamp(context.Background(), blockHash, eventsData.Header.GetTimeStamp())
	if err != nil {
		log.Warn("could not store block timestamp", "blockHash", blockHash, "error", err)
	}

	headerTimeStamp := eventsData.Header.GetTimeStamp()
	headerTimeStampMs := allEvents.HeaderTimeStampMs
	shardID := eventsData.Header.GetShardID()
	nonce := eventsData.Header.GetNonce()

	return eh.handleSaveBlockEvents(
		eventsData,
		headerTimeStamp,
		headerTimeStampMs,
		shardID,
		nonce,
	)
}

func (eh *eventsHandler) handleSaveBlockEvents(
	eventsData *data.InterceptorBlockData,
	headerTimeStamp uint64,
	headerTimeStampMs uint64,
	shardID uint32,
	nonce uint64,
) error {
	if eventsData == nil {
		return ErrNilEventsInterceptor
	}
	if check.IfNil(eventsData.Header) {
		return ErrNilBlockHeader
	}

	pushEvents := data.BlockEvents{
		Hash:        eventsData.Hash,
		ShardID:     shardID,
		TimeStamp:   headerTimeStamp,
		TimeStampMs: headerTimeStampMs,
		Events:      eventsData.LogEvents,
	}
	err := eh.handlePushEvents(pushEvents)
	if err != nil {
		return err
	}

	// Log completion after dedupe gate to avoid duplicate "completed" lines with multiple observers
	log.Info("eventsHandler: save block processed", "blockHash", eventsData.Hash, "events", len(eventsData.LogEvents), "txs", len(eventsData.Txs), "scrs", len(eventsData.Scrs), "alteredAccounts", len(eventsData.AlteredAccounts))

	txs := data.BlockTxs{
		Hash: eventsData.Hash,
		Txs:  eventsData.Txs,
	}
	eh.handleBlockTxs(txs)

	scrs := data.BlockScrs{
		Hash: eventsData.Hash,
		Scrs: eventsData.Scrs,
	}
	eh.handleBlockScrs(scrs)

	alteredEvent := data.AlteredAccountsEvent{
		Hash:      eventsData.Hash,
		ShardID:   eventsData.Header.GetShardID(),
		TimeStamp: eventsData.Header.GetTimeStamp(),
		Accounts:  eventsData.AlteredAccounts,
	}

	txsWithOrder := data.BlockEventsWithOrder{
		Hash:        eventsData.Hash,
		ShardID:     shardID,
		TimeStamp:   headerTimeStamp,
		TimeStampMs: headerTimeStampMs,
		Txs:         eventsData.TxsWithOrder,
		Scrs:        eventsData.ScrsWithOrder,
		Events:      eventsData.LogEvents,
	}
	eh.handleBlockEventsWithOrder(txsWithOrder)
	eh.handleAlteredAccounts(alteredEvent)

	stateAccesses := data.BlockStateAccesses{
		Hash:                     eventsData.Hash,
		ShardID:                  shardID,
		TimeStampMs:              headerTimeStampMs,
		Nonce:                    nonce,
		StateAccessesPerAccounts: eventsData.StateAccessesPerAccounts,
	}
	eh.handleStateAccesses(stateAccesses)

	return nil
}

func (eh *eventsHandler) handleSaveBlockEventsV3(allEvents data.ArgsSaveBlockData) error {
	executionResultsData, err := eh.eventsInterceptor.ProcessBlockEventsV3(&allEvents)
	if err != nil {
		return err
	}

	shardID := allEvents.Header.GetShardID()

	for _, executionResultData := range executionResultsData {
		timeStampSec := common.ConvertTimeStampMsToSec(executionResultData.TimeStampMs) // this is used for backwards compatibility
		err = eh.handleSaveBlockEvents(
			executionResultData,
			timeStampSec,
			executionResultData.TimeStampMs,
			shardID,
			executionResultData.Nonce,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// HandlePushEvents will handle push events received from observer
func (eh *eventsHandler) handlePushEvents(events data.BlockEvents) error {
	if events.Hash == "" {
		log.Debug("received empty hash", "event", common.PushLogsAndEvents,
			"will process", false,
		)
		return common.ErrReceivedEmptyEvents
	}

	if len(events.Events) == 0 {
		log.Debug("received empty events", "event", common.PushLogsAndEvents,
			"block hash", events.Hash,
		)
		events.Events = make([]data.Event, 0)
	}

	t := time.Now()
	eh.broadcastConcurrent(func(publisher Publisher) {
		publisher.Broadcast(events)
	})
	eh.metricsHandler.AddRequest(getRabbitOpID(common.PushLogsAndEvents), time.Since(t))
	return nil
}

func (eh *eventsHandler) shouldProcessSaveBlockEvents(blockHash string) bool {
	shouldProcessEvents := true
	if eh.checkDuplicates {
		shouldProcessEvents = eh.tryCheckProcessedWithRetry(common.PushLogsAndEvents, blockHash)
	}

	if !shouldProcessEvents {
		log.Info("received duplicated push events",
			"block hash", blockHash,
			"will process", false,
		)

		return false
	}

	return true
}

// HandleRevertEvents will handle reverts events received from observer
func (eh *eventsHandler) HandleRevertEvents(revertBlock data.RevertBlock) {
	if revertBlock.Hash == "" {
		log.Warn("received empty hash", "event", common.RevertBlockEvents,
			"will process", false,
		)
		return
	}

	shouldProcessRevert := true
	if eh.checkDuplicates {
		shouldProcessRevert = eh.tryCheckProcessedWithRetry(common.RevertBlockEvents, revertBlock.Hash)
	}

	if !shouldProcessRevert {
		return
	}

	t := time.Now()
	eh.broadcastConcurrent(func(publisher Publisher) {
		publisher.BroadcastRevert(revertBlock)
	})
	eh.metricsHandler.AddRequest(getRabbitOpID(common.RevertBlockEvents), time.Since(t))
}

// HandleFinalizedEvents will handle finalized events received from observer
func (eh *eventsHandler) HandleFinalizedEvents(finalizedBlock data.FinalizedBlock) {
	if finalizedBlock.Hash == "" {
		log.Warn("received empty hash", "event", common.FinalizedBlockEvents,
			"will process", false,
		)
		return
	}
	shouldProcessFinalized := true
	if eh.checkDuplicates {
		shouldProcessFinalized = eh.tryCheckProcessedWithRetry(common.FinalizedBlockEvents, finalizedBlock.Hash)
	}

	if !shouldProcessFinalized {
		return
	}

	t := time.Now()
	eh.broadcastConcurrent(func(publisher Publisher) {
		publisher.BroadcastFinalized(finalizedBlock)
	})
	eh.metricsHandler.AddRequest(getRabbitOpID(common.FinalizedBlockEvents), time.Since(t))
}

// handleBlockTxs will handle txs events received from observer
func (eh *eventsHandler) handleBlockTxs(blockTxs data.BlockTxs) {
	if blockTxs.Hash == "" {
		log.Warn("received empty hash", "event", common.BlockTxs,
			"will process", false,
		)
		return
	}

	if len(blockTxs.Txs) == 0 {
		log.Debug("received empty events", "event", common.BlockTxs,
			"block hash", blockTxs.Hash,
		)
	}

	t := time.Now()
	eh.broadcastConcurrent(func(publisher Publisher) {
		publisher.BroadcastTxs(blockTxs)
	})
	eh.metricsHandler.AddRequest(getRabbitOpID(common.BlockTxs), time.Since(t))
}

// handleBlockScrs will handle scrs events received from observer
func (eh *eventsHandler) handleBlockScrs(blockScrs data.BlockScrs) {
	if blockScrs.Hash == "" {
		log.Warn("received empty hash", "event", common.BlockScrs,
			"will process", false,
		)
		return
	}

	if len(blockScrs.Scrs) == 0 {
		log.Debug("received empty events", "event", common.BlockScrs,
			"block hash", blockScrs.Hash,
		)
	}

	t := time.Now()
	eh.broadcastConcurrent(func(publisher Publisher) {
		publisher.BroadcastScrs(blockScrs)
	})
	eh.metricsHandler.AddRequest(getRabbitOpID(common.BlockScrs), time.Since(t))
}

// handleBlockEventsWithOrder will handle full block events received from observer
func (eh *eventsHandler) handleBlockEventsWithOrder(blockTxs data.BlockEventsWithOrder) {
	if blockTxs.Hash == "" {
		log.Warn("received empty hash", "event", common.BlockEvents,
			"will process", false,
		)
		return
	}

	t := time.Now()
	eh.broadcastConcurrent(func(publisher Publisher) {
		publisher.BroadcastBlockEventsWithOrder(blockTxs)
	})
	eh.metricsHandler.AddRequest(getRabbitOpID(common.BlockEvents), time.Since(t))
}

// handleAlteredAccounts will handle altered accounts events received from observer
func (eh *eventsHandler) handleAlteredAccounts(alteredAccountsEvent data.AlteredAccountsEvent) {
	if len(alteredAccountsEvent.Accounts) == 0 {
		return
	}

	t := time.Now()
	eh.broadcastConcurrent(func(publisher Publisher) {
		publisher.BroadcastAlteredAccounts(alteredAccountsEvent)
	})
	eh.metricsHandler.AddRequest(getRabbitOpID(common.AlteredAccountsEvent), time.Since(t))
}

func (eh *eventsHandler) handleStateAccesses(stateAccesses data.BlockStateAccesses) {
	if stateAccesses.Hash == "" {
		log.Warn("received empty state accesses",
			"will process", false,
		)
		return
	}

	t := time.Now()
	eh.broadcastConcurrent(func(publisher Publisher) {
		publisher.BroadcastStateAccesses(stateAccesses)
	})
	eh.metricsHandler.AddRequest(getRabbitOpID(common.BlockStateAccesses), time.Since(t))
}

func (eh *eventsHandler) tryCheckProcessedWithRetry(id, blockHash string) bool {
	var err error
	var setSuccessful bool

	key := fmt.Sprintf("block:%s:%s", id, blockHash)

	for attempt := 0; attempt < maxRedisRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), redisOperationTimeout)

		t := time.Now()
		setSuccessful, err = eh.locker.IsEventProcessed(ctx, key)
		eh.metricsHandler.AddRequest(getRedisOpID(id), time.Since(t))
		cancel()

		if err == nil {
			log.Debug("locker", "event", id, "block hash", blockHash, "succeeded", setSuccessful, "attempt", attempt+1)
			return setSuccessful
		}

		log.Error("failed to check event in locker", "error", err.Error(), "attempt", attempt+1, "maxRetries", maxRedisRetries)

		if attempt >= maxRedisRetries-1 {
			break
		}

		if !eh.locker.HasConnection(context.Background()) {
			log.Error("failure connecting to locker service", "attempt", attempt+1)
			time.Sleep(reconnectRetryDuration)
		} else {
			time.Sleep(setRetryDuration)
		}
	}

	log.Error("exhausted all Redis retry attempts", "event", id, "blockHash", blockHash, "maxRetries", maxRedisRetries)
	return false
}

// broadcastConcurrent executes the given broadcast function concurrently across all publishers
func (eh *eventsHandler) broadcastConcurrent(broadcastFunc func(Publisher)) {
	var wg sync.WaitGroup
	for _, publisher := range eh.publishers {
		wg.Add(1)
		go func(pub Publisher) {
			defer wg.Done()
			broadcastFunc(pub)
		}(publisher)
	}
	wg.Wait()
}

func getRabbitOpID(operation string) string {
	return fmt.Sprintf("%s-%s", rabbitmqMetricPrefix, operation)
}

func getRedisOpID(operation string) string {
	return fmt.Sprintf("%s-%s", redisMetricPrefix, operation)
}

// IsInterfaceNil returns true if there is no value under the interface
func (eh *eventsHandler) IsInterfaceNil() bool {
	return eh == nil
}
