package process

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/multiversx/mx-chain-core-go/core"
	"github.com/multiversx/mx-chain-core-go/core/check"
	coreData "github.com/multiversx/mx-chain-core-go/data"
	"github.com/multiversx/mx-chain-core-go/data/alteredAccount"
	"github.com/multiversx/mx-chain-core-go/data/outport"
	"github.com/multiversx/mx-chain-core-go/data/smartContractResult"
	"github.com/multiversx/mx-chain-core-go/data/stateChange"
	"github.com/multiversx/mx-chain-core-go/data/transaction"
	logger "github.com/multiversx/mx-chain-logger-go"
	"github.com/multiversx/mx-chain-notifier-go/data"
)

type txType int

const (
	normalTx txType = iota
	scr
	rewardTx
	invalidTx
)

type txWithOrder struct {
	Hash   string
	Index  uint32
	TxType txType
}

// logEvent defines a log event associated with corresponding tx hash
type logEvent struct {
	EventHandler   coreData.EventHandler
	TxHash         string
	OriginalTxHash string
}

// ArgsEventsInterceptor defines the arguments needed for creating an events interceptor instance
type ArgsEventsInterceptor struct {
	PubKeyConverter      core.PubkeyConverter
	WithReadStateChanges bool
	LockService          LockService
}

type eventsInterceptor struct {
	pubKeyConverter      core.PubkeyConverter
	withReadStateChanges bool
	locker               LockService
}

// NewEventsInterceptor creates a new eventsInterceptor instance
func NewEventsInterceptor(args ArgsEventsInterceptor) (*eventsInterceptor, error) {
	if check.IfNil(args.PubKeyConverter) {
		return nil, ErrNilPubKeyConverter
	}

	return &eventsInterceptor{
		pubKeyConverter:      args.PubKeyConverter,
		withReadStateChanges: args.WithReadStateChanges,
		locker:               args.LockService,
	}, nil
}

func baseNilEventsDataChecks(eventsData *data.ArgsSaveBlockData) error {
	if eventsData == nil {
		return ErrNilBlockEvents
	}
	if eventsData.Body == nil {
		return ErrNilBlockBody
	}
	if eventsData.Header == nil {
		return ErrNilBlockHeader
	}

	return nil
}

// ProcessBlockEvents will process block events data
func (ei *eventsInterceptor) ProcessBlockEvents(eventsData *data.ArgsSaveBlockData) (*data.InterceptorBlockData, error) {
	err := baseNilEventsDataChecks(eventsData)
	if err != nil {
		return nil, err
	}
	if eventsData.TransactionsPool == nil {
		return nil, ErrNilTransactionsPool
	}

	transactionsPool := eventsData.TransactionsPool
	scrs := getScrsFromPool(transactionsPool)
	events := ei.getLogEventsFromTransactionsPool(transactionsPool.Logs, scrs)
	stateAccessesPerAccounts := ei.getStateAccessesPerAccounts(eventsData, hex.EncodeToString(eventsData.HeaderHash), transactionsPool)

	// left nil when there are none, so the emitted payload keeps the previous
	// `null` representation instead of switching to `[]` for every block
	var accounts []*alteredAccount.AlteredAccount
	if len(eventsData.AlteredAccounts) > 0 {
		accounts = make([]*alteredAccount.AlteredAccount, 0, len(eventsData.AlteredAccounts))
		for _, account := range eventsData.AlteredAccounts {
			accounts = append(accounts, account)
		}
	}

	return &data.InterceptorBlockData{
		Hash:                     hex.EncodeToString(eventsData.HeaderHash),
		Body:                     eventsData.Body,
		Header:                   eventsData.Header,
		Txs:                      getTxsFromPool(transactionsPool),
		TxsWithOrder:             transactionsPool.GetTransactions(),
		Scrs:                     scrs,
		ScrsWithOrder:            transactionsPool.GetSmartContractResults(),
		LogEvents:                events,
		AlteredAccounts:          accounts,
		StateAccessesPerAccounts: stateAccessesPerAccounts,
	}, nil
}

// ProcessBlockEventsV3 will process block events data for async execution model
func (ei *eventsInterceptor) ProcessBlockEventsV3(eventsData *data.ArgsSaveBlockData) ([]*data.InterceptorBlockData, error) {
	err := baseNilEventsDataChecks(eventsData)
	if err != nil {
		return nil, err
	}

	if !eventsData.Header.IsHeaderV3() {
		return nil, coreData.ErrInvalidHeaderType
	}

	// a decoded payload cannot distinguish a nil Results map from an empty one, since
	// protobuf encodes an empty map as nothing and decodes an absent map to nil. Both
	// mean the same thing: this V3 block carries no execution results.
	execBlocksData := make([]*data.InterceptorBlockData, 0)
	if len(eventsData.Results) == 0 {
		return execBlocksData, nil
	}

	// left nil when there are none, so the emitted payload keeps the previous
	// `null` representation instead of switching to `[]` for every block
	var accounts []*alteredAccount.AlteredAccount
	if len(eventsData.AlteredAccounts) > 0 {
		accounts = make([]*alteredAccount.AlteredAccount, 0, len(eventsData.AlteredAccounts))
		for _, account := range eventsData.AlteredAccounts {
			accounts = append(accounts, account)
		}
	}

	for headerHash, execBlockData := range eventsData.Results {
		transactionsPool := execBlockData.GetTransactionPool()
		if transactionsPool == nil {
			return nil, fmt.Errorf("%w: for execution results block data", ErrNilTransactionsPool)
		}

		body := execBlockData.Body
		scrs := getScrsFromPool(transactionsPool)
		events := ei.getLogEventsFromTransactionsPool(transactionsPool.GetLogs(), scrs)
		stateAccessesPerAccounts := ei.getStateAccessesPerAccountsV3(eventsData, headerHash, transactionsPool)

		blockData := &data.InterceptorBlockData{
			Hash:                     headerHash,
			Body:                     body,
			Header:                   eventsData.Header, // this holds current proposed header, not executed header
			Txs:                      getTxsFromPool(transactionsPool),
			TxsWithOrder:             transactionsPool.GetTransactions(),
			Scrs:                     scrs,
			ScrsWithOrder:            transactionsPool.GetSmartContractResults(),
			LogEvents:                events,
			AlteredAccounts:          accounts,
			StateAccessesPerAccounts: stateAccessesPerAccounts,
			Nonce:                    execBlockData.GetHeaderNonce(),
			TimeStampMs:              execBlockData.GetTimestampMs(),
		}

		execBlocksData = append(execBlocksData, blockData)
	}

	sort.Slice(execBlocksData, func(i, j int) bool {
		return execBlocksData[i].Nonce < execBlocksData[j].Nonce
	})

	return execBlocksData, nil
}

func getScrsFromPool(transactionsPool *outport.TransactionPool) map[string]*smartContractResult.SmartContractResult {
	scrs := make(map[string]*smartContractResult.SmartContractResult)

	for hash, scr := range transactionsPool.GetSmartContractResults() {
		scrs[hash] = scr.SmartContractResult
	}

	return scrs
}

func getTxsFromPool(transactionsPool *outport.TransactionPool) map[string]*transaction.Transaction {
	txs := make(map[string]*transaction.Transaction)

	for hash, tx := range transactionsPool.GetTransactions() {
		txs[hash] = tx.Transaction
	}

	return txs
}

func getTxsWithOrder(transactionsPool *outport.TransactionPool) []txWithOrder {
	// This map is needed because of duplicated transactions.
	// There can be a case when a transaction is included in the block, but also marked as invalid, so it will be present
	// in both transactions and invalidTxs maps from transactions pool, with the same execution order. In that case,
	// we want to make sure that we process that transaction only as invalid.
	numTxs := len(transactionsPool.Transactions) +
		len(transactionsPool.SmartContractResults) +
		len(transactionsPool.Rewards) +
		len(transactionsPool.InvalidTxs)
	txsWithOrderMap := make(map[string]txWithOrder, numTxs)

	for txHash, txInfo := range transactionsPool.Transactions {
		txsWithOrderMap[txHash] = txWithOrder{
			Hash:   txHash,
			Index:  txInfo.ExecutionOrder,
			TxType: normalTx,
		}
	}
	for txHash, txInfo := range transactionsPool.SmartContractResults {
		txsWithOrderMap[txHash] = txWithOrder{
			Hash:   txHash,
			Index:  txInfo.ExecutionOrder,
			TxType: scr,
		}
	}
	for txHash, txInfo := range transactionsPool.Rewards {
		txsWithOrderMap[txHash] = txWithOrder{
			Hash:   txHash,
			Index:  txInfo.ExecutionOrder,
			TxType: rewardTx,
		}
	}
	for txHash, txInfo := range transactionsPool.InvalidTxs {
		txsWithOrderMap[txHash] = txWithOrder{
			Hash:   txHash,
			Index:  txInfo.ExecutionOrder,
			TxType: invalidTx,
		}
	}

	txsWithOrder := make([]txWithOrder, 0, len(txsWithOrderMap))
	for _, txWithData := range txsWithOrderMap {
		txsWithOrder = append(txsWithOrder, txWithData)
	}

	sort.Slice(txsWithOrder, func(i, j int) bool {
		return txsWithOrder[i].Index < txsWithOrder[j].Index
	})

	return txsWithOrder
}

func (ei *eventsInterceptor) getStateAccessesPerAccounts(
	eventsData *data.ArgsSaveBlockData,
	headerHash string,
	transactionPool *outport.TransactionPool,
) map[string]*stateChange.StateAccesses {
	if eventsData.StateAccesses == nil {
		log.Debug("getStateAccessesPerAccounts failed: will return empty state accesses per accounts",
			"block hash", headerHash,
			"error", ErrNilStateAccesses,
		)

		return make(map[string]*stateChange.StateAccesses)
	}

	stateAccesses := eventsData.StateAccesses

	return ei.fetchStateAccessesPerAccounts(stateAccesses, transactionPool)
}

func (ei *eventsInterceptor) getStateAccessesPerAccountsV3(
	eventsData *data.ArgsSaveBlockData,
	headerHash string,
	transactionPool *outport.TransactionPool,
) map[string]*stateChange.StateAccesses {
	stateAccessesPerBlock, ok := eventsData.StateAccessesForBlock[headerHash]
	if !ok {
		log.Debug("stateAccessesPerBlock failed: will return empty state accesses per accounts",
			"block hash", headerHash,
		)

		return make(map[string]*stateChange.StateAccesses)
	}

	if stateAccessesPerBlock == nil {
		log.Debug("stateAccessesPerBlock failed: will return empty state accesses per accounts",
			"block hash", headerHash,
			"num state accesses for block", len(eventsData.StateAccessesForBlock),
		)

		return make(map[string]*stateChange.StateAccesses)
	}

	stateAccesses := stateAccessesPerBlock.StateAccesses

	return ei.fetchStateAccessesPerAccounts(stateAccesses, transactionPool)
}

func (ei *eventsInterceptor) fetchStateAccessesPerAccounts(
	stateAccesses map[string]*stateChange.StateAccesses,
	transactionPool *outport.TransactionPool,
) map[string]*stateChange.StateAccesses {
	if stateAccesses == nil {
		return make(map[string]*stateChange.StateAccesses)
	}

	stateAccessesPerAccounts := make(map[string]*stateChange.StateAccesses)

	logStateAccessesPerTxs(stateAccesses)

	// txs hashes with order
	txsWithOrder := getTxsWithOrder(transactionPool)

	for _, txInfo := range txsWithOrder {
		txHash, err := hex.DecodeString(txInfo.Hash)
		if err != nil {
			log.Error("failed to decode tx hash", "txHash", txInfo.Hash)
			continue
		}

		stateAccessesPerTx, ok := stateAccesses[string(txHash)]
		if !ok {
			if txInfo.TxType == scr {
				// there are cases when SCRs are generated but no state accesses are produced, so we will not log a warning in those cases
				log.Trace("SCR with no state accesses", "txHash", txInfo.Hash)
				continue
			}

			log.Warn("did not find state accesses for tx", "txHash", txInfo.Hash, "txType", txInfo.TxType)
			continue
		}

		for _, stateAccess := range stateAccessesPerTx.StateAccess {
			if stateAccess.Type == stateChange.Read && !ei.withReadStateChanges {
				continue
			}

			accKey := hex.EncodeToString(stateAccess.MainTrieKey)
			_, ok := stateAccessesPerAccounts[accKey]
			if !ok {
				stateAccessesPerAccounts[accKey] = &stateChange.StateAccesses{
					StateAccess: make([]*stateChange.StateAccess, 0),
				}
			}

			stateAccessesPerAccounts[accKey].StateAccess = append(stateAccessesPerAccounts[accKey].StateAccess, stateAccess)
		}
	}

	log.Trace("getStateAccessesPerAccounts",
		"num stateAccessesPerAccounts", len(stateAccessesPerAccounts),
	)

	return stateAccessesPerAccounts
}

func logStateAccessesPerTxs(stateAccesses map[string]*stateChange.StateAccesses) {
	if log.GetLevel() > logger.LogTrace {
		return
	}

	log.Trace("getStateAccessesPerAccounts",
		"num stateAccessesPerTxs", len(stateAccesses),
	)

	for txHash, sts := range stateAccesses {
		log.Trace("stateAccessesPerTx",
			"txHash", txHash,
		)

		for _, st := range sts.StateAccess {
			log.Trace("st",
				"actionType", st.GetType(),
				"operation", st.GetOperation(),
			)
		}
	}
}

func (ei *eventsInterceptor) getLogEventsFromTransactionsPool(logs []*transaction.LogData, scrs map[string]*smartContractResult.SmartContractResult) []data.Event {
	var logEvents []*logEvent
	for _, logData := range logs {
		if logData == nil {
			continue
		}
		if check.IfNilReflect(logData.Log) {
			continue
		}
		var tmpLogEvents []*logEvent
		skipTransfers := false
		duplicateTwiceSameBlock := make(map[string]bool)
		for _, event := range logData.Log.Events {
			eventIdentifier := string(event.Identifier)
			originalTxHash := logData.TxHash
			scResult, exists := scrs[originalTxHash]

			// Check if the current TX is a smart contract result, if so get the original TxHash
			if exists {
				originalTxHash = hex.EncodeToString(scResult.GetOriginalTxHash())
			}

			if eventIdentifier == core.SignalErrorOperation || eventIdentifier == core.InternalVMErrorsOperation {
				if !exists {
					skipTransfers = true
				}
				log.Debug("eventsInterceptor: received signalError or internalVMErrors event from log event",
					"txHash", logData.TxHash,
					"isSCResult", exists,
					"skipTransfers", skipTransfers,
				)
			}

			// Skip duplicated transfers for cross shard confirmation
			if (eventIdentifier == core.BuiltInFunctionMultiESDTNFTTransfer || eventIdentifier == core.BuiltInFunctionESDTNFTTransfer || eventIdentifier == core.BuiltInFunctionESDTTransfer) && ei.locker != nil {
				eventDuplicateCheck := data.EventDuplicateCheck{
					Address:    event.Address,
					Identifier: event.Identifier,
					Topics:     event.Topics,
				}
				jsonData, err := json.Marshal(eventDuplicateCheck)
				if err != nil {
					log.Error("could not marshal event", "err", err.Error())
					return nil
				}
				hexData := hex.EncodeToString(jsonData)
				_, isThere := duplicateTwiceSameBlock[originalTxHash+hexData]
				if !isThere {
					skipEvent, err := ei.locker.IsCrossShardConfirmation(context.Background(), originalTxHash, eventDuplicateCheck)
					// Save this as already seen in this logs block
					duplicateTwiceSameBlock[originalTxHash+hexData] = true
					if err != nil {
						log.Error("eventsInterceptor: failed to check cross shard confirmation", "error", err)
						continue
					}
					if skipEvent {
						log.Debug("eventsInterceptor: skip cross shard confirmation event", "txHash", logData.TxHash, "originalTxHash", originalTxHash, "eventIdentifier", eventIdentifier)
						continue
					}
				}
			}

			le := &logEvent{
				EventHandler:   event,
				TxHash:         logData.TxHash,
				OriginalTxHash: originalTxHash,
			}

			tmpLogEvents = append(tmpLogEvents, le)
		}
		if skipTransfers {
			filteredItems := make([]*logEvent, 0, len(tmpLogEvents))
			for _, item := range tmpLogEvents {
				identifier := string(item.EventHandler.GetIdentifier())
				if identifier == core.BuiltInFunctionMultiESDTNFTTransfer || identifier == core.BuiltInFunctionESDTNFTTransfer || identifier == core.BuiltInFunctionESDTTransfer {
					continue
				}
				filteredItems = append(filteredItems, item)
			}
			logEvents = append(logEvents, filteredItems...)
		} else {
			logEvents = append(logEvents, tmpLogEvents...)
		}
	}

	if len(logEvents) == 0 {
		return make([]data.Event, 0)
	}

	events := make([]data.Event, 0, len(logEvents))
	for _, event := range logEvents {
		if event == nil || check.IfNil(event.EventHandler) {
			continue
		}
		bech32Address, err := ei.pubKeyConverter.Encode(event.EventHandler.GetAddress())
		if err != nil {
			log.Error("eventsInterceptor: failed to decode event address", "error", err)
			continue
		}
		eventIdentifier := string(event.EventHandler.GetIdentifier())
		topics := event.EventHandler.GetTopics()
		if topics == nil {
			topics = make([][]byte, 0)
		}

		eventData := event.EventHandler.GetData()
		if eventData == nil {
			eventData = make([]byte, 0)
		}
		// Split the multi ESDTNFTTransfer in batches so they are emitted one by one
		if eventIdentifier == core.BuiltInFunctionMultiESDTNFTTransfer && len(topics) > 4 {
			topicsLen := len(topics)
			iterations := (topicsLen - 1) / 3
			receiver := topics[topicsLen-1]

			for i := 0; i < iterations; i++ {
				newTopics := make([][]byte, 4)
				newTopics[0] = topics[i*3]
				newTopics[1] = topics[1+i*3]
				newTopics[2] = topics[2+i*3]
				newTopics[3] = receiver

				events = append(events, data.Event{
					Address:        bech32Address,
					Identifier:     eventIdentifier,
					Topics:         newTopics,
					Data:           eventData,
					TxHash:         event.TxHash,
					OriginalTxHash: event.OriginalTxHash,
				})
			}
		} else {
			events = append(events, data.Event{
				Address:        bech32Address,
				Identifier:     eventIdentifier,
				Topics:         topics,
				Data:           eventData,
				TxHash:         event.TxHash,
				OriginalTxHash: event.OriginalTxHash,
			})
		}
	}

	return events
}

// IsInterfaceNil returns whether the interface is nil
func (ei *eventsInterceptor) IsInterfaceNil() bool {
	return ei == nil
}
