package process

import (
	"context"
	"encoding/hex"
	"encoding/json"

	"github.com/multiversx/mx-chain-core-go/core"
	"github.com/multiversx/mx-chain-core-go/core/check"
	nodeData "github.com/multiversx/mx-chain-core-go/data"
	"github.com/multiversx/mx-chain-core-go/data/alteredAccount"
	"github.com/multiversx/mx-chain-core-go/data/outport"
	"github.com/multiversx/mx-chain-core-go/data/smartContractResult"
	"github.com/multiversx/mx-chain-core-go/data/transaction"
	"github.com/multiversx/mx-chain-notifier-go/data"
)

// logEvent defines a log event associated with corresponding tx hash
type logEvent struct {
	EventHandler   nodeData.EventHandler
	TxHash         string
	OriginalTxHash string
}

// ArgsEventsInterceptor defines the arguments needed for creating an events interceptor instance
type ArgsEventsInterceptor struct {
	PubKeyConverter core.PubkeyConverter
	LockService     LockService
}

type eventsInterceptor struct {
	pubKeyConverter core.PubkeyConverter
	locker          LockService
}

// NewEventsInterceptor creates a new eventsInterceptor instance
func NewEventsInterceptor(args ArgsEventsInterceptor) (*eventsInterceptor, error) {
	if check.IfNil(args.PubKeyConverter) {
		return nil, ErrNilPubKeyConverter
	}

	return &eventsInterceptor{
		pubKeyConverter: args.PubKeyConverter,
		locker:          args.LockService,
	}, nil
}

// ProcessBlockEvents will process block events data
func (ei *eventsInterceptor) ProcessBlockEvents(eventsData *data.ArgsSaveBlockData) (*data.InterceptorBlockData, error) {
	if eventsData == nil {
		return nil, ErrNilBlockEvents
	}
	if eventsData.TransactionsPool == nil {
		return nil, ErrNilTransactionsPool
	}
	if eventsData.Body == nil {
		return nil, ErrNilBlockBody
	}
	if eventsData.Header == nil {
		return nil, ErrNilBlockHeader
	}
	// Map each hash with the SC result, before processing the events so we can skip cross shard duplicate transfers by checking the hash
	scrs := make(map[string]*smartContractResult.SmartContractResult)
	for hash, scr := range eventsData.TransactionsPool.SmartContractResults {
		scrs[hash] = scr.SmartContractResult
	}
	scrsWithOrder := eventsData.TransactionsPool.SmartContractResults

	events := ei.getLogEventsFromTransactionsPool(eventsData.TransactionsPool.Logs, scrs)

	txs := make(map[string]*transaction.Transaction)
	for hash, tx := range eventsData.TransactionsPool.Transactions {
		txs[hash] = tx.Transaction
	}
	txsWithOrder := eventsData.TransactionsPool.Transactions

	// Output as well the Altered Accounts
	accounts := make([]*alteredAccount.AlteredAccount, 0)
	for _, account := range eventsData.AlteredAccounts {
		accounts = append(accounts, account)
	}
	return &data.InterceptorBlockData{
		Hash:            hex.EncodeToString(eventsData.HeaderHash),
		Body:            eventsData.Body,
		Header:          eventsData.Header,
		Txs:             txs,
		TxsWithOrder:    txsWithOrder,
		Scrs:            scrs,
		ScrsWithOrder:   scrsWithOrder,
		LogEvents:       events,
		AlteredAccounts: accounts,
	}, nil
}
func (ei *eventsInterceptor) getLogEventsFromTransactionsPool(logs []*outport.LogData, scrs map[string]*smartContractResult.SmartContractResult) []data.Event {
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
			originalTxHash := logData.GetTxHash()
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
			if eventIdentifier == core.BuiltInFunctionMultiESDTNFTTransfer || eventIdentifier == core.BuiltInFunctionESDTNFTTransfer || eventIdentifier == core.BuiltInFunctionESDTTransfer {
				event := data.EventDuplicateCheck{
					Address:    event.Address,
					Identifier: event.Identifier,
					Topics:     event.Topics,
				}
				jsonData, err := json.Marshal(event)
				if err != nil {
					log.Error("could not marshal event", "err", err.Error())
					return nil
				}
				hexData := hex.EncodeToString(jsonData)
				_, isThere := duplicateTwiceSameBlock[originalTxHash + hexData]
				if !isThere {
					skipEvent, err := ei.locker.IsCrossShardConfirmation(context.Background(), originalTxHash, event)
					// Save this as already seen in this logs block 
					duplicateTwiceSameBlock[originalTxHash + hexData] = true
					if err != nil {
						log.Info("eventsInterceptor: failed to check cross shard confirmation", "error", err)
						continue
					}
					if skipEvent {
						log.Info("eventsInterceptor: skip cross shard confirmation event", "txHash", logData.TxHash, "originalTxHash", originalTxHash, "eventIdentifier", eventIdentifier)
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
			var filteredItems []*logEvent
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
		return nil
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

		// Split the multi ESDTNFTTransfer in batches so their are emmited one by one (works best for processing them separate in case of failures on one of them)
		if eventIdentifier == core.BuiltInFunctionMultiESDTNFTTransfer && len(topics) > 4 {
			topicsLen := len(topics)
			iterations := (topicsLen - 1) / 3
			receiver := topics[topicsLen-1]

			for i := 0; i < iterations; i++ {
				newTopics := make([][]byte, 0, 4)
				// identifier
				newTopics = append(newTopics, topics[i*3])
				// nonce
				newTopics = append(newTopics, topics[1+i*3])
				// amount
				newTopics = append(newTopics, topics[2+i*3])
				// receiver
				newTopics = append(newTopics, receiver)

				events = append(events, data.Event{
					Address:        bech32Address,
					Identifier:     eventIdentifier,
					Topics:         newTopics,
					Data:           event.EventHandler.GetData(),
					TxHash:         event.TxHash,
					OriginalTxHash: event.OriginalTxHash,
				})
			}
		} else {
			events = append(events, data.Event{
				Address:        bech32Address,
				Identifier:     eventIdentifier,
				Topics:         topics,
				Data:           event.EventHandler.GetData(),
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
