package servicebus

import (
	"encoding/hex"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/multiversx/mx-chain-core-go/core"
	"github.com/multiversx/mx-chain-core-go/core/check"
	"github.com/multiversx/mx-chain-core-go/marshal"
	logger "github.com/multiversx/mx-chain-logger-go"
	"github.com/multiversx/mx-chain-notifier-go/common"
	"github.com/multiversx/mx-chain-notifier-go/config"
	"github.com/multiversx/mx-chain-notifier-go/data"
)

var log = logger.GetOrCreate("servicebus-publisher")

// NFT/ESDT operation identifiers for optimization
var nftOperations = map[string]bool{
	core.BuiltInFunctionESDTNFTCreate:           true,
	core.BuiltInFunctionESDTNFTBurn:             true,
	core.BuiltInFunctionESDTNFTUpdateAttributes: true,
	core.BuiltInFunctionESDTNFTAddURI:           true,
	core.BuiltInFunctionESDTNFTAddQuantity:      true,
	core.BuiltInFunctionMultiESDTNFTTransfer:    true,
	core.BuiltInFunctionESDTNFTTransfer:         true,
	core.BuiltInFunctionESDTSetLimitedTransfer:  true,
	"registerAndSetAllRoles":                    true,
	"registerMetaESDT":                          true,
	"ESDTUnSetRole":                             true,
	"ESDTSetRole":                               true,
	"issueNonFungible":                          true,
	"issueSemiFungible":                         true,
	"ESDTTransferRoleAddAddress":                true,
	"ESDTTransferRoleDeleteAddress":             true,
	core.BuiltInFunctionESDTTransfer:            true,
}

// Execution event identifiers to skip
var executionEvents = map[string]bool{
	core.WriteLogIdentifier:         true,
	core.SignalErrorOperation:       true,
	core.InternalVMErrorsOperation:  true,
	core.CompletedTxEventIdentifier: true,
}

// ArgsServiceBusPublisher defines the arguments needed for Service Bus publisher creation
type ArgsServiceBusPublisher struct {
	Client     ServiceBusClient
	Config     config.AzureServiceBusConfig
	Marshaller marshal.Marshalizer
}

type serviceBusPublisher struct {
	client     ServiceBusClient
	marshaller marshal.Marshalizer
	cfg        config.AzureServiceBusConfig
}

// NewServiceBusPublisher creates a new Azure Service Bus publisher instance
func NewServiceBusPublisher(args ArgsServiceBusPublisher) (*serviceBusPublisher, error) {
	err := checkArgs(args)
	if err != nil {
		return nil, err
	}

	sb := &serviceBusPublisher{
		cfg:        args.Config,
		client:     args.Client,
		marshaller: args.Marshaller,
	}

	return sb, nil
}

func checkArgs(args ArgsServiceBusPublisher) error {
	if check.IfNil(args.Client) {
		return ErrNilServiceBusClient
	}
	if check.IfNil(args.Marshaller) {
		return common.ErrNilMarshaller
	}

	if args.Config.EventsExchange.Topic == "" {
		return ErrInvalidServiceBusExchangeName
	}
	if args.Config.RevertEventsExchange.Topic == "" {
		return ErrInvalidServiceBusExchangeName
	}
	if args.Config.FinalizedEventsExchange.Topic == "" {
		return ErrInvalidServiceBusExchangeName
	}
	if args.Config.BlockTxsExchange.Topic == "" {
		return ErrInvalidServiceBusExchangeName
	}
	if args.Config.BlockScrsExchange.Topic == "" {
		return ErrInvalidServiceBusExchangeName
	}
	if args.Config.BlockEventsExchange.Topic == "" {
		return ErrInvalidServiceBusExchangeName
	}

	return nil
}

// Publish processes and publishes block events to Azure Service Bus
func (sb *serviceBusPublisher) Publish(events data.BlockEvents) {
	if len(events.Events) == 0 {
		log.Debug("no events to publish")
		return
	}

	messages := make([]*azservicebus.Message, 0, len(events.Events))

	for _, event := range events.Events {
		// Skip execution logs if configured
		if sb.cfg.SkipExecutionEventLogs && executionEvents[event.Identifier] {
			continue
		}

		msg, err := sb.createMessageFromEvent(event)
		if err != nil {
			log.Error("failed to create message from event", "address", event.Address, "identifier", event.Identifier, "err", err)
			return
		}

		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		log.Debug("no messages to publish after filtering")
		return
	}

	err := sb.publishFanout(sb.cfg.EventsExchange, messages)
	if err != nil {
		log.Error("failed to publish events to servicebus", "messageCount", len(messages), "exchange", sb.cfg.EventsExchange.Topic, "err", err.Error())
	}
}

// createMessageFromEvent creates a Service Bus message from a blockchain event
func (sb *serviceBusPublisher) createMessageFromEvent(event data.Event) (*azservicebus.Message, error) {
	sessionId := event.Address
	isNFT := "true"

	// Handle NFT/ESDT operations
	if nftOperations[event.Identifier] {
		if len(event.Topics) >= 2 {
			hexStr := hex.EncodeToString(event.Topics[1])
			if hexStr == "" {
				isNFT = "false"
			}
			sessionId = string(event.Topics[0])
		}
	}

	payload, err := sb.marshaller.Marshal(event)
	if err != nil {
		return nil, err
	}

	msg := &azservicebus.Message{
		Body:                  payload,
		SessionID:             &sessionId,
		ApplicationProperties: make(map[string]interface{}),
	}

	// Set application properties for better observability
	msg.ApplicationProperties["Address"] = event.Address
	msg.ApplicationProperties["Identifier"] = event.Identifier
	msg.ApplicationProperties["Hash"] = event.TxHash
	msg.ApplicationProperties["OriginalTxHash"] = event.OriginalTxHash

	if event.Identifier == core.BuiltInFunctionMultiESDTNFTTransfer {
		msg.ApplicationProperties["isNFT"] = isNFT
	}

	return msg, nil
}

func (sb *serviceBusPublisher) PublishRevert(revertBlock data.RevertBlock) {
	revertBlockBytes, err := sb.marshaller.Marshal(revertBlock)
	if err != nil {
		log.Error("failed to marshal revert event", "hash", revertBlock.Hash, "err", err.Error())
		return
	}
	messages := make([]*azservicebus.Message, 0)

	msg := &azservicebus.Message{
		Body:                  revertBlockBytes,
		SessionID:             &revertBlock.Hash,
		ApplicationProperties: make(map[string]interface{})}

	msg.ApplicationProperties["Hash"] = revertBlock.Hash
	messages = append(messages, msg)

	err = sb.publishFanout(sb.cfg.RevertEventsExchange, messages)
	if err != nil {
		log.Error("failed to publish revert event to servicebus", "hash", revertBlock.Hash, "exchange", sb.cfg.RevertEventsExchange.Topic, "err", err.Error())
	}
}

func (sb *serviceBusPublisher) PublishFinalized(finalizedBlock data.FinalizedBlock) {
	finalizedBlockBytes, err := sb.marshaller.Marshal(finalizedBlock)
	if err != nil {
		log.Error("failed to marshal finalized event", "hash", finalizedBlock.Hash, "err", err.Error())
		return
	}
	messages := make([]*azservicebus.Message, 0)

	msg := &azservicebus.Message{
		Body:                  finalizedBlockBytes,
		SessionID:             &finalizedBlock.Hash,
		ApplicationProperties: make(map[string]interface{})}

	msg.ApplicationProperties["Hash"] = finalizedBlock.Hash
	messages = append(messages, msg)

	err = sb.publishFanout(sb.cfg.FinalizedEventsExchange, messages)
	if err != nil {
		log.Error("failed to publish finalized event to servicebus", "hash", finalizedBlock.Hash, "exchange", sb.cfg.FinalizedEventsExchange.Topic, "err", err.Error())
	}
}

func (sb *serviceBusPublisher) PublishTxs(blockTxs data.BlockTxs) {
	messages := make([]*azservicebus.Message, 0)

	for _, tx := range blockTxs.Txs {
		event, err := sb.marshaller.Marshal(tx)
		if err != nil {
			log.Error("failed to marshal transaction event", "hash", blockTxs.Hash, "err", err.Error())
			return
		}
		msg := &azservicebus.Message{
			Body:                  event,
			SessionID:             &blockTxs.Hash,
			ApplicationProperties: make(map[string]interface{})}

		msg.ApplicationProperties["Hash"] = blockTxs.Hash
		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		log.Debug("no transaction messages to publish")
		return
	}

	err := sb.publishFanout(sb.cfg.BlockTxsExchange, messages)
	if err != nil {
		log.Error("failed to publish block txs event to servicebus", "hash", blockTxs.Hash, "messageCount", len(messages), "exchange", sb.cfg.BlockTxsExchange.Topic, "err", err.Error())
	}
}

func (sb *serviceBusPublisher) PublishAlteredAccounts(accounts data.AlteredAccountsEvent) {
	messages := make([]*azservicebus.Message, 0)

	for _, account := range accounts.Accounts {
		event, err := sb.marshaller.Marshal(account)
		if err != nil {
			log.Error("failed to marshal altered account event", "address", account.Address, "err", err.Error())
			return
		}
		msg := &azservicebus.Message{
			Body:                  event,
			SessionID:             &account.Address,
			ApplicationProperties: make(map[string]interface{})}

		msg.ApplicationProperties["Address"] = account.Address
		msg.ApplicationProperties["Hash"] = accounts.Hash
		messages = append(messages, msg)
	}
	if len(messages) == 0 {
		log.Debug("no altered account messages to publish")
		return
	}

	err := sb.publishFanout(sb.cfg.AlteredAccountsExchange, messages)
	if err != nil {
		log.Error("failed to publish altered accounts event to servicebus", "hash", accounts.Hash, "messageCount", len(messages), "exchange", sb.cfg.AlteredAccountsExchange.Topic, "err", err.Error())
	}
}

func (sb *serviceBusPublisher) PublishScrs(blockScrs data.BlockScrs) {
	messages := make([]*azservicebus.Message, 0)

	for _, scr := range blockScrs.Scrs {
		event, err := sb.marshaller.Marshal(scr)
		if err != nil {
			log.Error("failed to marshal smart contract result event", "hash", blockScrs.Hash, "err", err.Error())
			return
		}
		msg := &azservicebus.Message{
			Body:                  event,
			SessionID:             &blockScrs.Hash,
			ApplicationProperties: make(map[string]interface{})}

		msg.ApplicationProperties["BlockHash"] = blockScrs.Hash
		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		log.Debug("no smart contract result messages to publish")
		return
	}

	err := sb.publishFanout(sb.cfg.BlockScrsExchange, messages)
	if err != nil {
		log.Error("failed to publish block scrs event to servicebus", "hash", blockScrs.Hash, "messageCount", len(messages), "exchange", sb.cfg.BlockScrsExchange.Topic, "err", err.Error())
	}
}

func (sb *serviceBusPublisher) PublishBlockEventsWithOrder(blockTxs data.BlockEventsWithOrder) {
	txsBlockBytes, err := sb.marshaller.Marshal(blockTxs)
	if err != nil {
		log.Error("failed to marshal block events with order", "hash", blockTxs.Hash, "err", err.Error())
		return
	}

	messages := make([]*azservicebus.Message, 0)

	msg := &azservicebus.Message{
		Body:                  txsBlockBytes,
		SessionID:             &blockTxs.Hash,
		ApplicationProperties: make(map[string]interface{})}

	msg.ApplicationProperties["Hash"] = blockTxs.Hash
	messages = append(messages, msg)

	err = sb.publishFanout(sb.cfg.BlockEventsExchange, messages)
	if err != nil {
		log.Error("failed to publish full block events to servicebus", "hash", blockTxs.Hash, "exchange", sb.cfg.BlockEventsExchange.Topic, "err", err.Error())
	}
}

func (sb *serviceBusPublisher) publishFanout(exchangeConfig config.ServiceBusExchangeConfig, payload []*azservicebus.Message) error {
	return sb.client.Publish(exchangeConfig, sb.cfg, payload)
}

// Close closes the Azure Service Bus publisher and client
func (sb *serviceBusPublisher) Close() error {
	sb.client.Close()
	return nil
}

// IsInterfaceNil returns true if there is no value under the interface
func (sb *serviceBusPublisher) IsInterfaceNil() bool {
	return sb == nil
}
