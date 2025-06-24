package servicebus

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	logger "github.com/multiversx/mx-chain-logger-go"
	"github.com/multiversx/mx-chain-notifier-go/config"
)

var clientLog = logger.GetOrCreate("servicebus-client")

const (
	// Retry configuration constants
	reconnectRetryMs   = 500
	maxRetryAttempts   = 10
	initialBackoffMs   = 100
	maxBackoffMs       = 30000
	backoffMultiplier  = 2.0
	deliveryTimeoutSec = 30

	// Operational constants
	minBatchSize = 1
	maxBatchSize = 256 // Azure Service Bus limit
)

type serviceBusClient struct {
	url          string
	publishMutex sync.Mutex

	client *azservicebus.Client
}

// NewServiceBusClient creates a new Azure Service Bus client instance
func NewServiceBusClient(url string) (*serviceBusClient, error) {
	sb := &serviceBusClient{
		url:          url,
		publishMutex: sync.Mutex{},
	}

	err := sb.connect()
	if err != nil {
		return nil, err
	}

	return sb, nil
}

// Publish publishes a batch of messages to the specified Service Bus topic
func (sb *serviceBusClient) Publish(exchangeConfig config.ServiceBusExchangeConfig, cfg config.AzureServiceBusConfig, messages []*azservicebus.Message) error {
	if !exchangeConfig.Enabled {
		clientLog.Debug("exchange disabled, skipping publish", "topic", exchangeConfig.Topic)
		return nil
	}

	if len(messages) == 0 {
		clientLog.Debug("no messages to publish", "topic", exchangeConfig.Topic)
		return nil
	}

	if len(messages) > maxBatchSize {
		clientLog.Warn("batch size exceeds maximum limit, truncating", "requestedSize", len(messages), "maxSize", maxBatchSize, "topic", exchangeConfig.Topic)
		messages = messages[:maxBatchSize]
	}

	sb.publishMutex.Lock()
	defer sb.publishMutex.Unlock()

	sender, err := sb.createSender(exchangeConfig.Topic)
	if err != nil {
		return err
	}
	defer sb.closeSender(sender, exchangeConfig.Topic)

	currentMessageBatch, err := sender.NewMessageBatch(context.Background(), nil)
	if err != nil {
		clientLog.Error("failed to create message batch", "topic", exchangeConfig.Topic, "err", err.Error())
		return err
	}

	for i := 0; i < len(messages); i++ {
		msg := messages[i]
		err = currentMessageBatch.AddMessage(msg, nil)

		if errors.Is(err, azservicebus.ErrMessageTooLarge) {
			if currentMessageBatch.NumMessages() == 0 {
				clientLog.Error("message too large for batch", "topic", exchangeConfig.Topic)
				return err
			}

			clientLog.Debug("batch full, sending and creating new batch", "messageCount", currentMessageBatch.NumMessages(), "topic", exchangeConfig.Topic)

			// send what we have since the batch is full with retry logic
			if sendErr := sb.sendWithRetry(sender, currentMessageBatch); sendErr != nil {
				clientLog.Error("Error sending the batch of messages after retries", "err", sendErr)
				return sendErr
			}

			// Create a new batch and retry adding this message to our batch.
			newBatch, err := sender.NewMessageBatch(context.Background(), nil)

			if err != nil {
				clientLog.Error("Error creating a new batch of messages", err)
				return err
			}

			currentMessageBatch = newBatch

			// rewind the counter and attempt to add the message again (this batch
			// was full so it didn't go out with the previous SendMessageBatch call).
			i--
		} else if err != nil {
			clientLog.Error("Error adding message to batch", "count", currentMessageBatch.NumMessages(), "err", err.Error())
			return err
		}
	}

	// check if any messages are remaining to be sent.
	if currentMessageBatch.NumMessages() > 0 {
		if sendErr := sb.sendWithRetry(sender, currentMessageBatch); sendErr != nil {
			clientLog.Error("Error send remaining messages in batch after retries", "err", sendErr)
			return sendErr
		}
	}

	return nil
}

func (sb *serviceBusClient) connect() error {
	client, err := azservicebus.NewClientFromConnectionString(sb.url, nil)
	if err != nil {
		return err
	}
	sb.client = client
	return nil
}

// createSender creates a new sender for the specified topic
func (sb *serviceBusClient) createSender(topic string) (*azservicebus.Sender, error) {
	sender, err := sb.client.NewSender(topic, nil)
	if err != nil {
		clientLog.Error("failed to create service bus sender", "topic", topic, "err", err.Error())
		return nil, err
	}
	return sender, nil
}

// closeSender safely closes the sender with error logging
func (sb *serviceBusClient) closeSender(sender *azservicebus.Sender, topic string) {
	if err := sender.Close(context.Background()); err != nil {
		clientLog.Warn("failed to close sender", "topic", topic, "err", err.Error())
	}
}

// sendWithRetry sends a message batch with exponential backoff retry logic
func (sb *serviceBusClient) sendWithRetry(sender *azservicebus.Sender, batch *azservicebus.MessageBatch) error {
	var lastErr error
	backoffMs := initialBackoffMs

	for attempt := 1; attempt <= maxRetryAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*deliveryTimeoutSec)
		err := sender.SendMessageBatch(ctx, batch, nil)
		cancel()

		if err == nil {
			if attempt > 1 {
				clientLog.Debug("message batch sent successfully after retry", "attempt", attempt)
			}
			return nil
		}

		lastErr = err
		clientLog.Warn("failed to send message batch", "attempt", attempt, "maxAttempts", maxRetryAttempts, "err", err.Error())

		if attempt < maxRetryAttempts {
			sleepDuration := time.Duration(backoffMs) * time.Millisecond
			clientLog.Debug("retrying after backoff", "sleepMs", backoffMs, "nextAttempt", attempt+1)
			time.Sleep(sleepDuration)

			// Exponential backoff with jitter
			backoffMs = int(math.Min(float64(backoffMs)*backoffMultiplier, float64(maxBackoffMs)))
		}
	}

	clientLog.Error("exhausted all retry attempts for message batch", "attempts", maxRetryAttempts, "lastErr", lastErr.Error())
	return lastErr
}

// Reconnect will try to reconnect to Service Bus with exponential backoff
func (sb *serviceBusClient) Reconnect() {
	backoffMs := initialBackoffMs
	attempt := 1

	for {
		time.Sleep(time.Duration(backoffMs) * time.Millisecond)

		err := sb.connect()
		if err != nil {
			clientLog.Debug("could not reconnect", "attempt", attempt, "err", err.Error())
			// Exponential backoff for reconnection
			backoffMs = int(math.Min(float64(backoffMs)*backoffMultiplier, float64(maxBackoffMs)))
			attempt++
		} else {
			clientLog.Debug("connection established after reconnect attempts", "attempts", attempt)
			break
		}
	}
}

// Close closes the Azure Service Bus client connection
func (sb *serviceBusClient) Close() {
	err := sb.client.Close(context.Background())
	if err != nil {
		clientLog.Error("failed to close servicebus client", "err", err.Error())
	}
}

// IsInterfaceNil returns true if there is no value under the interface
func (sb *serviceBusClient) IsInterfaceNil() bool {
	return sb == nil
}
