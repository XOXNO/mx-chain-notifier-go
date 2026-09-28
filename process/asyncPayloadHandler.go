package process

import (
	"errors"
	"sync"
	"time"

	"github.com/multiversx/mx-chain-communication-go/websocket"
	"github.com/multiversx/mx-chain-core-go/core/check"
)

const (
	defaultAsyncAckQueueSize  = 1024
	defaultAsyncAckMaxRetries = 5
	asyncAckInitialBackoff    = 200 * time.Millisecond
	asyncAckMaxBackoff        = 5 * time.Second
	asyncAckDrainTimeout      = 30 * time.Second
	protoFieldOneVarintTag    = 0x08
)

// ErrAsyncPayloadHandlerClosed signals that the async payload handler no longer accepts payloads
var ErrAsyncPayloadHandlerClosed = errors.New("async payload handler is closed")

// ArgsAsyncPayloadHandler holds the arguments needed to create an asyncPayloadHandler
type ArgsAsyncPayloadHandler struct {
	Handler    websocket.PayloadHandler
	QueueSize  uint32
	MaxRetries uint32
	// ShardLanes should be true only for the protobuf marshaller, where ShardID is the first encoded field.
	// Otherwise, all payloads share a single lane.
	ShardLanes bool
}

type queuedPayload struct {
	payload []byte
	topic   string
	version uint32
}

// asyncPayloadHandler acknowledges a payload as soon as it is queued in a bounded, per-shard FIFO lane and
// processes it in the background, retrying failures. A full lane blocks the caller, so the ack is delayed and
// the observer is slowed down instead of the queue growing without bound.
// Payloads accepted but not yet processed are lost if the process dies (in-memory queue).
type asyncPayloadHandler struct {
	handler    websocket.PayloadHandler
	queueSize  int
	maxRetries int
	shardLanes bool

	// mutState is held for reading for the whole ProcessPayload call, so Close never races a sender
	mutState sync.RWMutex
	closed   bool
	mutLanes sync.Mutex
	lanes    map[uint32]chan queuedPayload
	workerWG sync.WaitGroup
}

// NewAsyncPayloadHandler creates a payload handler that decouples ack from processing
func NewAsyncPayloadHandler(args ArgsAsyncPayloadHandler) (*asyncPayloadHandler, error) {
	if check.IfNil(args.Handler) {
		return nil, ErrNilDataProcessor
	}

	queueSize := int(args.QueueSize)
	if queueSize == 0 {
		queueSize = defaultAsyncAckQueueSize
	}
	maxRetries := int(args.MaxRetries)
	if maxRetries == 0 {
		maxRetries = defaultAsyncAckMaxRetries
	}

	return &asyncPayloadHandler{
		handler:    args.Handler,
		queueSize:  queueSize,
		maxRetries: maxRetries,
		shardLanes: args.ShardLanes,
		lanes:      make(map[uint32]chan queuedPayload),
	}, nil
}

// ProcessPayload queues the payload and returns, letting the transport acknowledge it
func (h *asyncPayloadHandler) ProcessPayload(payload []byte, topic string, version uint32) error {
	h.mutState.RLock()
	defer h.mutState.RUnlock()

	if h.closed {
		return ErrAsyncPayloadHandlerClosed
	}

	h.getLane(h.laneKey(payload)) <- queuedPayload{payload: payload, topic: topic, version: version}

	return nil
}

func (h *asyncPayloadHandler) laneKey(payload []byte) uint32 {
	if !h.shardLanes {
		return 0
	}

	return leadingShardID(payload)
}

// leadingShardID reads protobuf field 1 (varint) when it is the first field. proto3 omits a zero value, so
// any other first byte means shard 0. OutportBlock, BlockData and FinalizedBlock all encode ShardID first.
func leadingShardID(payload []byte) uint32 {
	if len(payload) < 2 || payload[0] != protoFieldOneVarintTag {
		return 0
	}

	var value uint64
	for i, shift := 1, uint(0); i < len(payload) && i <= 5; i, shift = i+1, shift+7 {
		value |= uint64(payload[i]&0x7f) << shift
		if payload[i] < 0x80 {
			return uint32(value)
		}
	}

	return 0
}

func (h *asyncPayloadHandler) getLane(key uint32) chan queuedPayload {
	h.mutLanes.Lock()
	defer h.mutLanes.Unlock()

	lane, found := h.lanes[key]
	if found {
		return lane
	}

	lane = make(chan queuedPayload, h.queueSize)
	h.lanes[key] = lane
	h.workerWG.Add(1)
	go h.runLane(key, lane)

	return lane
}

func (h *asyncPayloadHandler) runLane(key uint32, lane chan queuedPayload) {
	defer h.workerWG.Done()

	for item := range lane {
		h.processWithRetries(key, item)
	}
}

func (h *asyncPayloadHandler) processWithRetries(key uint32, item queuedPayload) {
	backoff := asyncAckInitialBackoff
	var err error
	for attempt := 1; attempt <= h.maxRetries; attempt++ {
		err = h.handler.ProcessPayload(item.payload, item.topic, item.version)
		if err == nil {
			return
		}

		log.Warn("asyncPayloadHandler: processing failed",
			"lane", key, "topic", item.topic, "attempt", attempt, "max attempts", h.maxRetries, "error", err)
		if attempt == h.maxRetries {
			break
		}

		time.Sleep(backoff)
		backoff = min(backoff*2, asyncAckMaxBackoff)
	}

	// the observer was already acked, so it will not resend; keep the head of the lane from blocking forever
	log.Error("asyncPayloadHandler: dropping payload after exhausting retries",
		"lane", key, "topic", item.topic, "attempts", h.maxRetries, "error", err)
}

// Close stops accepting payloads, drains the queued ones (bounded by a timeout) and closes the inner handler
func (h *asyncPayloadHandler) Close() error {
	// waits for in-flight ProcessPayload calls, which finish as the running lanes keep draining
	h.mutState.Lock()
	if h.closed {
		h.mutState.Unlock()
		return nil
	}
	h.closed = true
	h.mutLanes.Lock()
	for _, lane := range h.lanes {
		close(lane)
	}
	h.mutLanes.Unlock()
	h.mutState.Unlock()

	drained := make(chan struct{})
	go func() {
		h.workerWG.Wait()
		close(drained)
	}()

	select {
	case <-drained:
	case <-time.After(asyncAckDrainTimeout):
		log.Error("asyncPayloadHandler: timed out draining queued payloads on close")
	}

	return h.handler.Close()
}

// IsInterfaceNil returns true if there is no value under the interface
func (h *asyncPayloadHandler) IsInterfaceNil() bool {
	return h == nil
}
