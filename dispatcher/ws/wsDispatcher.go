package ws

import (
	"bytes"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/multiversx/mx-chain-core-go/core/check"
	"github.com/multiversx/mx-chain-core-go/marshal"
	logger "github.com/multiversx/mx-chain-logger-go"
	"github.com/multiversx/mx-chain-notifier-go/common"
	"github.com/multiversx/mx-chain-notifier-go/data"
	"github.com/multiversx/mx-chain-notifier-go/dispatcher"
)

var log = logger.GetOrCreate("websocket")

// Object pools for frequently allocated structures to reduce GC pressure
var (
	wsMessagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 4096) // Pre-allocate 4KB capacity
		},
	}
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
	maxMsgSize = 1024 * 1024
	// raised from 256 for throughput; trySend drops a subscriber once this fills,
	// so this only widens the burst a slow-but-alive subscriber may absorb
	sendChanBuffSize = 10000
)

var (
	newline = []byte{'\n'}
	space   = []byte{' '}
)

// argsWebSocketDispatcher defines the arguments needed for ws dispatcher
type argsWebSocketDispatcher struct {
	Dispatcher dispatcher.Dispatcher
	Conn       dispatcher.WSConnection
	Marshaller marshal.Marshalizer
}

type websocketDispatcher struct {
	id         uuid.UUID
	wg         sync.WaitGroup
	send       chan []byte
	mutSend    sync.RWMutex
	sendClosed bool
	conn       dispatcher.WSConnection
	dispatcher dispatcher.Dispatcher
	marshaller marshal.Marshalizer
}

// newWebSocketDispatcher createa a new ws dispatcher instance
func newWebSocketDispatcher(args argsWebSocketDispatcher) (*websocketDispatcher, error) {
	if check.IfNil(args.Dispatcher) {
		return nil, ErrNilDispatcher
	}
	if args.Conn == nil {
		return nil, ErrNilWSConn
	}
	if check.IfNil(args.Marshaller) {
		return nil, common.ErrNilMarshaller
	}

	return &websocketDispatcher{
		id:         uuid.New(),
		send:       make(chan []byte, sendChanBuffSize),
		conn:       args.Conn,
		dispatcher: args.Dispatcher,
		marshaller: args.Marshaller,
	}, nil
}

// GetID returns the id corresponding to this dispatcher instance
func (wd *websocketDispatcher) GetID() uuid.UUID {
	return wd.id
}

// trySend attempts a non-blocking send on the dispatcher's send channel.
// If the channel's buffer is full - meaning the subscriber isn't reading fast
// enough, or at all - the connection is closed instead of blocking, so that
// callers (the hub's publish path) never stall waiting on a stuck subscriber.
func (wd *websocketDispatcher) trySend(payload []byte) {
	isBufferFull := wd.sendPayload(payload)
	if !isBufferFull {
		return
	}

	log.Warn("dispatcher send buffer full, dropping subscriber", "dispatcherID", wd.id)
	if err := wd.conn.Close(); err != nil {
		log.Debug("failed to close socket after full send buffer", "err", err.Error())
	}
}

func (wd *websocketDispatcher) sendPayload(payload []byte) bool {
	wd.mutSend.RLock()
	defer wd.mutSend.RUnlock()

	if wd.sendClosed {
		return false
	}

	select {
	case wd.send <- payload:
		return false
	default:
		return true
	}
}

// closeSend closes the send channel exactly once, synchronized against
// trySend so no goroutine can send on an already-closed channel.
func (wd *websocketDispatcher) closeSend() {
	wd.mutSend.Lock()
	defer wd.mutSend.Unlock()

	if wd.sendClosed {
		return
	}
	wd.sendClosed = true
	close(wd.send)
}

// PushEvents receives an events slice and processes it before pushing to socket
func (wd *websocketDispatcher) PushEvents(events []data.Event) {
	// Direct marshaling optimization - eliminates double marshaling
	directWSMessage := struct {
		Type string       `json:"type"`
		Data []data.Event `json:"data"`
	}{
		Type: common.PushLogsAndEvents,
		Data: events,
	}

	wsEventBytes, err := wd.marshaller.Marshal(directWSMessage)
	if err != nil {
		log.Error("failure marshalling events", "err", err.Error())
		return
	}

	wd.trySend(wsEventBytes)
}

// RevertEvent receives a reverted block event and process it before pushing to socket
func (wd *websocketDispatcher) RevertEvent(event data.RevertBlock) {
	// Direct marshaling optimization - eliminates double marshaling
	directWSMessage := struct {
		Type string           `json:"type"`
		Data data.RevertBlock `json:"data"`
	}{
		Type: common.RevertBlockEvents,
		Data: event,
	}

	wsEventBytes, err := wd.marshaller.Marshal(directWSMessage)
	if err != nil {
		log.Error("failure marshalling revert event", "err", err.Error())
		return
	}

	wd.trySend(wsEventBytes)
}

// FinalizedEvent receives a finalized block event and process it before pushing to socket
func (wd *websocketDispatcher) FinalizedEvent(event data.FinalizedBlock) {
	// Direct marshaling optimization - eliminates double marshaling
	directWSMessage := struct {
		Type string              `json:"type"`
		Data data.FinalizedBlock `json:"data"`
	}{
		Type: common.FinalizedBlockEvents,
		Data: event,
	}

	wsEventBytes, err := wd.marshaller.Marshal(directWSMessage)
	if err != nil {
		log.Error("failure marshalling finalized event", "err", err.Error())
		return
	}

	wd.trySend(wsEventBytes)
}

// TxsEvent receives a block txs event and process it before pushing to socket
func (wd *websocketDispatcher) TxsEvent(event data.BlockTxs) {
	// Direct marshaling optimization - eliminates double marshaling
	directWSMessage := struct {
		Type string        `json:"type"`
		Data data.BlockTxs `json:"data"`
	}{
		Type: common.BlockTxs,
		Data: event,
	}

	wsEventBytes, err := wd.marshaller.Marshal(directWSMessage)
	if err != nil {
		log.Error("failure marshalling txs event", "err", err.Error())
		return
	}

	wd.trySend(wsEventBytes)
}

// BlockEvents receives block events with data and processes it before pushing to socket
func (wd *websocketDispatcher) BlockEvents(event data.BlockEventsWithOrder) {
	// Direct marshaling optimization - eliminates double marshaling
	directWSMessage := struct {
		Type string                    `json:"type"`
		Data data.BlockEventsWithOrder `json:"data"`
	}{
		Type: common.BlockEvents,
		Data: event,
	}

	wsEventBytes, err := wd.marshaller.Marshal(directWSMessage)
	if err != nil {
		log.Error("failure marshalling block events", "err", err.Error())
		return
	}

	wd.trySend(wsEventBytes)
}

// ScrsEvent receives a block scrs event and process it before pushing to socket
func (wd *websocketDispatcher) ScrsEvent(event data.BlockScrs) {
	// Direct marshaling optimization - eliminates double marshaling
	directWSMessage := struct {
		Type string         `json:"type"`
		Data data.BlockScrs `json:"data"`
	}{
		Type: common.BlockScrs,
		Data: event,
	}

	wsEventBytes, err := wd.marshaller.Marshal(directWSMessage)
	if err != nil {
		log.Error("failure marshalling scrs event", "err", err.Error())
		return
	}

	wd.trySend(wsEventBytes)
}

// AlteredAccounts receives a block scrs event and process it before pushing to socket
func (wd *websocketDispatcher) AlteredAccounts(event data.AlteredAccountsEvent) {
	// Direct marshaling optimization - eliminates double marshaling
	directWSMessage := struct {
		Type string                    `json:"type"`
		Data data.AlteredAccountsEvent `json:"data"`
	}{
		Type: common.AlteredAccountsEvent,
		Data: event,
	}

	wsEventBytes, err := wd.marshaller.Marshal(directWSMessage)
	if err != nil {
		log.Error("failure marshalling altered accounts event", "err", err.Error())
		return
	}

	wd.trySend(wsEventBytes)
}

// StateAccessesEvent receives a block state accesses event and process it before pushing to socket
func (wd *websocketDispatcher) StateAccessesEvent(event data.BlockStateAccesses) {
	eventBytes, err := wd.marshaller.Marshal(event)
	if err != nil {
		log.Error("failure marshalling events", "err", err.Error())
		return
	}
	wsEvent := &data.WebSocketEvent{
		Type: common.BlockStateAccesses,
		Data: eventBytes,
	}
	wsEventBytes, err := wd.marshaller.Marshal(wsEvent)
	if err != nil {
		log.Error("failure marshalling events", "err", err.Error())
		return
	}

	wd.trySend(wsEventBytes)
}

// writePump listens on the send-channel and pushes data on the socket stream
func (wd *websocketDispatcher) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		if err := wd.conn.Close(); err != nil {
			if _, ok := err.(*net.OpError); ok {
				log.Debug("close attempt on closed connection", "err", err.Error())
				return
			}

			log.Error("failed to close socket", "err", err.Error())
		}
	}()

	nextWriterWrap := func(msgType int, data []byte) error {
		writer, err := wd.conn.NextWriter(msgType)
		if err != nil {
			return err
		}
		_, err = writer.Write(data)
		if err != nil {
			return err
		}
		return writer.Close()
	}

	for {
		select {
		case message, ok := <-wd.send:
			if err := wd.setSocketWriteLimits(); err != nil {
				log.Error("channel: failed to set socket write limits", "err", err.Error())
				return
			}

			if !ok {
				if err := wd.conn.WriteMessage(websocket.CloseMessage, []byte{}); err != nil {
					log.Debug("failed to write close message", "err", err.Error())
					return
				}
			}

			if err := nextWriterWrap(websocket.TextMessage, message); err != nil {
				log.Error("failed to write text message", "err", err.Error())
				return
			}
		case <-ticker.C:
			if err := wd.setSocketWriteLimits(); err != nil {
				log.Error("ticker: failed to set socket write limits", "err", err.Error())
			}
			if err := wd.conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				log.Error("ticker: failed to write ping message", "err", err.Error())
				return
			}
		}
	}
}

// readPump listens for incoming events and reads the content from the socket stream
func (wd *websocketDispatcher) readPump() {
	defer func() {
		wd.dispatcher.UnregisterEvent(wd)
		if err := wd.conn.Close(); err != nil {
			log.Error("failed to close socket on defer", "err", err.Error())
		}
		wd.closeSend()
	}()

	if err := wd.setSocketReadLimits(); err != nil {
		log.Error("failed to set socket read limits", "err", err.Error())
	}

	for {
		_, msg, innerErr := wd.conn.ReadMessage()
		if innerErr != nil {
			log.Debug("failed reading socket", "err", innerErr.Error())

			if websocket.IsUnexpectedCloseError(
				innerErr,
				websocket.CloseGoingAway,
				websocket.CloseAbnormalClosure,
			) {
				log.Debug("received unexpected socket close", "err", innerErr.Error())
			}
			break
		}

		msg = bytes.TrimSpace(bytes.Replace(msg, newline, space, -1))
		wd.trySendSubscribeEvent(msg)
	}
}

func (wd *websocketDispatcher) trySendSubscribeEvent(eventBytes []byte) {
	var subscribeEvent data.SubscribeEvent
	err := wd.marshaller.Unmarshal(&subscribeEvent, eventBytes)
	if err != nil {
		log.Error("failure unmarshalling subscribe event", "err", err.Error())
		return
	}
	subscribeEvent.DispatcherID = wd.id
	wd.dispatcher.Subscribe(subscribeEvent)
}

func (wd *websocketDispatcher) setSocketWriteLimits() error {
	if err := wd.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
		return err
	}
	return nil
}

func (wd *websocketDispatcher) setSocketReadLimits() error {
	wd.conn.SetReadLimit(maxMsgSize)
	if err := wd.conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
		return err
	}
	wd.conn.SetPongHandler(func(string) error {
		return wd.conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	return nil
}
