package process_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multiversx/mx-chain-core-go/data/outport"
	"github.com/multiversx/mx-chain-notifier-go/process"
	"github.com/stretchr/testify/require"
)

type payloadHandlerMock struct {
	processCalled func(payload []byte, topic string, version uint32) error
	closeCalled   atomic.Bool
}

func (m *payloadHandlerMock) ProcessPayload(payload []byte, topic string, version uint32) error {
	return m.processCalled(payload, topic, version)
}

func (m *payloadHandlerMock) Close() error {
	m.closeCalled.Store(true)
	return nil
}

func (m *payloadHandlerMock) IsInterfaceNil() bool { return m == nil }

func TestNewAsyncPayloadHandler_NilHandler(t *testing.T) {
	t.Parallel()

	h, err := process.NewAsyncPayloadHandler(process.ArgsAsyncPayloadHandler{})
	require.Nil(t, h)
	require.Equal(t, process.ErrNilDataProcessor, err)
}

func TestAsyncPayloadHandler_ReturnsBeforeProcessingFinishes(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	done := make(chan struct{})
	inner := &payloadHandlerMock{processCalled: func([]byte, string, uint32) error {
		<-release
		close(done)
		return nil
	}}
	h, _ := process.NewAsyncPayloadHandler(process.ArgsAsyncPayloadHandler{Handler: inner})

	start := time.Now()
	require.Nil(t, h.ProcessPayload([]byte("a"), "topic", 1))
	require.Less(t, time.Since(start), time.Second)

	close(release)
	<-done
	require.Nil(t, h.Close())
	require.True(t, inner.closeCalled.Load())
}

func TestAsyncPayloadHandler_KeepsOrderWithinAShard(t *testing.T) {
	t.Parallel()

	var mut sync.Mutex
	processed := make([]byte, 0)
	inner := &payloadHandlerMock{processCalled: func(p []byte, _ string, _ uint32) error {
		mut.Lock()
		processed = append(processed, p[len(p)-1])
		mut.Unlock()
		return nil
	}}
	h, _ := process.NewAsyncPayloadHandler(process.ArgsAsyncPayloadHandler{Handler: inner, ShardLanes: true})

	shard1 := func(i byte) []byte { return []byte{0x08, 0x01, i} }
	for i := byte(0); i < 50; i++ {
		require.Nil(t, h.ProcessPayload(shard1(i), "topic", 1))
	}
	require.Nil(t, h.Close())

	require.Len(t, processed, 50)
	for i := range processed {
		require.Equal(t, byte(i), processed[i])
	}
}

func TestAsyncPayloadHandler_ShardsDoNotBlockEachOther(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	fast := make(chan struct{})
	inner := &payloadHandlerMock{processCalled: func(p []byte, _ string, _ uint32) error {
		if p[1] == 0x01 { // shard 1 is stuck
			<-release
			return nil
		}
		close(fast)
		return nil
	}}
	h, _ := process.NewAsyncPayloadHandler(process.ArgsAsyncPayloadHandler{Handler: inner, ShardLanes: true})

	require.Nil(t, h.ProcessPayload([]byte{0x08, 0x01}, "topic", 1))
	require.Nil(t, h.ProcessPayload([]byte{0x08, 0x02}, "topic", 1))

	select {
	case <-fast:
	case <-time.After(2 * time.Second):
		t.Fatal("shard 2 was blocked by shard 1")
	}
	close(release)
	require.Nil(t, h.Close())
}

func TestAsyncPayloadHandler_RetriesThenSucceeds(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	inner := &payloadHandlerMock{processCalled: func([]byte, string, uint32) error {
		if calls.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	}}
	h, _ := process.NewAsyncPayloadHandler(process.ArgsAsyncPayloadHandler{Handler: inner, MaxRetries: 5})

	require.Nil(t, h.ProcessPayload([]byte("a"), "topic", 1))
	require.Nil(t, h.Close())
	require.Equal(t, int32(3), calls.Load())
}

func TestAsyncPayloadHandler_DropsAfterMaxRetries(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	inner := &payloadHandlerMock{processCalled: func([]byte, string, uint32) error {
		calls.Add(1)
		return errors.New("permanent")
	}}
	h, _ := process.NewAsyncPayloadHandler(process.ArgsAsyncPayloadHandler{Handler: inner, MaxRetries: 2})

	require.Nil(t, h.ProcessPayload([]byte("a"), "topic", 1))
	require.Nil(t, h.Close())
	require.Equal(t, int32(2), calls.Load())
}

func TestAsyncPayloadHandler_FullQueueBlocksInsteadOfDropping(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	var processed atomic.Int32
	inner := &payloadHandlerMock{processCalled: func([]byte, string, uint32) error {
		<-release
		processed.Add(1)
		return nil
	}}
	h, _ := process.NewAsyncPayloadHandler(process.ArgsAsyncPayloadHandler{Handler: inner, QueueSize: 1})

	// one in the worker, one in the queue; the third must wait for room
	require.Nil(t, h.ProcessPayload([]byte("1"), "topic", 1))
	require.Nil(t, h.ProcessPayload([]byte("2"), "topic", 1))

	returned := make(chan struct{})
	go func() {
		_ = h.ProcessPayload([]byte("3"), "topic", 1)
		close(returned)
	}()
	select {
	case <-returned:
		t.Fatal("expected backpressure while the queue is full")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	<-returned
	require.Nil(t, h.Close())
	require.Equal(t, int32(3), processed.Load())
}

func TestAsyncPayloadHandler_RejectsAfterClose(t *testing.T) {
	t.Parallel()

	inner := &payloadHandlerMock{processCalled: func([]byte, string, uint32) error { return nil }}
	h, _ := process.NewAsyncPayloadHandler(process.ArgsAsyncPayloadHandler{Handler: inner})

	require.Nil(t, h.Close())
	require.Equal(t, process.ErrAsyncPayloadHandlerClosed, h.ProcessPayload([]byte("a"), "topic", 1))
	require.Nil(t, h.Close())
}

func TestLeadingShardID_MatchesProtobufEncoding(t *testing.T) {
	t.Parallel()

	for _, shardID := range []uint32{0, 1, 2, 127, 128, 300, 4294967295} {
		block := &outport.OutportBlock{ShardID: shardID, BlockData: &outport.BlockData{HeaderHash: []byte("h")}}
		bytes, err := block.Marshal()
		require.Nil(t, err)
		require.Equal(t, shardID, process.LeadingShardID(bytes), "OutportBlock %d", shardID)

		finalized := &outport.FinalizedBlock{ShardID: shardID, HeaderHash: []byte("h")}
		bytes, err = finalized.Marshal()
		require.Nil(t, err)
		require.Equal(t, shardID, process.LeadingShardID(bytes), "FinalizedBlock %d", shardID)
	}

	require.Equal(t, uint32(0), process.LeadingShardID(nil))
	require.Equal(t, uint32(0), process.LeadingShardID([]byte{0x08}))
}
