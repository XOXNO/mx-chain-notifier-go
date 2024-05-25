package servicebus

import "errors"

// ErrNilServiceBusClient signals that a nil servicebus client has been provided
var ErrNilServiceBusClient = errors.New("nil servicebus client")

// ErrInvalidServiceBusExchangeName signals that an empty servicebus exchange name has been provided
var ErrInvalidServiceBusExchangeName = errors.New("invalid servicebus exchange name")

// ErrInvalidServiceBusExchangeType signals that an empty servicebus exchange type has been provided
var ErrInvalidServiceBusExchangeType = errors.New("invalid servicebus exchange type")
