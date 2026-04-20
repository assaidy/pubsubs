// Package pubsub provides a generic publish/subscribe abstraction.
package pubsub

import (
	"context"
	"errors"
	"log/slog"
	"sync"
)

// Pubsub combines Publisher and Subscriber capabilities.
type Pubsub interface {
	Publisher
	Subscriber
}

// Publisher defines the interface for sending messages to a channel.
type Publisher interface {
	// Publish sends a payload to the specified channel.
	Publish(ctx context.Context, channel string, payload []byte) error
}

// Subscriber defines the interface for receiving messages from a channel.
type Subscriber interface {
	// Subscribe registers a handler for messages on the specified channel.
	Subscribe(ctx context.Context, channel string, handler Handler) Subscription
}

// Handler is a function that processes a message payload.
type Handler func(ctx context.Context, payload []byte) error

// Subscription represents a subscription to a channel and provides methods
// to manage its lifecycle.
type Subscription interface {
	// Errs returns a channel that delivers handler errors.
	Errs() <-chan error
	// Done returns a channel that's closed when the subscription ends.
	Done() <-chan struct{}
	// Close terminates the subscription.
	Close() error
	// TODO: Add some sort of acknowledgment mechanisms to ensure messages are handled, so it's used with kafka and rmq.
}

var ErrSubscriptionClosed = errors.New("subscription closed")

// BasicSubscription is a basic implementation for [Subscription]
type BasicSubscription struct {
	// ErrsChan is a channel that receives errors from the message handler.
	ErrsChan chan error
	// DoneChan is a channel that is closed when the subscription ends.
	// Callers can wait on this channel to know when the subscription is finished.
	DoneChan chan struct{}
	// CloseFinishedChan is used to ensure Close() blocks until the receiver goroutine
	// has stopped. The receiver must close this channel after it stops receiving.
	// Close() waits on this channel before returning.
	CloseFinishedChan chan struct{}
	isClosed          bool
	mu                sync.Mutex
}

// NewBasicSubscription returns an instance of [BasicSubscription]
// that implements [Subscription]
func NewBasicSubscription() *BasicSubscription {
	return &BasicSubscription{
		ErrsChan:          make(chan error, 10),
		DoneChan:          make(chan struct{}, 1),
		CloseFinishedChan: make(chan struct{}, 1),
	}
}

func (me *BasicSubscription) Errs() <-chan error {
	return me.ErrsChan
}

func (me *BasicSubscription) Done() <-chan struct{} {
	return me.DoneChan
}

func (me *BasicSubscription) Close() error {
	me.mu.Lock()
	defer me.mu.Unlock()

	if me.isClosed {
		return ErrSubscriptionClosed
	}
	close(me.DoneChan)
	<-me.CloseFinishedChan
	me.isClosed = true
	return nil
}

// WaitAll starts a wg.Go() for each subscription that waits for it to finish.
// Errors from each subscription are passed to errorHandler.
func WaitAll(wg *sync.WaitGroup, errorHandler func(error), subscriptions ...Subscription) {
	for _, sub := range subscriptions {
		wg.Go(func() {
			defer sub.Close()

			for {
				select {
				case <-sub.Done():
					return
				case err, ok := <-sub.Errs():
					if !ok {
						return
					}
					errorHandler(err)
				}
			}
		})
	}
}

func DefaultErrorHandler(logger *slog.Logger) func(error) {
	return func(err error) {
		logger.Error("subscription error", "error", err)
	}
}
