// Package pubsub provides a generic publish/subscribe abstraction.
package pubsub

import (
	"context"
	"sync"
)

// Pubsub combines Publisher and Subscriber capabilities.
type Pubsub interface {
	Publisher
	Subscriber
}

// Publisher defines the interface for sending messages to a channel.
type Publisher interface {
	// Publish sends a payload to the specified channel. If a codec is provided,
	// it is applied first to marshal the payload before publishing.
	Publish(ctx context.Context, channel string, payload []byte) error
}

// Subscriber defines the interface for receiving messages from a channel.
type Subscriber interface {
	// Subscribe registers a handler for messages on the specified channel.
	// If a codec is provided, it is applied first to unmarshal the payload
	// before passing it to the handler.
	Subscribe(ctx context.Context, channel string, handler Handler) (Subscription, error)
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
}

// WaitAll starts a wg.Go() for each subscription that waits for it to finish.
// Errors from each subscription are passed to errorHandler.
func WaitAll(wg *sync.WaitGroup, errorHandler func(error), subscriptions ...Subscription) {
	for _, sub := range subscriptions {
		wg.Go(func() {
			select {
			case <-sub.Done():
				return
			case err := <-sub.Errs():
				errorHandler(err)
			}
		})
	}
}
