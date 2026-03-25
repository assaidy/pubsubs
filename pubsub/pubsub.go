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
	Close()
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
				case err := <-sub.Errs():
					errorHandler(err)
				}
			}
		})
	}
}
