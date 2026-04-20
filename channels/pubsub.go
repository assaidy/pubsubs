package channels

import (
	"context"
	"slices"
	"sync"

	"github.com/assaidy/pubsubs"
)

var _ pubsub.Pubsub = (*Pubsub)(nil)

type Pubsub struct {
	subscribers map[string][]chan<- []byte
	config      Config
	mu          sync.RWMutex
}

// Config configures the pubsub.
type Config struct {
	// BufferLength is the capacity of the buffered channel used for each
	// subscriber. A larger buffer provides more tolerance for slow subscribers,
	// but increases memory usage. A full buffer causes Publish to block until
	// the subscriber reads.
	//
	// Defaults to 100.
	BufferLength int
}

// New creates a new in-memory pub/sub instance backed by go's channels.
func New(config ...Config) *Pubsub {
	conf := Config{
		BufferLength: 100,
	}
	if len(config) > 0 {
		c := config[0]
		if c.BufferLength > 0 {
			conf.BufferLength = c.BufferLength
		}
	}
	ps := &Pubsub{
		subscribers: make(map[string][]chan<- []byte),
		config:      conf,
	}
	_ = pubsub.Pubsub(ps)
	return ps
}

// Publish sends a payload to the specified channel.
func (me *Pubsub) Publish(ctx context.Context, channel string, payload []byte) error {
	me.mu.RLock()
	defer me.mu.RUnlock()
	for _, sub := range me.subscribers[channel] {
		select {
		case sub <- payload:
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

// Subscribe registers a handler for messages on the specified channel.
// It returns an error when the subscription initiation failes.
func (me *Pubsub) Subscribe(ctx context.Context, channel string, handler pubsub.Handler) pubsub.Subscription {
	subscriber := make(chan []byte, me.config.BufferLength)
	me.mu.Lock()
	me.subscribers[channel] = append(me.subscribers[channel], subscriber)
	me.mu.Unlock()

	sub := pubsub.NewBasicSubscription()

	go func() {
		defer func() {
			me.mu.Lock()
			me.subscribers[channel] = slices.DeleteFunc(
				me.subscribers[channel],
				func(s chan<- []byte) bool { return s == subscriber },
			)
			me.mu.Unlock()
			close(sub.ErrsChan)
			close(sub.CloseFinishedChan)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-sub.DoneChan:
				return
			case payload := <-subscriber:
				if err := handler(ctx, payload); err != nil {
					select {
					case sub.ErrsChan <- err:
					case <-sub.DoneChan:
					case <-ctx.Done():
					}
				}
			}
		}
	}()

	return sub
}
