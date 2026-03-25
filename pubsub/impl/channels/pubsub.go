package channels

import (
	"context"
	"slices"
	"sync"

	"github.com/assaidy/brokers/pubsub"
)

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

// Publish sends a payload to the specified channel. If a codec is provided,
// it is applied first to marshal the payload before publishing.
func (me *Pubsub) Publish(ctx context.Context, channel string, payload []byte) error {
	me.mu.RLock()
	defer me.mu.RUnlock()
	for _, sub := range me.subscribers[channel] {
		// go func() { sub <- payload }()
		sub <- payload
	}
	return nil
}

// Subscribe registers a handler for messages on the specified channel.
// If a codec is provided, it is applied first to unmarshal the payload
// before passing it to the handler.
func (me *Pubsub) Subscribe(ctx context.Context, channel string, handler pubsub.Handler) (pubsub.Subscription, error) {
	sub := &subscription{
		errs: make(chan error, 10),
		done: make(chan struct{}),
	}

	subscriber := make(chan []byte, me.config.BufferLength)
	me.mu.Lock()
	me.subscribers[channel] = append(me.subscribers[channel], subscriber)
	me.mu.Unlock()

	go func() {
		defer func() {
			me.mu.Lock()
			me.subscribers[channel] = slices.DeleteFunc(me.subscribers[channel], func(s chan<- []byte) bool {
				return s == subscriber
			})
			me.mu.Unlock()
			close(sub.done)
			close(sub.errs)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-sub.done:
				return
			case payload := <-subscriber:
				if err := handler(ctx, payload); err != nil {
					select {
					case sub.errs <- err:
					case <-ctx.Done():
					case <-sub.done:
					}
				}
			}
		}
	}()

	return sub, nil
}

type subscription struct {
	errs chan error
	done chan struct{}
}

func (me *subscription) Errs() <-chan error {
	return me.errs
}

func (me *subscription) Done() <-chan struct{} {
	return me.done
}

func (me *subscription) Close() error {
	close(me.errs)
	close(me.done)
	return nil
}
