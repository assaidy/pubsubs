package valkey

import (
	"context"
	"sync"

	"github.com/assaidy/brokers/pubsub"
	"github.com/valkey-io/valkey-go"
)

type Pubsub struct {
	client valkey.Client
}

// New creates a new in-memory pub/sub instance backed by go's channels.
func New(client valkey.Client) *Pubsub {
	ps := &Pubsub{
		client: client,
	}
	_ = pubsub.Pubsub(ps)
	return ps
}

// Publish sends a payload to the specified channel. If a codec is provided,
// it is applied first to marshal the payload before publishing.
func (me *Pubsub) Publish(ctx context.Context, channel string, payload []byte) error {
	return me.client.Do(ctx, me.client.B().Publish().Channel(channel).Message(string(payload)).Build()).Error()
}

// Subscribe registers a handler for messages on the specified channel.
func (me *Pubsub) Subscribe(ctx context.Context, channel string, handler pubsub.Handler) pubsub.Subscription {
	sub := &subscription{
		errsChan:  make(chan error, 10),
		doneChan:  make(chan struct{}),
		closeChan: make(chan struct{}),
	}

	go func() {
		var wg sync.WaitGroup
		receiveCtx, receiveCtxCancel := context.WithCancel(ctx)
		defer func() {
			receiveCtxCancel() // cancel the receiving when the user closes the subscription manually.
			wg.Wait()          // wait for the receiving to stop.
			close(sub.doneChan)
			close(sub.errsChan)
		}()

		wg.Go(func() {
			me.client.Receive(
				receiveCtx,
				me.client.B().Subscribe().Channel(channel).Build(),
				func(msg valkey.PubSubMessage) {
					if err := handler(ctx, []byte(msg.Message)); err != nil {
						sub.errsChan <- err
					}
				},
			)
		})

		select {
		case <-sub.closeChan:
		case <-ctx.Done():
		}
	}()

	return sub
}

type subscription struct {
	errsChan  chan error
	doneChan  chan struct{}
	closeChan chan struct{}
	once      sync.Once
}

func (me *subscription) Errs() <-chan error {
	return me.errsChan
}

func (me *subscription) Done() <-chan struct{} {
	return me.doneChan
}

func (me *subscription) Close() {
	me.once.Do(func() { close(me.closeChan) })
}
