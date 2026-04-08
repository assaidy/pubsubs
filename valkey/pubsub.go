package valkey

import (
	"context"
	"sync"

	"github.com/assaidy/pubsub"
	"github.com/valkey-io/valkey-go"
)

var _ pubsub.Pubsub = (*Pubsub)(nil)

type Pubsub struct {
	client valkey.Client
}

// New creates a new in-memory pub/sub instance backed by go's channels.
func New(client valkey.Client) *Pubsub {
	return &Pubsub{
		client: client,
	}
}

// Publish sends a payload to the specified channel. If a codec is provided,
// it is applied first to marshal the payload before publishing.
func (me *Pubsub) Publish(ctx context.Context, channel string, payload []byte) error {
	return me.client.Do(ctx, me.client.B().Publish().Channel(channel).Message(string(payload)).Build()).Error()
}

// Subscribe registers a handler for messages on the specified channel.
func (me *Pubsub) Subscribe(ctx context.Context, channel string, handler pubsub.Handler) pubsub.Subscription {
	sub := pubsub.NewBasicSubscription()

	go func() {
		var wg sync.WaitGroup
		receiveCtx, receiveCtxCancel := context.WithCancel(ctx)
		defer func() {
			receiveCtxCancel() // cancel the receiving when the user closes the subscription manually.
			wg.Wait()          // wait for the receiving to stop.
			close(sub.ErrsChan)
		}()

		wg.Go(func() {
			me.client.Receive(
				receiveCtx,
				me.client.B().Subscribe().Channel(channel).Build(),
				func(msg valkey.PubSubMessage) {
					if err := handler(ctx, []byte(msg.Message)); err != nil {
						sub.ErrsChan <- err
					}
				},
			)
		})

		select {
		case <-sub.DoneChan:
		case <-ctx.Done():
		}
	}()

	return sub
}
