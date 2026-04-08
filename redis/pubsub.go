package redis

import (
	"context"
	"sync"

	"github.com/assaidy/pubsub"
	"github.com/redis/go-redis/v9"
)

var _ pubsub.Pubsub = (*Pubsub)(nil)

type Pubsub struct {
	client redis.UniversalClient
}

func New(client redis.UniversalClient) *Pubsub {
	return &Pubsub{
		client: client,
	}
}

func (me *Pubsub) Publish(ctx context.Context, channel string, payload []byte) error {
	return me.client.Publish(ctx, channel, string(payload)).Err()
}

func (me *Pubsub) Subscribe(ctx context.Context, channel string, handler pubsub.Handler) pubsub.Subscription {
	sub := pubsub.NewBasicSubscription()

	go func() {
		var wg sync.WaitGroup
		receiveCtx, receiveCtxCancel := context.WithCancel(ctx)
		defer func() {
			receiveCtxCancel()
			wg.Wait()
			close(sub.ErrsChan)
		}()

		wg.Go(func() {
			pubsubConn := me.client.Subscribe(receiveCtx, channel)
			defer func() {
				pubsubConn.Close()
				close(sub.CloseFinishedChan)
			}()

			ch := pubsubConn.Channel(redis.WithChannelSize(100))
			for {
				select {
				case <-receiveCtx.Done():
					return
				case msg := <-ch:
					if err := handler(ctx, []byte(msg.Payload)); err != nil {
						sub.ErrsChan <- err
					}
				}
			}
		})

		select {
		case <-sub.DoneChan:
		case <-ctx.Done():
		}
	}()

	return sub
}
