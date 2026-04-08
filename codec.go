package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
)

// Codec defines the interface for encoding and decoding message payloads.
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(raw []byte, v any) error
}

// PublishWithCodec encodes a value using the provided codec and publishes it to the given channel.
func PublishWithCodec(ctx context.Context, pub Publisher, channel string, v any, codec Codec) error {
	payload, err := codec.Encode(v)
	if err != nil {
		return fmt.Errorf("codec: failed to encode payload: %w", err)
	}
	return pub.Publish(ctx, channel, payload)
}

// SubscribeWithCodec subscribes to a channel and decodes messages using the provided codec before passing them to the handler.
func SubscribeWithCodec[T any](ctx context.Context, sub Subscriber, channel string, handler CodecHandler[T], codec Codec) Subscription {
	return sub.Subscribe(ctx, channel, func(ctx context.Context, payload []byte) error {
		var v T
		if err := codec.Decode(payload, &v); err != nil {
			return fmt.Errorf("codec: failed to decode payload: %w", err)
		}
		return handler(ctx, v)
	})
}

// CodecHandler is a handler function for decoded messages.
type CodecHandler[T any] func(ctx context.Context, payload T) error

var (
	CodecJson        = Codec(jsonCodec{})
	CodecMessagePack = Codec(messagePack{})
)

type jsonCodec struct{}

func (me jsonCodec) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (me jsonCodec) Decode(raw []byte, v any) error {
	return json.Unmarshal(raw, v)
}

type messagePack struct{}

func (me messagePack) Encode(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (me messagePack) Decode(raw []byte, v any) error {
	return msgpack.Unmarshal(raw, v)
}
