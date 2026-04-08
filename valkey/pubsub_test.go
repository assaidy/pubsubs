package valkey

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/assaidy/pubsub"
	"github.com/stretchr/testify/assert"
	testcontainersvalkey "github.com/testcontainers/testcontainers-go/modules/valkey"
	"github.com/valkey-io/valkey-go"
)

func newTestContainer(t *testing.T) (valkey.Client, func()) {
	ctx := context.Background()
	container, err := testcontainersvalkey.Run(ctx, "docker.io/valkey/valkey:8.0-alpine")
	assert.NoError(t, err)
	connStr, err := container.ConnectionString(ctx)
	assert.NoError(t, err)
	options, err := valkey.ParseURL(connStr)
	assert.NoError(t, err)
	client, err := valkey.NewClient(options)
	assert.NoError(t, err)

	cleanup := func() {
		client.Close()
		container.Terminate(ctx)
	}

	return client, cleanup
}

func TestPublish_Subscribe(t *testing.T) {
	client, cleanup := newTestContainer(t)
	t.Cleanup(cleanup)

	ps := New(client)
	ctx := context.Background()

	msgCh := make(chan []byte, 1)
	sub := ps.Subscribe(ctx, "test-channel", func(ctx context.Context, payload []byte) error {
		msgCh <- payload
		return nil
	})
	defer sub.Close()

	// wait for the subscribe command to complete
	time.Sleep(50 * time.Millisecond)

	err := ps.Publish(ctx, "test-channel", []byte("hello world"))
	assert.NoError(t, err)

	select {
	case msg := <-msgCh:
		assert.Equal(t, "hello world", string(msg))
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestSubscribe_MultipleMessages(t *testing.T) {
	client, cleanup := newTestContainer(t)
	t.Cleanup(cleanup)

	ps := New(client)
	ctx := context.Background()

	var mu sync.Mutex
	messages := []string{}
	var wg sync.WaitGroup
	wg.Add(1)

	sub := ps.Subscribe(ctx, "test-channel", func(ctx context.Context, payload []byte) error {
		mu.Lock()
		messages = append(messages, string(payload))
		if len(messages) == 3 {
			wg.Done()
		}
		mu.Unlock()
		return nil
	})
	defer sub.Close()

	time.Sleep(50 * time.Millisecond)

	for i := range 3 {
		err := ps.Publish(ctx, "test-channel", []byte("message-"+string(rune('a'+i))))
		assert.NoError(t, err)
	}

	select {
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for messages")
	default:
	}

	assert.Len(t, messages, 3)
}

func TestMultipleSubscribers(t *testing.T) {
	client, cleanup := newTestContainer(t)
	t.Cleanup(cleanup)

	ps := New(client)
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)

	msgCh1 := make(chan []byte, 1)
	sub1 := ps.Subscribe(ctx, "test-channel", func(ctx context.Context, payload []byte) error {
		msgCh1 <- payload
		wg.Done()
		return nil
	})
	defer sub1.Close()

	msgCh2 := make(chan []byte, 1)
	sub2 := ps.Subscribe(ctx, "test-channel", func(ctx context.Context, payload []byte) error {
		msgCh2 <- payload
		wg.Done()
		return nil
	})
	defer sub2.Close()

	time.Sleep(50 * time.Millisecond)

	err := ps.Publish(ctx, "test-channel", []byte("broadcast"))
	assert.NoError(t, err)

	select {
	case msg1 := <-msgCh1:
		assert.Equal(t, "broadcast", string(msg1))
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message on subscriber 1")
	}

	select {
	case msg2 := <-msgCh2:
		assert.Equal(t, "broadcast", string(msg2))
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message on subscriber 2")
	}
}

func TestSubscription_Close(t *testing.T) {
	client, cleanup := newTestContainer(t)
	t.Cleanup(cleanup)

	ps := New(client)
	ctx := context.Background()

	sub := ps.Subscribe(ctx, "test-channel", func(ctx context.Context, payload []byte) error {
		return nil
	})

	err := sub.Close()
	assert.NoError(t, err)

	select {
	case <-sub.Done():
	case <-time.After(1 * time.Second):
		t.Fatal("subscription done channel not closed after close")
	}

	err = sub.Close()
	assert.Equal(t, pubsub.ErrSubscriptionClosed, err)
}

func TestHandlerError(t *testing.T) {
	client, cleanup := newTestContainer(t)
	t.Cleanup(cleanup)

	ps := New(client)
	ctx := context.Background()

	errCh := make(chan error, 1)
	sub := ps.Subscribe(ctx, "test-channel", func(ctx context.Context, payload []byte) error {
		errCh <- assert.AnError
		return assert.AnError
	})
	defer sub.Close()

	time.Sleep(50 * time.Millisecond)

	err := ps.Publish(ctx, "test-channel", []byte("test"))
	assert.NoError(t, err)

	select {
	case err := <-errCh:
		assert.Equal(t, assert.AnError, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for handler error")
	}
}

func TestUnsubscribe(t *testing.T) {
	client, cleanup := newTestContainer(t)
	t.Cleanup(cleanup)

	ps := New(client)
	ctx := context.Background()

	msgCh := make(chan []byte, 1)
	sub := ps.Subscribe(ctx, "test-channel", func(ctx context.Context, payload []byte) error {
		msgCh <- payload
		return nil
	})

	time.Sleep(50 * time.Millisecond)

	err := ps.Publish(ctx, "test-channel", []byte("before close"))
	assert.NoError(t, err)

	select {
	case msg := <-msgCh:
		assert.Equal(t, "before close", string(msg))
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	sub.Close()

	err = ps.Publish(ctx, "test-channel", []byte("after close"))
	assert.NoError(t, err)

	select {
	case <-msgCh:
		t.Fatal("should not receive message after unsubscribe")
	case <-time.After(500 * time.Millisecond):
	}
}
