package redis

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/assaidy/pubsubs"
	"github.com/assaidy/pubsubs/test_utils"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	testcontainersredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

func newTestContainer(t *testing.T) (*redis.Client, func()) {
	ctx := context.Background()
	container, err := testcontainersredis.Run(ctx, "docker.io/redis:7-alpine")
	assert.NoError(t, err)
	connStr, err := container.ConnectionString(ctx)
	assert.NoError(t, err)
	u, err := url.Parse(connStr)
	assert.NoError(t, err)
	client := redis.NewClient(&redis.Options{
		Addr: u.Host,
	})

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
	var messages []string
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
		time.Sleep(10 * time.Millisecond)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	test_utils.WaitForChannelClosed(t, done)
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
	case msg := <-msgCh1:
		assert.Equal(t, "broadcast", string(msg))
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message on subscriber 1")
	}

	select {
	case msg := <-msgCh2:
		assert.Equal(t, "broadcast", string(msg))
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

	test_utils.WaitForChannelClosed(t, sub.Done())

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
