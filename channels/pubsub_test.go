package channels

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/assaidy/pubsub"
	"github.com/stretchr/testify/assert"
)

func TestPublish_Subscribe(t *testing.T) {
	ps := New()
	ctx := context.Background()

	msgCh := make(chan []byte, 1)
	sub := ps.Subscribe(ctx, "test-channel", func(ctx context.Context, payload []byte) error {
		msgCh <- payload
		return nil
	})
	defer sub.Close()

	// Wait for goroutine to start
	time.Sleep(10 * time.Millisecond)

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
	ps := New()
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

	time.Sleep(10 * time.Millisecond)

	for i := range 3 {
		err := ps.Publish(ctx, "test-channel", []byte("message-"+string(rune('a'+i))))
		assert.NoError(t, err)
	}

	// Wait for either all messages or timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for messages")
	}

	assert.Len(t, messages, 3)
}

func TestMultipleSubscribers(t *testing.T) {
	ps := New()
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

	time.Sleep(10 * time.Millisecond)

	err := ps.Publish(ctx, "test-channel", []byte("broadcast"))
	assert.NoError(t, err)

	// Wait for both subscribers or timeout
	done1 := make(chan struct{})
	done2 := make(chan struct{})
	go func() {
		<-msgCh1
		close(done1)
	}()
	go func() {
		<-msgCh2
		close(done2)
	}()

	select {
	case <-done1:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message on subscriber 1")
	}

	select {
	case <-done2:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message on subscriber 2")
	}
}

func TestSubscription_Close(t *testing.T) {
	ps := New()
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
	ps := New()
	ctx := context.Background()

	errCh := make(chan error, 1)
	sub := ps.Subscribe(ctx, "test-channel", func(ctx context.Context, payload []byte) error {
		errCh <- assert.AnError
		return assert.AnError
	})
	defer sub.Close()

	time.Sleep(10 * time.Millisecond)

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
	ps := New()
	ctx := context.Background()

	msgCh := make(chan []byte, 1)
	sub := ps.Subscribe(ctx, "test-channel", func(ctx context.Context, payload []byte) error {
		msgCh <- payload
		return nil
	})

	time.Sleep(10 * time.Millisecond)

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

func TestPublish_WithContextCancellation(t *testing.T) {
	ps := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub := ps.Subscribe(ctx, "test-channel", func(ctx context.Context, payload []byte) error {
		return nil
	})
	defer sub.Close()

	time.Sleep(10 * time.Millisecond)

	// Cancel context before publishing
	cancel()

	// Should not return error, just not deliver the message
	assert.NoError(t, ps.Publish(ctx, "test-channel", []byte("should not be delivered")))
}

func TestBufferLength(t *testing.T) {
	// Test with custom buffer length
	ps := New(Config{BufferLength: 1})
	ctx := context.Background()

	msgCh := make(chan []byte, 1)
	sub := ps.Subscribe(ctx, "test-channel", func(ctx context.Context, payload []byte) error {
		msgCh <- payload
		return nil
	})
	defer sub.Close()

	time.Sleep(10 * time.Millisecond)

	// First publish should work
	err := ps.Publish(ctx, "test-channel", []byte("first"))
	assert.NoError(t, err)

	// Second publish should block because buffer is full and no one is reading yet
	done := make(chan struct{})
	go func() {
		ps.Publish(ctx, "test-channel", []byte("second"))
		// This might block or return immediately depending on implementation
		close(done)
	}()

	// Give it a moment to potentially block
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		// If it's still blocking, that's expected behavior
	}

	// Read the first message to unblock
	select {
	case <-msgCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for first message")
	}

	// Now the second should be able to go through
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for second publish to complete")
	}
}
