package pubsub_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/assaidy/pubsub"
	"github.com/assaidy/pubsub/test_utils"
	"github.com/stretchr/testify/assert"
)

func TestBasicSubscription_Done(t *testing.T) {
	sub := pubsub.NewBasicSubscription()

	select {
	case <-sub.Done():
		t.Fatal("done channel should not be closed before Close()")
	default:
	}

	go func() {
		sub.CloseFinishedChan <- struct{}{}
	}()

	err := sub.Close()
	assert.NoError(t, err)

	test_utils.WaitForChannelClosed(t, sub.Done())
}

func TestBasicSubscription_CloseIdempotent(t *testing.T) {
	sub := pubsub.NewBasicSubscription()

	go func() {
		sub.CloseFinishedChan <- struct{}{}
	}()

	err := sub.Close()
	assert.NoError(t, err)

	test_utils.WaitForChannelClosed(t, sub.Done())

	err = sub.Close()
	assert.Equal(t, pubsub.ErrSubscriptionClosed, err)
}

func TestBasicSubscription_Errs(t *testing.T) {
	sub := pubsub.NewBasicSubscription()

	errCh := sub.Errs()
	assert.NotNil(t, errCh)

	testErr := assert.AnError

	go func() {
		sub.ErrsChan <- testErr
	}()

	select {
	case err := <-errCh:
		assert.Equal(t, testErr, err)
	case <-time.After(2 * time.Second):
		t.Fatal("expected error on Errs channel")
	}
}

func TestWaitAll(t *testing.T) {
	var wg sync.WaitGroup

	sub1 := pubsub.NewBasicSubscription()
	sub2 := pubsub.NewBasicSubscription()

	errHandler := func(err error) {}

	pubsub.WaitAll(&wg, errHandler, sub1, sub2)

	sub1.CloseFinishedChan <- struct{}{}
	sub2.CloseFinishedChan <- struct{}{}
	sub1.Close()
	sub2.Close()

	test_utils.WaitForChannelClosed(t, sub1.Done())
	test_utils.WaitForChannelClosed(t, sub2.Done())
}

func TestWaitAll_ClosesSubscriptions(t *testing.T) {
	var wg sync.WaitGroup

	sub1 := pubsub.NewBasicSubscription()
	sub2 := pubsub.NewBasicSubscription()

	pubsub.WaitAll(&wg, func(error) {}, sub1, sub2)

	sub1.CloseFinishedChan <- struct{}{}
	sub2.CloseFinishedChan <- struct{}{}
	sub1.Close()
	sub2.Close()

	test_utils.WaitForChannelClosed(t, sub1.Done())
	test_utils.WaitForChannelClosed(t, sub2.Done())
}

func TestWaitAll_EmptySubscriptions(t *testing.T) {
	var wg sync.WaitGroup

	pubsub.WaitAll(&wg, func(error) {})
}

func TestDefaultErrorHandler(t *testing.T) {
	var buf struct {
		logs []string
		mu   sync.Mutex
	}

	logger := slog.New(slog.NewTextHandler(&logWriter{&buf}, nil))

	handler := pubsub.DefaultErrorHandler(logger)

	testErr := context.DeadlineExceeded
	handler(testErr)

	time.Sleep(50 * time.Millisecond)

	buf.mu.Lock()
	defer buf.mu.Unlock()
	assert.Len(t, buf.logs, 1)
}

type logWriter struct {
	buf *struct {
		logs []string
		mu   sync.Mutex
	}
}

func (w *logWriter) Write(p []byte) (n int, err error) {
	w.buf.mu.Lock()
	defer w.buf.mu.Unlock()
	w.buf.logs = append(w.buf.logs, string(p))
	return len(p), nil
}
