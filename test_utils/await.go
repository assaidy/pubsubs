package test_utils

import (
	"testing"
	"time"
)

const (
	DefaultWaitTimeout = 2 * time.Second
	PollingInterval    = 10 * time.Millisecond
)

func WaitForChannelClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(DefaultWaitTimeout):
		t.Fatal("timeout waiting for channel to close")
	}
}
