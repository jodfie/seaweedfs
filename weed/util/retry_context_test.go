package util

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryUntilContextCanceledBeforeAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts := 0
	err := RetryUntilContext(ctx, "test", func() error { attempts++; return nil }, func(error) bool { return true })
	if !errors.Is(err, context.Canceled) || attempts != 0 {
		t.Fatalf("err=%v attempts=%d", err, attempts)
	}
}
func TestRetryUntilContextCancelsBackoffWithoutReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backingOff := make(chan struct{})
	result := make(chan error, 1)
	attempts := 0
	go func() {
		result <- RetryUntilContext(ctx, "test", func() error { attempts++; return errors.New("synthetic failure") }, func(error) bool { close(backingOff); return true })
	}()
	<-backingOff
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || attempts != 1 {
			t.Fatalf("err=%v attempts=%d", err, attempts)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("retry slept through owner cancellation")
	}
}
func TestRetryUntilContextPreservesSuccessAndErrorDecision(t *testing.T) {
	if err := RetryUntilContext(context.Background(), "test", func() error { return nil }, func(error) bool { t.Fatal("unexpected retry"); return true }); err != nil {
		t.Fatal(err)
	}
	expected := errors.New("synthetic permanent failure")
	if err := RetryUntilContext(context.Background(), "test", func() error { return expected }, func(error) bool { return false }); err != expected {
		t.Fatal(err)
	}
}
