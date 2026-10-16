package s3api

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestS3ShutdownCancelsAndJoinsOriginalMetadataWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	canceled := make(chan struct{})
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); <-ctx.Done(); close(canceled); <-release }()
	var reconcilerStops atomic.Int32
	s := &S3ApiServer{metadataFollowCancel: cancel, metadataFollowDone: workerDone, versionsReconcilerStop: func() { reconcilerStops.Add(1) }}
	returned := make(chan struct{}, 2)
	go func() { s.Shutdown(); returned <- struct{}{} }()
	go func() { s.Shutdown(); returned <- struct{}{} }()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("worker was not canceled")
	}
	select {
	case <-returned:
		t.Fatal("shutdown returned before original worker joined")
	case <-time.After(25 * time.Millisecond):
	}
	if reconcilerStops.Load() != 0 {
		t.Fatal("retired dependent state before metadata callback joined")
	}
	unblock()
	for range 2 {
		select {
		case <-returned:
		case <-time.After(time.Second):
			t.Fatal("shutdown caller did not join")
		}
	}
	s.Shutdown()
	if reconcilerStops.Load() != 1 {
		t.Fatalf("reconciler stops=%d", reconcilerStops.Load())
	}
}
