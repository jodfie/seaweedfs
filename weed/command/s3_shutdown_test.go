package command

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestS3ShutdownDrainsEveryListenerBeforeAPIAndJoinsCallers(t *testing.T) {
	var apiStops atomic.Int32
	owner := &s3ServiceShutdown{stopAPI: func() { apiStops.Add(1) }}
	var releases []func()
	var completed []chan struct{}
	var closing []chan struct{}
	for i := range 3 {
		entered, release, done, closed := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		releases = append(releases, unblock)
		completed = append(completed, done)
		closing = append(closing, closed)
		var listener net.Listener
		var err error
		if i == 2 {
			listener, err = net.Listen("unix", filepath.Join(t.TempDir(), "s3.sock"))
		} else {
			listener, err = net.Listen("tcp", "127.0.0.1:0")
		}
		if err != nil {
			t.Fatal(err)
		}
		server := owner.track(&http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release; w.WriteHeader(204) })})
		server.RegisterOnShutdown(func() { close(closed) })
		owner.start(func() { server.Serve(listener) })
		transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, listener.Addr().Network(), listener.Addr().String())
		}}
		defer transport.CloseIdleConnections()
		client := &http.Client{Transport: transport}
		go func() {
			defer close(done)
			resp, err := client.Get("http://fixture/")
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body)
			if resp.StatusCode != 204 {
				t.Errorf("status=%d", resp.StatusCode)
			}
		}()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("request did not enter")
		}
	}
	returned := make(chan struct{}, 2)
	go func() { owner.stop(); returned <- struct{}{} }()
	go func() { owner.stop(); returned <- struct{}{} }()
	for _, c := range closing {
		select {
		case <-c:
		case <-time.After(3 * time.Second):
			t.Fatal("listener was not fenced")
		}
	}
	releases[0]()
	<-completed[0]
	releases[1]()
	<-completed[1]
	if apiStops.Load() != 0 {
		t.Fatal("API stopped while auxiliary listener request was active")
	}
	select {
	case <-returned:
		t.Fatal("shutdown returned before all handlers joined")
	default:
	}
	releases[2]()
	<-completed[2]
	for range 2 {
		select {
		case <-returned:
		case <-time.After(3 * time.Second):
			t.Fatal("shutdown did not join")
		}
	}
	if apiStops.Load() != 1 {
		t.Fatalf("API stops=%d", apiStops.Load())
	}
}
func TestS3ShutdownFencesLateServersAndClosesUnservedListener(t *testing.T) {
	owner := &s3ServiceShutdown{}
	owner.stop()
	options := &S3Options{lifecycle: owner}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ran := false
	options.serveListener(listener, func() { ran = true })
	if ran {
		t.Fatal("late worker started")
	}
	if _, err := listener.Accept(); err == nil {
		t.Fatal("late listener remained open")
	}
	listener2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener2.Close()
	server := owner.track(&http.Server{})
	if err := server.Serve(listener2); err != http.ErrServerClosed {
		t.Fatalf("late Serve=%v", err)
	}
}
func TestS3ShutdownJoinsWorkerStillRegisteringListener(t *testing.T) {
	owner := &s3ServiceShutdown{}
	started, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	owner.start(func() { defer close(exited); close(started); <-release; owner.track(&http.Server{}) })
	<-started
	stopped := make(chan struct{})
	go func() { owner.stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("shutdown abandoned startup worker")
	case <-time.After(25 * time.Millisecond):
	}
	unblock()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not join late startup worker")
	}
	<-exited
}
func TestS3CanceledStartupSignalsCompletionWithoutUsingOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	options := &S3Options{shutdownCtx: ctx, shutdownDone: done}
	if !options.startS3Server() {
		t.Fatal("canceled startup failed")
	}
	select {
	case <-done:
	default:
		t.Fatal("startup completion not signaled")
	}
}
func TestFilerShutdownWaitsForS3BeforeClosingAdmission(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer server.Close()
	var stopped atomic.Int32
	shutdown := newFilerShutdownAfter(func() { close(entered); <-release }, func() { stopped.Add(1) }, func() { stopped.Add(1) }, server.Config)
	joined := make(chan struct{})
	go func() { shutdown(); close(joined) }()
	<-entered
	response, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatal("filer unavailable while S3 still drains:", err)
	}
	response.Body.Close()
	if stopped.Load() != 0 {
		t.Fatal("filer drain started before dependent shutdown joined")
	}
	unblock()
	select {
	case <-joined:
	case <-time.After(3 * time.Second):
		t.Fatal("filer shutdown did not finish")
	}
	if stopped.Load() != 2 {
		t.Fatalf("retirement calls=%d", stopped.Load())
	}
}
