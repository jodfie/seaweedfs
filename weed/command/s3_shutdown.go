package command

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"google.golang.org/grpc"
)

// s3ServiceShutdown owns every S3 HTTP listener (including local and auxiliary
// listeners). Registration and the stopping fence share a lock, so a server
// finishing startup after shutdown cannot reopen admission.
type s3ServiceShutdown struct {
	done     chan struct{}
	mu       sync.Mutex
	stopping bool
	servers  []*http.Server
	workers  sync.WaitGroup
	once     sync.Once
	stopGrpc func()
	stopAPI  func()
}

func (s *s3ServiceShutdown) track(server *http.Server) *http.Server {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		// Close before Serve also prevents a late listener from accepting requests.
		server.Close()
	} else {
		s.servers = append(s.servers, server)
	}
	return server
}

func (s *s3ServiceShutdown) start(work func()) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return false
	}
	s.workers.Add(1)
	go func() { defer s.workers.Done(); work() }()
	return true
}

func (s *s3ServiceShutdown) stop() {
	s.once.Do(func() {
		if s.done != nil {
			defer close(s.done)
		}
		s.mu.Lock()
		s.stopping = true
		servers := append([]*http.Server(nil), s.servers...)
		s.mu.Unlock()
		// Preserve the existing S3 request-drain contract: do not retire the filer
		// under a still-running request. The process owner retains its outer bound.
		var drained sync.WaitGroup
		for _, server := range servers {
			drained.Add(1)
			go func() {
				defer drained.Done()
				if err := server.Shutdown(context.Background()); err != nil {
					glog.Warningf("S3 HTTP shutdown: %v", err)
				}
			}()
		}
		drained.Wait()
		if s.stopGrpc != nil {
			s.stopGrpc()
		}
		s.workers.Wait()
		if s.stopAPI != nil {
			s.stopAPI()
		}
	})
}

func (s3opt *S3Options) ownHTTP(server *http.Server) *http.Server {
	if s3opt.lifecycle == nil {
		return server
	}
	return s3opt.lifecycle.track(server)
}
func (s3opt *S3Options) serveAsync(work func()) bool {
	if s3opt.lifecycle == nil {
		go work()
		return true
	}
	return s3opt.lifecycle.start(work)
}

func (s3opt *S3Options) serveListener(listener net.Listener, work func()) {
	if !s3opt.serveAsync(work) {
		listener.Close()
	}
}

func stopS3Grpc(server *grpc.Server) {
	done := make(chan struct{})
	go func() { server.GracefulStop(); close(done) }()
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		server.Stop()
		<-done
	}
}
