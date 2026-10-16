package filer

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type aggregationShutdownRPC struct {
	filer_pb.UnimplementedSeaweedFilerServer
	stage           string
	entered, exited chan struct{}
	calls           atomic.Int32
}

func (s *aggregationShutdownRPC) wait(ctx context.Context) error {
	s.calls.Add(1)
	close(s.entered)
	defer close(s.exited)
	<-ctx.Done()
	return ctx.Err()
}
func (s *aggregationShutdownRPC) GetFilerConfiguration(ctx context.Context, _ *filer_pb.GetFilerConfigurationRequest) (*filer_pb.GetFilerConfigurationResponse, error) {
	if s.stage == "configuration" {
		return nil, s.wait(ctx)
	}
	return &filer_pb.GetFilerConfigurationResponse{Signature: 7}, nil
}
func (s *aggregationShutdownRPC) SubscribeLocalMetadata(_ *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeLocalMetadataServer) error {
	return s.wait(stream.Context())
}
func (s *aggregationShutdownRPC) TraverseBfsMetadata(_ *filer_pb.TraverseBfsMetadataRequest, stream filer_pb.SeaweedFiler_TraverseBfsMetadataServer) error {
	return s.wait(stream.Context())
}

type aggregationBootstrapStore struct{ VirtualFilerStore }

func (*aggregationBootstrapStore) KvGet(context.Context, []byte) ([]byte, error) {
	return nil, ErrKvNotFound
}

func TestMetadataAggregationShutdownCancelsOwnedRPCs(t *testing.T) {
	for _, stage := range []string{"configuration", "subscription", "traversal"} {
		t.Run(stage, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			handler := &aggregationShutdownRPC{stage: stage, entered: make(chan struct{}), exited: make(chan struct{})}
			filer_pb.RegisterSeaweedFilerServer(server, handler)
			served := make(chan struct{})
			go func() { defer close(served); server.Serve(listener) }()
			t.Cleanup(func() { server.Stop(); listener.Close(); <-served })
			address := pb.NewServerAddress("127.0.0.1", 1, listener.Addr().(*net.TCPAddr).Port)
			f := &Filer{Signature: 7, UniqueFilerId: 123, Store: &aggregationBootstrapStore{}}
			if stage == "traversal" {
				f.Signature = 8
			}
			ma := NewMetaAggregator(f, address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			t.Cleanup(func() {
				ma.StopSubscriptions()
				ma.MetaLogBuffer.ShutdownLogBuffer()
				ma.MetaLogBuffer.WaitForShutdown()
			})
			add := &master_pb.ClusterNodeUpdate{Address: string(address), IsAdd: true}
			ma.OnPeerUpdate(add, time.Now())
			select {
			case <-handler.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("owned RPC did not start")
			}
			drained := make(chan struct{})
			go func() { server.GracefulStop(); close(drained) }()
			select {
			case <-drained:
				t.Fatal("gRPC drain returned with owned RPC active")
			case <-time.After(25 * time.Millisecond):
			}
			joined := make(chan struct{}, 2)
			for range 2 {
				go func() { ma.StopSubscriptions(); joined <- struct{}{} }()
			}
			for range 2 {
				select {
				case <-joined:
				case <-time.After(3 * time.Second):
					t.Fatal("aggregation did not join")
				}
			}
			select {
			case <-handler.exited:
			case <-time.After(3 * time.Second):
				t.Fatal("RPC handler did not exit")
			}
			select {
			case <-drained:
			case <-time.After(3 * time.Second):
				t.Fatal("gRPC drain remained blocked")
			}
			ma.OnPeerUpdate(add, time.Now())
			ma.peerChansLock.Lock()
			n := len(ma.peerChans)
			ma.peerChansLock.Unlock()
			if n != 0 || handler.calls.Load() != 1 {
				t.Fatalf("late subscription: peers=%d calls=%d", n, handler.calls.Load())
			}
		})
	}
}

func TestMetadataAggregationShutdownJoinsCallbacksAndFencesUpdates(t *testing.T) {
	ma := NewMetaAggregator(nil, "self", nil)
	defer func() { ma.MetaLogBuffer.ShutdownLogBuffer(); ma.MetaLogBuffer.WaitForShutdown() }()
	stop, entered, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	ma.peerChans["self"] = stop
	ma.workers.Add(1)
	go func() { defer ma.workers.Done(); <-stop; close(entered); <-release }()
	returned := make(chan struct{}, 2)
	for range 2 {
		go func() { ma.StopSubscriptions(); returned <- struct{}{} }()
	}
	<-entered
	ma.OnPeerUpdate(&master_pb.ClusterNodeUpdate{Address: "late", IsAdd: true}, time.Now())
	select {
	case <-returned:
		t.Fatal("shutdown abandoned an active callback")
	default:
	}
	unblock()
	for range 2 {
		select {
		case <-returned:
		case <-time.After(time.Second):
			t.Fatal("shutdown did not join")
		}
	}
	if len(ma.peerChans) != 0 {
		t.Fatal("late peer reopened aggregation")
	}
}

func TestMetadataAggregationCanceledBeforeStartupDoesNotDial(t *testing.T) {
	stop := make(chan struct{})
	close(stop)
	ma := &MetaAggregator{}
	done := make(chan struct{})
	go func() { ma.loopSubscribeToOneFiler(nil, "self", "invalid", time.Now(), stop); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled startup attempted a connection")
	}
}
