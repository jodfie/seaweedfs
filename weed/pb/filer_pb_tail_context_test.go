package pb

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type ownedFollowServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	entered chan struct{}
	exited  chan struct{}
}

func (s *ownedFollowServer) SubscribeMetadata(_ *filer_pb.SubscribeMetadataRequest, stream grpc.ServerStreamingServer[filer_pb.SubscribeMetadataResponse]) error {
	close(s.entered)
	defer close(s.exited)
	<-stream.Context().Done()
	return stream.Context().Err()
}

type ownedFollowClient struct {
	filer_pb.FilerClient
	client filer_pb.SeaweedFilerClient
	calls  int
}

func (c *ownedFollowClient) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	c.calls++
	return fn(c.client)
}
func TestOwnedMetadataContextUnblocksActualGrpcGracefulStop(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	handler := &ownedFollowServer{entered: make(chan struct{}), exited: make(chan struct{})}
	filer_pb.RegisterSeaweedFilerServer(server, handler)
	served := make(chan struct{})
	go func() { defer close(served); server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); listener.Close(); <-served })
	conn, err := grpc.NewClient("passthrough:///owned-filer", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	follower := &ownedFollowClient{client: filer_pb.NewSeaweedFilerClient(conn)}
	followed := make(chan error, 1)
	go func() {
		followed <- WithFilerClientFollowMetadataContext(ctx, follower, &MetadataFollowOption{ClientName: "s3"}, func(*filer_pb.SubscribeMetadataResponse) error { return nil })
	}()
	select {
	case <-handler.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("subscription did not start")
	}
	stopped := make(chan struct{})
	go func() { server.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("drain returned while owned stream remained active")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-followed:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("want canceled stream, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("follower did not join")
	}
	select {
	case <-handler.exited:
	case <-time.After(3 * time.Second):
		t.Fatal("server handler did not join")
	}
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("graceful stop did not join")
	}
}
func TestOwnedMetadataContextAlreadyCanceledDoesNotDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &ownedFollowClient{}
	err := WithFilerClientFollowMetadataContext(ctx, c, &MetadataFollowOption{}, func(*filer_pb.SubscribeMetadataResponse) error { return nil })
	if !errors.Is(err, context.Canceled) || c.calls != 0 {
		t.Fatalf("err=%v calls=%d", err, c.calls)
	}
}
func TestOwnedMetadataContextCancellationDoesNotAdvanceFailedEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	option := &MetadataFollowOption{StartTsNs: 10, EventErrorType: RetryForeverOnError}
	client := &fakeFilerClient{stream: &fakeSubscribeStream{responses: []*filer_pb.SubscribeMetadataResponse{{TsNs: 20, EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "synthetic"}}}}}}
	attempts := 0
	err := makeSubscribeMetadataFuncContext(ctx, option, func(*filer_pb.SubscribeMetadataResponse) error {
		attempts++
		cancel()
		return errors.New("synthetic failure")
	})(client)
	if !errors.Is(err, context.Canceled) || option.StartTsNs != 10 || attempts != 1 {
		t.Fatalf("err=%v cursor=%d attempts=%d", err, option.StartTsNs, attempts)
	}
}
