package server

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestTaskGatewayOpaqueStreamsAndCancellation(t *testing.T) {
	cancelled := make(chan struct{})
	received := make(chan wireFrame, 8)
	backend := grpc.NewServer(grpc.ForceServerCodec(gatewayCodec{}), grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		var req wireFrame
		if err := stream.RecvMsg(&req); err != nil {
			return err
		}
		received <- req
		method, _ := grpc.MethodFromServerStream(stream)
		stream.SetTrailer(metadata.Pairs("backend-trailer", "finished"))
		if err := stream.SendHeader(metadata.Pairs("backend-header", "ready")); err != nil {
			return err
		}
		if err := stream.SendMsg(&req); err != nil {
			return err
		}
		if method == "/lf.a2a.v1.A2AService/SubscribeToTask" {
			<-stream.Context().Done()
			close(cancelled)
			return stream.Context().Err()
		}
		if method == "/lf.a2a.v1.A2AService/SendStreamingMessage" {
			return stream.SendMsg(&req)
		}
		return nil
	}))
	listener := bufconn.Listen(1 << 20)
	go backend.Serve(listener)
	t.Cleanup(backend.Stop)
	upstream, err := grpc.NewClient("passthrough:///a2a", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { upstream.Close() })
	routed := make(chan string, 8)
	client, _, conn, _ := managedTransport(t, func(ctx context.Context, task *ax.Task) (context.Context, grpc.ClientConnInterface, error) {
		routed <- task.Metadata.Uid
		return ctx, upstream, nil
	})
	task := createManagedTask(t, client, "Service")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(ax.TargetTaskHeader, task.Metadata.Atespace+"/"+task.Metadata.Name, ax.TargetTaskUIDHeader, task.Metadata.Uid))
	// The gateway resumes the suspended Service before forwarding the first call.
	request := wireFrame{10, 3, 'a', 0, 255}
	for _, method := range []string{"SendMessage", "CancelTask", "SendStreamingMessage"} {
		stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, "/lf.a2a.v1.A2AService/"+method, grpc.ForceCodec(gatewayCodec{}))
		if err != nil {
			t.Fatal(err)
		}
		if err = stream.SendMsg(&request); err != nil {
			t.Fatal(err)
		}
		if err = stream.CloseSend(); err != nil {
			t.Fatal(err)
		}
		headers, err := stream.Header()
		if err != nil {
			t.Fatal(err)
		}
		if headers.Get("backend-header")[0] != "ready" {
			t.Fatal(headers)
		}
		count := 0
		for {
			var response wireFrame
			err = stream.RecvMsg(&response)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if string(response) != string(request) {
				t.Fatalf("payload changed: %v", response)
			}
			count++
		}
		expected := 1
		if method == "SendStreamingMessage" {
			expected = 2
		}
		if count != expected {
			t.Fatalf("got %d frames", count)
		}
		if stream.Trailer().Get("backend-trailer")[0] != "finished" {
			t.Fatal(stream.Trailer())
		}
		if uid := <-routed; uid != task.Metadata.Uid {
			t.Fatal("wrong runtime UID")
		}
		<-received
	}
	streamCtx, stop := context.WithCancel(ctx)
	defer stop()
	stream, err := conn.NewStream(streamCtx, &grpc.StreamDesc{ServerStreams: true}, "/lf.a2a.v1.A2AService/SubscribeToTask", grpc.ForceCodec(gatewayCodec{}))
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.SendMsg(&request); err != nil {
		t.Fatal(err)
	}
	stream.CloseSend()
	var response wireFrame
	if err = stream.RecvMsg(&response); err != nil {
		t.Fatal(err)
	}
	stop()
	select {
	case <-cancelled:
	case <-ctx.Done():
		t.Fatal("gateway did not cancel upstream")
	}
	<-routed
	<-received
	wrong := metadata.NewOutgoingContext(ctx, metadata.Pairs(ax.TargetTaskHeader, "test/"+task.Metadata.Name, ax.TargetTaskUIDHeader, "wrong"))
	err = conn.Invoke(wrong, "/lf.a2a.v1.A2AService/SendMessage", &request, &response, grpc.ForceCodec(gatewayCodec{}))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("wrong UID: %v", err)
	}
	err = conn.Invoke(ctx, "/lf.a2a.v1.A2AService/ExecuteUnknown", &request, &response, grpc.ForceCodec(gatewayCodec{}))
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("unknown method: %v", err)
	}
	select {
	case <-routed:
		t.Fatal("rejected call reached upstream")
	default:
	}
}
