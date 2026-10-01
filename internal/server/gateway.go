package server

import (
	"context"
	"fmt"
	"io"
	"strings"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// wireFrame keeps A2A payloads opaque. Routing is only by authenticated AX refs,
// never by fields in the business payload or caller-supplied backend headers.
type wireFrame []byte
type gatewayCodec struct{}

func (gatewayCodec) Name() string { return "proto" }
func (gatewayCodec) Marshal(v any) ([]byte, error) {
	if frame, ok := v.(*wireFrame); ok {
		return []byte(*frame), nil
	}
	m, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("invalid protobuf message")
	}
	return proto.Marshal(m)
}
func (gatewayCodec) Unmarshal(raw []byte, v any) error {
	if frame, ok := v.(*wireFrame); ok {
		*frame = append((*frame)[:0], raw...)
		return nil
	}
	m, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("invalid protobuf message")
	}
	return proto.Unmarshal(raw, m)
}

var a2aMethods = map[string]bool{
	"/lf.a2a.v1.A2AService/SendMessage":          false,
	"/lf.a2a.v1.A2AService/SendStreamingMessage": true,
	"/lf.a2a.v1.A2AService/GetTask":              false,
	"/lf.a2a.v1.A2AService/ListTasks":            false,
	"/lf.a2a.v1.A2AService/CancelTask":           false,
	"/lf.a2a.v1.A2AService/SubscribeToTask":      true,
	"/lf.a2a.v1.A2AService/GetExtendedAgentCard": false,
}

func (s *Server) gateway(_ any, stream grpc.ServerStream) error {
	method, ok := grpc.MethodFromServerStream(stream)
	if !ok {
		return status.Error(codes.Unimplemented, "unknown method")
	}
	streaming, ok := a2aMethods[method]
	if !ok {
		return status.Error(codes.Unimplemented, "method is not exposed by TaskGateway")
	}
	targets := metadata.ValueFromIncomingContext(stream.Context(), ax.TargetTaskHeader)
	uids := metadata.ValueFromIncomingContext(stream.Context(), ax.TargetTaskUIDHeader)
	if len(targets) != 1 || len(uids) != 1 {
		return status.Error(codes.InvalidArgument, "one AX task target and UID are required")
	}
	parts := strings.Split(targets[0], "/")
	if len(parts) != 2 {
		return status.Error(codes.InvalidArgument, "invalid AX task target")
	}
	ref := &ax.ResourceRef{Atespace: parts[0], Name: parts[1], Uid: uids[0]}
	if err := ax.ValidateRef(ref, true); err != nil {
		return invalidExecution(err)
	}
	if err := s.authorizeManaged(stream.Context(), ref.Atespace); err != nil {
		return err
	}
	var request wireFrame
	if err := stream.RecvMsg(&request); err != nil {
		return err
	}
	var extra wireFrame
	if err := stream.RecvMsg(&extra); err != io.EOF {
		if err == nil {
			return status.Error(codes.InvalidArgument, "A2A requires one request message")
		}
		return err
	}
	task, err := s.managed.AdmitService(stream.Context(), ref)
	if err != nil {
		return err
	}
	if s.runtimeDialer == nil {
		return status.Error(codes.FailedPrecondition, "runtime transport is not configured")
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	ctx, conn, err := s.runtimeDialer(ctx, task)
	if err != nil {
		return err
	}
	upstream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: streaming}, method, grpc.ForceCodec(gatewayCodec{}))
	if err != nil {
		return err
	}
	// No retry after forwarding any payload: transport failure may mean execution.
	if err = upstream.SendMsg(&request); err != nil {
		return err
	}
	if err = upstream.CloseSend(); err != nil {
		return err
	}
	headers, err := upstream.Header()
	if err != nil {
		return err
	}
	if err = stream.SendHeader(headers); err != nil {
		return err
	}
	defer func() { stream.SetTrailer(upstream.Trailer()) }()
	for {
		var response wireFrame
		err = upstream.RecvMsg(&response)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = stream.SendMsg(&response); err != nil {
			return err
		}
	}
}
