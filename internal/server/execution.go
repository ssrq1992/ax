package server

import (
	"context"
	"errors"
	"io"
	"math"
	"time"

	guest "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const maxExecutionFileBytes = 64 << 20

type executionServer struct {
	ax.UnimplementedTaskExecutionServiceServer
	server *Server
}

func (e *executionServer) connect(ctx context.Context, ref *ax.ResourceRef) (context.Context, grpc.ClientConnInterface, error) {
	if err := ax.ValidateRef(ref, true); err != nil {
		return nil, nil, invalidExecution(err)
	}
	if err := e.server.authorizeManaged(ctx, ref.Atespace); err != nil {
		return nil, nil, err
	}
	task, err := e.server.managed.Admit(ctx, ref, "Sandbox")
	if err != nil {
		return nil, nil, err
	}
	if e.server.runtimeDialer == nil {
		return nil, nil, status.Error(codes.FailedPrecondition, "runtime transport is not configured")
	}
	return e.server.runtimeDialer(ctx, task)
}
func invalidExecution(err error) error { return status.Error(codes.InvalidArgument, err.Error()) }
func process(p *guest.Process) *ax.Process {
	state := ax.ProcessStatus_PROCESS_STATUS_UNSPECIFIED
	if p.State == guest.ProcessState_PROCESS_STATE_RUNNING {
		state = ax.ProcessStatus_PROCESS_STATUS_RUNNING
	} else if p.State == guest.ProcessState_PROCESS_STATE_EXITED {
		state = ax.ProcessStatus_PROCESS_STATUS_COMPLETED
		if p.ExitCode != 0 {
			state = ax.ProcessStatus_PROCESS_STATUS_FAILED
		}
		if p.ExitCode >= 128 {
			state = ax.ProcessStatus_PROCESS_STATUS_TERMINATED
		}
	}
	return &ax.Process{ProcessId: p.ProcessId, Command: p.Command, Status: state, ExitCode: p.ExitCode, StartedAt: p.StartedAt, FinishedAt: p.FinishedAt}
}
func (e *executionServer) StartProcess(ctx context.Context, req *ax.StartProcessRequest) (*ax.StartProcessResponse, error) {
	if req == nil || len(req.Command) == 0 || req.TimeoutMs < 0 || req.TimeoutMs > math.MaxInt64/int64(time.Millisecond) {
		return nil, status.Error(codes.InvalidArgument, "valid command and timeout are required")
	}
	ctx, conn, err := e.connect(ctx, req.TaskRef)
	if err != nil {
		return nil, err
	}
	result, err := guest.NewProcessServiceClient(conn).StartProcess(ctx, &guest.StartProcessRequest{Command: req.Command, Cwd: req.Cwd, Env: req.Env, Timeout: durationpb.New(time.Duration(req.TimeoutMs) * time.Millisecond)})
	if err != nil {
		return nil, err
	}
	return &ax.StartProcessResponse{ProcessId: result.ProcessId}, nil
}
func (e *executionServer) GetProcess(ctx context.Context, req *ax.GetProcessRequest) (*ax.Process, error) {
	if req.GetProcessId() == "" {
		return nil, status.Error(codes.InvalidArgument, "process ID is required")
	}
	ctx, conn, err := e.connect(ctx, req.TaskRef)
	if err != nil {
		return nil, err
	}
	result, err := guest.NewProcessServiceClient(conn).GetProcess(ctx, &guest.GetProcessRequest{ProcessId: req.ProcessId})
	if err != nil {
		return nil, err
	}
	return process(result), nil
}
func (e *executionServer) KillProcess(ctx context.Context, req *ax.KillProcessRequest) (*ax.KillProcessResponse, error) {
	if req.GetProcessId() == "" {
		return nil, status.Error(codes.InvalidArgument, "process ID is required")
	}
	ctx, conn, err := e.connect(ctx, req.TaskRef)
	if err != nil {
		return nil, err
	}
	result, err := guest.NewProcessServiceClient(conn).SignalProcess(ctx, &guest.SignalProcessRequest{ProcessId: req.ProcessId, Signal: guest.Signal_SIGNAL_KILL})
	if err != nil {
		return nil, err
	}
	for result.State == guest.ProcessState_PROCESS_STATE_RUNNING {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
		result, err = guest.NewProcessServiceClient(conn).GetProcess(ctx, &guest.GetProcessRequest{ProcessId: req.ProcessId})
		if err != nil {
			return nil, err
		}
	}
	return &ax.KillProcessResponse{ExitCode: result.ExitCode}, nil
}
func (e *executionServer) StreamProcessOutputs(req *ax.StreamProcessOutputsRequest, stream grpc.ServerStreamingServer[ax.OutputChunk]) error {
	if req.GetProcessId() == "" || req.StdoutOffset < 0 || req.StderrOffset < 0 {
		return status.Error(codes.InvalidArgument, "process ID and non-negative offsets are required")
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	ctx, conn, err := e.connect(ctx, req.TaskRef)
	if err != nil {
		return err
	}
	upstream, err := guest.NewProcessServiceClient(conn).StreamProcessOutput(ctx, &guest.StreamProcessOutputRequest{ProcessId: req.ProcessId, StdoutOffset: req.StdoutOffset, StderrOffset: req.StderrOffset, Follow: req.Follow})
	if err != nil {
		return err
	}
	stdout, stderr := req.StdoutOffset, req.StderrOffset
	for {
		out, err := upstream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		chunk := &ax.OutputChunk{}
		switch content := out.Output.(type) {
		case *guest.ProcessOutput_Stdout:
			chunk.Source = ax.OutputSource_OUTPUT_SOURCE_STDOUT
			chunk.Data = content.Stdout
			stdout += int64(len(chunk.Data))
			chunk.NextOffset = stdout
		case *guest.ProcessOutput_Stderr:
			chunk.Source = ax.OutputSource_OUTPUT_SOURCE_STDERR
			chunk.Data = content.Stderr
			stderr += int64(len(chunk.Data))
			chunk.NextOffset = stderr
		case *guest.ProcessOutput_Exit:
			chunk.Exit = process(content.Exit)
		default:
			return status.Error(codes.DataLoss, "unsupported Guest output")
		}
		if err = stream.Send(chunk); err != nil {
			return err
		}
	}
}
func (e *executionServer) ReadFile(req *ax.ReadFileRequest, stream grpc.ServerStreamingServer[ax.FileChunk]) error {
	if req.GetPath() == "" {
		return status.Error(codes.InvalidArgument, "file path is required")
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	ctx, conn, err := e.connect(ctx, req.TaskRef)
	if err != nil {
		return err
	}
	upstream, err := guest.NewFileSystemServiceClient(conn).ReadFile(ctx, &guest.ReadFileRequest{Path: req.Path})
	if err != nil {
		return err
	}
	total := 0
	for {
		chunk, err := upstream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		total += len(chunk.Chunk)
		if total > maxExecutionFileBytes {
			return status.Error(codes.ResourceExhausted, "file exceeds 64 MiB")
		}
		if err = stream.Send(&ax.FileChunk{Data: chunk.Chunk}); err != nil {
			return err
		}
	}
}
func (e *executionServer) WriteFile(stream grpc.ClientStreamingServer[ax.WriteFileRequest, ax.WriteFileResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "first file chunk is required")
		}
		return err
	}
	if first.Path == "" || first.Mode&^uint32(0777) != 0 {
		return status.Error(codes.InvalidArgument, "file path and permission mode are invalid")
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	ctx, conn, err := e.connect(ctx, first.TaskRef)
	if err != nil {
		return err
	}
	upstream, err := guest.NewFileSystemServiceClient(conn).WriteFile(ctx)
	if err != nil {
		return err
	}
	total := int64(0)
	chunk := first
	for {
		if chunk.TaskRef != nil && !proto.Equal(chunk.TaskRef, first.TaskRef) {
			return status.Error(codes.InvalidArgument, "task identity changed during file upload")
		}
		if (chunk.Path != "" && chunk.Path != first.Path) || (chunk.Mode != 0 && chunk.Mode != first.Mode) {
			return status.Error(codes.InvalidArgument, "file metadata changed during upload")
		}
		total += int64(len(chunk.Chunk))
		if total > maxExecutionFileBytes {
			return status.Error(codes.ResourceExhausted, "file exceeds 64 MiB")
		}
		if err = upstream.Send(&guest.WriteFileRequest{Path: first.Path, Mode: first.Mode, Chunk: chunk.Chunk}); err != nil {
			return err
		}
		chunk, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	result, err := upstream.CloseAndRecv()
	if err != nil {
		return err
	}
	if result.BytesWritten != total {
		return status.Error(codes.DataLoss, "Guest write count differs")
	}
	return stream.SendAndClose(&ax.WriteFileResponse{BytesWritten: total})
}
