package server

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/google/ax/internal/store"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type managedTLSKey struct{}

func (s *Server) authorizeManaged(ctx context.Context, space string) error {
	if s.managed == nil {
		return status.Error(codes.FailedPrecondition, "managed runtime is not configured")
	}
	if ax.ValidateName(space) != nil {
		return status.Error(codes.InvalidArgument, "atespace is required")
	}
	state, _ := ctx.Value(managedTLSKey{}).(*tls.ConnectionState)
	if state == nil {
		if p, ok := peer.FromContext(ctx); ok {
			if info, ok := p.AuthInfo.(credentials.TLSInfo); ok {
				state = &info.State
			}
		}
	}
	if state == nil || len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
		return status.Error(codes.Unauthenticated, "managed runtime requires verified mTLS")
	}
	for _, uri := range state.PeerCertificates[0].URIs {
		for _, allowed := range s.managedClients[uri.String()] {
			if allowed == space {
				return nil
			}
		}
	}
	return status.Error(codes.PermissionDenied, "client identity is not authorized for this atespace")
}
func (s *Server) managedTask(ctx context.Context, space, name string) (bool, error) {
	if s.managed == nil {
		if managedStore, ok := s.store.(store.ManagedStore); ok {
			records, err := managedStore.ReadManaged(ctx, space)
			if err != nil {
				return false, err
			}
			if _, exists := records["task/"+name]; exists {
				return true, status.Error(codes.FailedPrecondition, "managed runtime is disabled; refusing legacy ownership takeover")
			}
		}
		return false, nil
	}
	owns, err := s.managed.OwnsTask(ctx, space, name)
	if err != nil {
		return false, err
	}
	if !owns {
		return false, nil
	}
	return true, s.authorizeManaged(ctx, space)
}
func (s *Server) CreateTaskGroup(ctx context.Context, req *ax.CreateTaskGroupRequest) (*ax.TaskGroup, error) {
	if err := s.authorizeManaged(ctx, req.GetGroup().GetMetadata().GetAtespace()); err != nil {
		return nil, err
	}
	return s.managed.CreateGroup(ctx, req)
}
func (s *Server) GetTaskGroup(ctx context.Context, req *ax.GetTaskGroupRequest) (*ax.TaskGroup, error) {
	if err := s.authorizeManaged(ctx, req.GetAtespace()); err != nil {
		return nil, err
	}
	return s.managed.GetGroup(ctx, req.Atespace, req.Name)
}
func (s *Server) ListTaskGroups(ctx context.Context, req *ax.ListTaskGroupsRequest) (*ax.ListTaskGroupsResponse, error) {
	if err := s.authorizeManaged(ctx, req.GetAtespace()); err != nil {
		return nil, err
	}
	return s.managed.ListGroups(ctx, req)
}
func (s *Server) UpdateTaskGroup(ctx context.Context, req *ax.UpdateTaskGroupRequest) (*ax.TaskGroup, error) {
	if err := s.authorizeManaged(ctx, req.GetRef().GetAtespace()); err != nil {
		return nil, err
	}
	return s.managed.UpdateGroup(ctx, req)
}
func (s *Server) DeleteTaskGroup(ctx context.Context, req *ax.DeleteTaskGroupRequest) (*ax.DeleteTaskGroupResponse, error) {
	if err := s.authorizeManaged(ctx, req.GetRef().GetAtespace()); err != nil {
		return nil, err
	}
	if err := s.managed.DeleteResource(ctx, "group", req.Ref, req.OperationId); err != nil {
		return nil, err
	}
	return &ax.DeleteTaskGroupResponse{}, nil
}
func (s *Server) PrepareRuntime(ctx context.Context, req *ax.PrepareRuntimeRequest) (*ax.PreparedRuntime, error) {
	if err := s.authorizeManaged(ctx, req.GetMetadata().GetAtespace()); err != nil {
		return nil, err
	}
	return s.managed.PrepareRuntime(ctx, req)
}
func (s *Server) GetPreparedRuntime(ctx context.Context, req *ax.GetPreparedRuntimeRequest) (*ax.PreparedRuntime, error) {
	if err := s.authorizeManaged(ctx, req.GetRef().GetAtespace()); err != nil {
		return nil, err
	}
	return s.managed.GetRuntime(ctx, req.Ref)
}
func (s *Server) ReleasePreparedRuntime(ctx context.Context, req *ax.ReleasePreparedRuntimeRequest) (*ax.ReleasePreparedRuntimeResponse, error) {
	if err := s.authorizeManaged(ctx, req.GetRef().GetAtespace()); err != nil {
		return nil, err
	}
	if err := s.managed.DeleteResource(ctx, "runtime", req.Ref, req.OperationId); err != nil {
		return nil, err
	}
	return &ax.ReleasePreparedRuntimeResponse{}, nil
}
func (s *Server) PauseTask(ctx context.Context, req *ax.PauseTaskRequest) (*ax.Task, error) {
	if err := s.authorizeManaged(ctx, req.GetRef().GetAtespace()); err != nil {
		return nil, err
	}
	return s.managed.Transition(ctx, req.Ref, req.OperationId, "Pause")
}
func (s *Server) AuthenticateRuntime(ctx context.Context, req *ax.AuthenticateRuntimeRequest) (*ax.AuthenticateRuntimeResponse, error) {
	if err := s.authorizeManaged(ctx, req.GetAtespace()); err != nil {
		return nil, err
	}
	return s.managed.Authenticate(ctx, req)
}
func (s *Server) watchManaged(req *ax.WatchTaskRequest, stream grpc.ServerStreamingServer[ax.WatchTaskResponse]) error {
	space := req.Atespace
	if space == "" {
		space = "default"
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var version uint64
	for {
		task, err := s.managed.GetTask(stream.Context(), space, req.Name)
		if err != nil {
			return err
		}
		if task.Metadata.ResourceVersion != version {
			action := "MODIFIED"
			if version == 0 {
				action = "INITIAL"
			}
			if err = stream.Send(&ax.WatchTaskResponse{Task: task, Action: action}); err != nil {
				return err
			}
			version = task.Metadata.ResourceVersion
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
		}
	}
}
func (s *Server) CreateTaskCheckpoint(ctx context.Context, req *ax.CreateTaskCheckpointRequest) (*ax.TaskCheckpoint, error) {
	if err := s.authorizeManaged(ctx, req.GetTaskRef().GetAtespace()); err != nil {
		return nil, err
	}
	return s.managed.CreateCheckpoint(ctx, req)
}
func (s *Server) GetTaskCheckpoint(ctx context.Context, req *ax.GetTaskCheckpointRequest) (*ax.TaskCheckpoint, error) {
	if err := s.authorizeManaged(ctx, req.GetRef().GetAtespace()); err != nil {
		return nil, err
	}
	return s.managed.GetCheckpoint(ctx, req.Ref)
}
func (s *Server) DeleteTaskCheckpoint(ctx context.Context, req *ax.DeleteTaskCheckpointRequest) (*ax.DeleteTaskCheckpointResponse, error) {
	if err := s.authorizeManaged(ctx, req.GetRef().GetAtespace()); err != nil {
		return nil, err
	}
	if err := s.managed.DeleteResource(ctx, "checkpoint", req.Ref, req.OperationId); err != nil {
		return nil, err
	}
	return &ax.DeleteTaskCheckpointResponse{}, nil
}
