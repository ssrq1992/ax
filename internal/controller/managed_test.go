package controller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type managedFake struct {
	ManagedBackend
	mu          sync.Mutex
	creates     int
	transitions int
	createError error
	enter       chan struct{}
	release     chan struct{}
	token       string
}

func (f *managedFake) EnsureGroup(_ context.Context, g *ax.TaskGroup, _ string) (string, int32, error) {
	return "pool-uid", g.Spec.GetReplicas(), nil
}
func (f *managedFake) GroupIdle(context.Context, *ax.TaskGroup) error           { return nil }
func (f *managedFake) DeleteGroup(context.Context, *ax.TaskGroup, string) error { return nil }
func (f *managedFake) Prepare(context.Context, *ax.PreparedRuntime, *ax.TaskGroup) (string, string, error) {
	return "template-uid", "Ready", nil
}
func (f *managedFake) DeleteRuntime(context.Context, *ax.PreparedRuntime, string) error { return nil }
func (f *managedFake) Create(_ context.Context, _ *ax.Task, _ *ax.PreparedRuntime, _ *ax.TaskCheckpoint, token string) (substrate.Observation, error) {
	f.mu.Lock()
	f.creates++
	f.token = token
	f.mu.Unlock()
	if f.enter != nil {
		close(f.enter)
		<-f.release
	}
	return substrate.Observation{UID: "instance-uid", Phase: "Suspended"}, f.createError
}
func (f *managedFake) Transition(_ context.Context, _ *ax.Task, uid, kind string) (substrate.Observation, error) {
	f.transitions++
	phase := map[string]string{"Resume": "Running", "Suspend": "Suspended", "Pause": "Paused", "Delete": "Deleted"}[kind]
	return substrate.Observation{UID: uid, Phase: phase}, nil
}
func managedFixture(t *testing.T) (*ManagedController, *managedFake, *ax.TaskGroup, *ax.PreparedRuntime, *ax.CreateTaskRequest) {
	t.Helper()
	m := &ManagedController{Store: memory.NewStore()}
	f := &managedFake{}
	m.Backend = f
	ctx := context.Background()
	g, err := m.CreateGroup(ctx, &ax.CreateTaskGroupRequest{RequestId: "create-group", Group: &ax.TaskGroup{Metadata: &ax.ObjectMeta{Name: "agents", Atespace: "test"}, Spec: &ax.TaskGroupSpec{SnapshotLocation: "gs://test/snapshots"}}})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := m.PrepareRuntime(ctx, &ax.PrepareRuntimeRequest{RequestId: "prepare", Metadata: &ax.ObjectMeta{Name: "runtime", Atespace: "test"}, Spec: &ax.PreparedRuntimeSpec{Kind: "Service", Image: "image@sha256:" + strings.Repeat("a", 64), GroupRef: ax.Ref(g.Metadata)}})
	if err != nil {
		t.Fatal(err)
	}
	req := &ax.CreateTaskRequest{RequestId: "create-task", Task: &ax.Task{Metadata: &ax.ObjectMeta{Name: "session", Atespace: "test"}, Spec: &ax.TaskSpec{GroupRef: ax.Ref(g.Metadata), PreparedRuntimeRef: ax.Ref(runtime.Metadata)}}}
	return m, f, g, runtime, req
}
func TestManagedCreateReceiptAndUID(t *testing.T) {
	m, f, g, r, req := managedFixture(t)
	ctx := context.Background()
	task, err := m.CreateTask(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := m.CreateTask(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Metadata.Uid != task.Metadata.Uid || f.creates != 1 {
		t.Fatal("retry created another instance")
	}
	changed := proto.Clone(req).(*ax.CreateTaskRequest)
	changed.Task.Spec.GroupRef.Uid = "another"
	if _, err = m.CreateTask(ctx, changed); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("changed request: %v", err)
	}
	wrong := ax.Ref(task.Metadata)
	wrong.Uid = "another"
	if _, err = m.Transition(ctx, wrong, "resume", "Resume"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("UID mismatch: %v", err)
	}
	if err = m.DeleteResource(ctx, "runtime", ax.Ref(r.Metadata), "release"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("runtime reference lost: %v", err)
	}
	if _, err = m.UpdateGroup(ctx, &ax.UpdateTaskGroupRequest{Ref: ax.Ref(g.Metadata), ExpectedVersion: g.Metadata.ResourceVersion, Replicas: 0}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unsafe shrink: %v", err)
	}
}
func TestManagedConcurrentCreateAndDeleteProtection(t *testing.T) {
	m, f, _, r, req := managedFixture(t)
	f.enter = make(chan struct{})
	f.release = make(chan struct{})
	ctx := context.Background()
	done := make(chan error, 1)
	go func() { _, err := m.CreateTask(ctx, req); done <- err }()
	<-f.enter
	if _, err := m.CreateTask(ctx, req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("pending operation replayed: %v", err)
	}
	if err := m.DeleteResource(ctx, "runtime", ax.Ref(r.Metadata), "release"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("pending create lost reference: %v", err)
	}
	close(f.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if f.creates != 1 {
		t.Fatalf("creates=%d", f.creates)
	}
}
func TestManagedUnknownResultNeverRetriesOrReleases(t *testing.T) {
	m, f, _, r, req := managedFixture(t)
	f.createError = status.Error(codes.DeadlineExceeded, "response lost")
	if _, err := m.CreateTask(context.Background(), req); status.Code(err) != codes.DeadlineExceeded {
		t.Fatal(err)
	}
	if _, err := m.CreateTask(context.Background(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	if err := m.DeleteResource(context.Background(), "runtime", ax.Ref(r.Metadata), "release"); status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	if f.creates != 1 {
		t.Fatal("uncertain operation was replayed")
	}
}

type failingCommit struct {
	store.ManagedStore
	calls  int
	failAt int
}

func (s *failingCommit) UpdateManaged(ctx context.Context, space string, fn func(store.ManagedRecords) error) error {
	s.calls++
	if s.calls == s.failAt {
		return errors.New("Redis unavailable")
	}
	return s.ManagedStore.UpdateManaged(ctx, space, fn)
}
func TestManagedFailedResultCommitRetainsClaim(t *testing.T) {
	m, f, _, _, req := managedFixture(t)
	wrapped := &failingCommit{ManagedStore: m.Store, failAt: 2}
	m.Store = wrapped
	if _, err := m.CreateTask(context.Background(), req); err == nil {
		t.Fatal("expected commit failure")
	}
	if _, err := m.CreateTask(context.Background(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	if f.creates != 1 {
		t.Fatal("failed commit allowed duplicate create")
	}
}
func TestManagedRuntimeCredentialRevocation(t *testing.T) {
	m, f, _, _, req := managedFixture(t)
	ctx := context.Background()
	task, err := m.CreateTask(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	auth := &ax.AuthenticateRuntimeRequest{Atespace: "test", Credential: f.token}
	if _, err = m.Authenticate(ctx, auth); status.Code(err) != codes.Unauthenticated {
		t.Fatal("suspended credential accepted")
	}
	task, err = m.Transition(ctx, ax.Ref(task.Metadata), "resume", "Resume")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := m.Authenticate(ctx, auth)
	if err != nil {
		t.Fatal(err)
	}
	if identity.TaskRef.Uid != task.Metadata.Uid {
		t.Fatal("wrong identity")
	}
	if _, err = m.Transition(ctx, ax.Ref(task.Metadata), "delete", "Delete"); err != nil {
		t.Fatal(err)
	}
	if _, err = m.Authenticate(ctx, auth); status.Code(err) != codes.Unauthenticated {
		t.Fatal("deleted credential accepted")
	}
	owns, err := m.OwnsTask(ctx, "test", task.Metadata.Name)
	if err != nil || !owns {
		t.Fatal("tombstone lost ownership routing")
	}
}
func (f *managedFake) VerifyPreparation(context.Context, *ax.PreparedRuntime, string, *ax.TaskCheckpoint, string) error {
	return nil
}
func (f *managedFake) ReadyWorkers(_ context.Context, g *ax.TaskGroup, _ string) (int32, error) {
	return g.Spec.GetReplicas(), nil
}

func TestManagedGroupRecreationFencesOldUIDAndRequests(t *testing.T) {
	m := &ManagedController{Store: memory.NewStore(), Backend: &managedFake{}}
	ctx := t.Context()
	req := &ax.CreateTaskGroupRequest{RequestId: "first", Group: &ax.TaskGroup{Metadata: &ax.ObjectMeta{Atespace: "test", Name: "group"}, Spec: &ax.TaskGroupSpec{SnapshotLocation: "gs://test"}}}
	first, err := m.CreateGroup(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	secondRequest := proto.Clone(req).(*ax.CreateTaskGroupRequest)
	secondRequest.RequestId = "second"
	if _, err = m.CreateGroup(ctx, secondRequest); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("existing group recreated: %v", err)
	}
	if err = m.DeleteResource(ctx, "group", ax.Ref(first.Metadata), "delete"); err != nil {
		t.Fatal(err)
	}
	second, err := m.CreateGroup(ctx, secondRequest)
	if err != nil {
		t.Fatal(err)
	}
	if second.Metadata.Uid == first.Metadata.Uid {
		t.Fatal("reused deleted group UID")
	}
	if _, err = m.UpdateGroup(ctx, &ax.UpdateTaskGroupRequest{Ref: ax.Ref(first.Metadata), ExpectedVersion: first.Metadata.ResourceVersion, Replicas: 3}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("old group reference accepted: %v", err)
	}
	if _, err = m.CreateGroup(ctx, req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("late creation accepted: %v", err)
	}
}
