package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/substrate"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type recoveringBackend struct {
	*managedFake
	evidence substrate.RecoveryResult
	err      error
	inspect  func()
}

func (b *recoveringBackend) RecoverObserved(context.Context, substrate.RecoveryInput) (substrate.RecoveryResult, error) {
	if b.inspect != nil {
		b.inspect()
	}
	return b.evidence, b.err
}

func TestManagedRecoveryRequiresFenceAndExactIntent(t *testing.T) {
	m, f, _, _, req := managedFixture(t)
	f.createError = errors.New("response lost")
	if _, err := m.CreateTask(t.Context(), req); err == nil {
		t.Fatal("expected response loss")
	}
	backend := &recoveringBackend{managedFake: f, evidence: substrate.RecoveryResult{BackendUID: "instance-uid", Task: substrate.Observation{UID: "instance-uid", Phase: "Suspended"}}}
	m.Backend = backend
	plans, err := m.InspectRecovery(t.Context(), "test")
	if err != nil || len(plans) != 1 {
		t.Fatalf("plans=%v error=%v", plans, err)
	}
	if err = m.RecoverManaged(t.Context(), plans[0], false); status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	stale := plans[0]
	stale.UID = "wrong"
	if err = m.RecoverManaged(t.Context(), stale, true); status.Code(err) != codes.Aborted {
		t.Fatal(err)
	}
	backend.err = errors.New("backend not settled")
	if err = m.RecoverManaged(t.Context(), plans[0], true); err == nil {
		t.Fatal("uncertain evidence accepted")
	}
	pending, _ := m.InspectRecovery(t.Context(), "test")
	if len(pending) != 1 || pending[0] != plans[0] {
		t.Fatal("failed inspection changed ownership")
	}
	backend.err = nil
	if err = m.RecoverManaged(t.Context(), plans[0], true); err != nil {
		t.Fatal(err)
	}
	task, err := m.CreateTask(t.Context(), req)
	if err != nil || task.Status.RuntimeStatus.Phase != "Suspended" || f.creates != 1 {
		t.Fatalf("replay=%v error=%v creates=%d", task, err, f.creates)
	}
	if err = m.RecoverManaged(t.Context(), plans[0], true); status.Code(err) != codes.Aborted {
		t.Fatal("stale recovery accepted", err)
	}
}
func TestManagedRecoveryCASRejectsChangedReference(t *testing.T) {
	m, f, _, _, req := managedFixture(t)
	f.createError = errors.New("lost")
	_, _ = m.CreateTask(t.Context(), req)
	plans, err := m.InspectRecovery(t.Context(), "test")
	if err != nil {
		t.Fatal(err)
	}
	m.Backend = &recoveringBackend{managedFake: f, evidence: substrate.RecoveryResult{BackendUID: "instance-uid", Task: substrate.Observation{UID: "instance-uid", Phase: "Suspended"}}, inspect: func() {
		err := m.Store.UpdateManaged(t.Context(), "test", func(rs store.ManagedRecords) error {
			r, e := record(rs, "runtime/runtime")
			if e != nil {
				return e
			}
			r.Runtime.Metadata.ResourceVersion++
			return put(rs, "runtime/runtime", r)
		})
		if err != nil {
			t.Fatal(err)
		}
	}}
	if err = m.RecoverManaged(t.Context(), plans[0], true); status.Code(err) != codes.Aborted {
		t.Fatal(err)
	}
	pending, _ := m.InspectRecovery(t.Context(), "test")
	if len(pending) != 1 {
		t.Fatal("CAS failure cleared owner")
	}
}

type checkpointRecoveryBackend struct{ *recoveringBackend }

func (b *checkpointRecoveryBackend) Checkpoint(context.Context, *ax.Task, string, *ax.TaskCheckpoint) (string, error) {
	return "checkpoint-backend-uid", errors.New("checkpoint response lost")
}
func TestManagedRecoveryCompletesCheckpointAndSourceAtomically(t *testing.T) {
	m, f, _, _, req := managedFixture(t)
	task, err := m.CreateTask(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	err = m.Store.UpdateManaged(t.Context(), "test", func(rs store.ManagedRecords) error {
		r, e := record(rs, "task/session")
		if e != nil {
			return e
		}
		r.Task.Status.RuntimeStatus.Restorable = true
		r.Task.Status.RuntimeStatus.BoundaryRef = "boundary"
		return put(rs, "task/session", r)
	})
	if err != nil {
		t.Fatal(err)
	}
	b := &checkpointRecoveryBackend{&recoveringBackend{managedFake: f, evidence: substrate.RecoveryResult{BackendUID: "checkpoint-backend-uid", Phase: "Ready"}}}
	m.Backend = b
	request := &ax.CreateTaskCheckpointRequest{TaskRef: ax.Ref(task.Metadata), Name: "checkpoint", RequestId: "save", ExpectedBoundaryRef: "boundary"}
	if _, err = m.CreateCheckpoint(t.Context(), request); err == nil {
		t.Fatal("lost reply expected")
	}
	plans, err := m.InspectRecovery(t.Context(), "test")
	if err != nil || len(plans) != 1 || plans[0].Key != "checkpoint/checkpoint" {
		t.Fatalf("plans=%v err=%v", plans, err)
	}
	original := m.Store
	m.Store = &failingCommit{ManagedStore: original, failAt: 1}
	if err = m.RecoverManaged(t.Context(), plans[0], true); err == nil {
		t.Fatal("commit failure expected")
	}
	m.Store = original
	for _, key := range []string{"checkpoint/checkpoint", "task/session"} {
		r, e := m.read(t.Context(), "test", key)
		if e != nil || r.Operation.Phase != "Uncertain" {
			t.Fatalf("partial unlock %s: %v", key, e)
		}
	}
	if err = m.RecoverManaged(t.Context(), plans[0], true); err != nil {
		t.Fatal(err)
	}
	cp, err := m.CreateCheckpoint(t.Context(), request)
	if err != nil || cp.Phase != "Ready" {
		t.Fatalf("checkpoint replay=%v %v", cp, err)
	}
	if _, err = m.Transition(t.Context(), ax.Ref(task.Metadata), "resume-after-recovery", "Resume"); err != nil {
		t.Fatal("source remained locked", err)
	}
}
