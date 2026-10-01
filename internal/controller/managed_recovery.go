package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/substrate"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Recovery is an offline operator procedure, never an RPC, lease timeout or
// reconciler retry. Fence all AX executors and drain backend requests first.
// The fingerprint binds approval to the exact durable intent, including owner.
// Inspect does not expose credentials or private backend addresses.
type Recovery struct {
	Atespace    string `json:"atespace"`
	Key         string `json:"key"`
	UID         string `json:"uid"`
	OperationID string `json:"operationID"`
	Kind        string `json:"kind"`
	Fingerprint string `json:"fingerprint"`
}

func recoveryFingerprint(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func (m *ManagedController) InspectRecovery(ctx context.Context, space string) ([]Recovery, error) {
	if err := ax.ValidateName(space); err != nil {
		return nil, invalid(err)
	}
	rs, err := m.Store.ReadManaged(ctx, space)
	if err != nil {
		return nil, err
	}
	result := []Recovery{}
	for key, raw := range rs {
		r, err := record(rs, key)
		if err != nil {
			return nil, err
		}
		if r.Deleted || r.Operation == nil || r.Operation.Phase == "Succeeded" || strings.HasPrefix(key, "deleted-") {
			continue
		}
		// Checkpoint creation owns both records and is recovered as one transaction.
		if r.Task != nil && r.Operation.Kind == "Checkpoint" {
			continue
		}
		result = append(result, Recovery{Atespace: space, Key: key, UID: metadataOf(r).Uid, OperationID: r.Operation.ID, Kind: r.Operation.Kind, Fingerprint: recoveryFingerprint(raw)})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}
func (m *ManagedController) RecoverManaged(ctx context.Context, plan Recovery, executorsFenced bool) error {
	if !executorsFenced {
		return status.Error(codes.FailedPrecondition, "offline recovery requires fenced executors and drained backend requests")
	}
	backend, ok := m.Backend.(interface {
		RecoverObserved(context.Context, substrate.RecoveryInput) (substrate.RecoveryResult, error)
	})
	if !ok {
		return status.Error(codes.FailedPrecondition, "backend has no recovery verifier")
	}
	if err := ax.ValidateName(plan.Atespace); err != nil {
		return invalid(err)
	}
	rs, err := m.Store.ReadManaged(ctx, plan.Atespace)
	if err != nil {
		return err
	}
	r, err := record(rs, plan.Key)
	if err != nil {
		return err
	}
	if r.Operation == nil || r.Operation.Phase == "Succeeded" || r.Deleted || metadataOf(r).Uid != plan.UID || r.Operation.ID != plan.OperationID || r.Operation.Kind != plan.Kind || plan.Fingerprint != recoveryFingerprint(rs[plan.Key]) {
		return status.Error(codes.Aborted, "recovery plan no longer matches the unresolved intent")
	}
	in := substrate.RecoveryInput{Kind: plan.Kind, BackendUID: r.BackendUID, CredentialHash: r.CredentialHash, Group: r.Group, Runtime: r.Runtime, Task: r.Task, Checkpoint: r.Checkpoint}
	pins := map[string]string{plan.Key: plan.Fingerprint}
	load := func(kind string, ref *ax.ResourceRef) (*managedRecord, error) {
		v, e := reference(rs, kind, ref)
		if e == nil {
			pins[kind+"/"+ref.Name] = recoveryFingerprint(rs[kind+"/"+ref.Name])
		}
		return v, e
	}
	if r.Runtime != nil && plan.Kind != "Delete" {
		g, e := load("group", r.Runtime.Spec.GroupRef)
		if e != nil {
			return e
		}
		in.Group = g.Group
	}
	if r.Task != nil && plan.Kind == "Create" {
		runtime, e := load("runtime", r.Task.Spec.PreparedRuntimeRef)
		if e != nil {
			return e
		}
		in.Runtime = runtime.Runtime
		in.RuntimeUID = runtime.BackendUID
		if ref := r.Task.Spec.RestoreFrom; ref != nil {
			cp, e := load("checkpoint", ref)
			if e != nil {
				return e
			}
			in.Checkpoint = cp.Checkpoint
			in.CheckpointUID = cp.BackendUID
		}
	}
	var sourceKey string
	if r.Checkpoint != nil && plan.Kind == "Checkpoint" {
		sourceKey = "task/" + r.Checkpoint.SourceTask.Name
		source, e := record(rs, sourceKey)
		if e != nil {
			return e
		}
		if source.Task == nil || source.Operation == nil || source.Operation.Owner != r.Operation.Owner || source.Operation.Kind != "Checkpoint" || metadataOf(source).Uid != r.Checkpoint.SourceTask.Uid {
			return status.Error(codes.DataLoss, "checkpoint source ownership differs")
		}
		pins[sourceKey] = recoveryFingerprint(rs[sourceKey])
		in.Task = source.Task
		in.TaskUID = source.BackendUID
		runtime, e := load("runtime", r.Checkpoint.RuntimeRef)
		if e != nil {
			return e
		}
		in.Runtime = runtime.Runtime
		in.RuntimeUID = runtime.BackendUID
	}
	evidence, err := backend.RecoverObserved(ctx, in)
	if err != nil {
		return fmt.Errorf("unresolved operation retained: %w", err)
	}
	if evidence.BackendUID == "" {
		return status.Error(codes.FailedPrecondition, "recovery returned no bound identity")
	}
	return m.Store.UpdateManaged(ctx, plan.Atespace, func(current store.ManagedRecords) error {
		for key, fingerprint := range pins {
			if recoveryFingerprint(current[key]) != fingerprint {
				return status.Error(codes.Aborted, "resource or reference changed during recovery")
			}
		}
		saved, e := record(current, plan.Key)
		if e != nil {
			return e
		}
		// Only after the operator's fence and backend proof may an Uncertain record
		// be completed. complete's ordinary owner checks remain unchanged.
		saved.Operation.Phase = "Issued"
		if e = complete(saved, saved.Operation.Owner, nil); e != nil {
			return e
		}
		saved.BackendUID = evidence.BackendUID
		switch {
		case plan.Kind == "Delete":
			saved.Deleted = true
			if saved.Task != nil {
				saved.CredentialHash = ""
				applyObservation(saved, evidence.Task, nil)
			}
		case saved.Group != nil:
			saved.Group.Status = &ax.TaskGroupStatus{Phase: evidence.Phase, ReadyWorkers: evidence.ReadyWorkers}
		case saved.Runtime != nil:
			saved.Runtime.Phase = evidence.Phase
			saved.Runtime.Message = ""
		case saved.Task != nil:
			applyObservation(saved, evidence.Task, nil)
		case saved.Checkpoint != nil:
			saved.Checkpoint.Phase = "Ready"
			saved.Checkpoint.Message = ""
		}
		if sourceKey != "" {
			source, e := record(current, sourceKey)
			if e != nil {
				return e
			}
			source.Operation.Phase = "Issued"
			if e = complete(source, source.Operation.Owner, nil); e != nil {
				return e
			}
			if e = put(current, sourceKey, source); e != nil {
				return e
			}
		}
		return put(current, plan.Key, saved)
	})
}
