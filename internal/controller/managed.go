package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/substrate"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ManagedBackend is intentionally private to the control plane. Neither its
// observations nor backend identities are part of the public AX contract.
type ManagedBackend interface {
	ReadyWorkers(context.Context, *ax.TaskGroup, string) (int32, error)
	VerifyPreparation(context.Context, *ax.PreparedRuntime, string, *ax.TaskCheckpoint, string) error
	Checkpoint(context.Context, *ax.Task, string, *ax.TaskCheckpoint) (string, error)
	DeleteCheckpoint(context.Context, *ax.TaskCheckpoint, string) error
	EnsureGroup(context.Context, *ax.TaskGroup, string) (string, int32, error)
	GroupIdle(context.Context, *ax.TaskGroup) error
	DeleteGroup(context.Context, *ax.TaskGroup, string) error
	Prepare(context.Context, *ax.PreparedRuntime, *ax.TaskGroup) (string, string, error)
	DeleteRuntime(context.Context, *ax.PreparedRuntime, string) error
	ObserveRuntime(context.Context, *ax.PreparedRuntime, string) (string, error)
	Create(context.Context, *ax.Task, *ax.PreparedRuntime, *ax.TaskCheckpoint, string) (substrate.Observation, error)
	Observe(context.Context, *ax.Task) (substrate.Observation, error)
	Transition(context.Context, *ax.Task, string, string) (substrate.Observation, error)
}

type ManagedController struct {
	Store   store.ManagedStore
	Backend ManagedBackend
}

// Operation ownership never expires. An unknown RPC result or failed commit
// retains its claim until an explicit, backend-aware recovery resolves it.
type managedOperation struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	Digest string `json:"digest"`
	Owner  string `json:"owner"`
	Phase  string `json:"phase"` // Issued, Succeeded, Uncertain
}
type managedRecord struct {
	Group          *ax.TaskGroup       `json:"group,omitempty"`
	Runtime        *ax.PreparedRuntime `json:"runtime,omitempty"`
	Task           *ax.Task            `json:"task,omitempty"`
	Checkpoint     *ax.TaskCheckpoint  `json:"checkpoint,omitempty"`
	BackendUID     string              `json:"backendUID,omitempty"`
	CredentialHash string              `json:"credentialHash,omitempty"`
	Deleted        bool                `json:"deleted,omitempty"`
	Operation      *managedOperation   `json:"operation,omitempty"`
	Receipts       map[string]string   `json:"receipts,omitempty"`
}

func record(records store.ManagedRecords, key string) (*managedRecord, error) {
	raw, ok := records[key]
	if !ok {
		return nil, status.Error(codes.NotFound, "AX resource not found")
	}
	var r managedRecord
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, status.Error(codes.DataLoss, "invalid managed resource record")
	}
	return &r, nil
}
func put(records store.ManagedRecords, key string, r *managedRecord) error {
	raw, err := json.Marshal(r)
	if err == nil {
		records[key] = raw
	}
	return err
}
func uid() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func digest(m proto.Message) string {
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func metadata(in *ax.ObjectMeta) *ax.ObjectMeta {
	return &ax.ObjectMeta{Name: in.Name, Atespace: in.Atespace, Uid: uid(), CreationTimestamp: timestamppb.Now(), ResourceVersion: 1}
}
func metadataOf(r *managedRecord) *ax.ObjectMeta {
	switch {
	case r.Group != nil:
		return r.Group.Metadata
	case r.Runtime != nil:
		return r.Runtime.Metadata
	case r.Task != nil:
		return r.Task.Metadata
	case r.Checkpoint != nil:
		return r.Checkpoint.Metadata
	}
	return nil
}
func invalid(err error) error { return status.Error(codes.InvalidArgument, err.Error()) }
func validateID(id string) error {
	if id == "" || len(id) > 128 {
		return status.Error(codes.InvalidArgument, "operation/request ID of at most 128 bytes is required")
	}
	return nil
}
func readReference(rs store.ManagedRecords, kind string, ref *ax.ResourceRef) (*managedRecord, error) {
	if err := ax.ValidateRef(ref, false); err != nil {
		return nil, invalid(err)
	}
	r, err := record(rs, kind+"/"+ref.Name)
	if err != nil {
		return nil, err
	}
	m := metadataOf(r)
	if r.Deleted {
		return nil, status.Error(codes.NotFound, "AX resource was deleted")
	}
	if m == nil || m.Atespace != ref.Atespace || (ref.Uid != "" && m.Uid != ref.Uid) {
		return nil, status.Error(codes.FailedPrecondition, "AX resource identity changed")
	}
	return r, nil
}

func reference(records store.ManagedRecords, kind string, ref *ax.ResourceRef) (*managedRecord, error) {
	if err := ax.ValidateRef(ref, true); err != nil {
		return nil, invalid(err)
	}
	r, err := record(records, kind+"/"+ref.Name)
	if err != nil {
		return nil, err
	}
	m := metadataOf(r)
	if r.Deleted || m == nil || m.Uid != ref.Uid || m.Atespace != ref.Atespace {
		return nil, status.Error(codes.FailedPrecondition, "AX resource identity changed")
	}
	if r.Operation != nil && r.Operation.Phase != "Succeeded" {
		return nil, status.Error(codes.FailedPrecondition, "AX resource has an unresolved operation")
	}
	return r, nil
}
func begin(r *managedRecord, id, kind, dig, owner string) (bool, error) {
	if err := validateID(id); err != nil {
		return false, err
	}
	if old, ok := r.Receipts[id]; ok {
		if old != kind+":"+dig {
			return false, status.Error(codes.AlreadyExists, "operation ID reused with different input")
		}
		return false, nil
	}
	if op := r.Operation; op != nil && op.Phase != "Succeeded" {
		return false, status.Error(codes.FailedPrecondition, "an unresolved operation owns this resource; recovery is required")
	}
	if r.Deleted {
		return false, status.Error(codes.NotFound, "AX resource was deleted")
	}
	r.Operation = &managedOperation{ID: id, Kind: kind, Digest: dig, Owner: owner, Phase: "Issued"}
	return true, nil
}
func complete(r *managedRecord, owner string, err error) error {
	op := r.Operation
	if op == nil || op.Owner != owner || op.Phase != "Issued" {
		return status.Error(codes.Aborted, "operation ownership changed")
	}
	if err != nil {
		op.Phase = "Uncertain"
		return nil
	}
	op.Phase = "Succeeded"
	if r.Receipts == nil {
		r.Receipts = map[string]string{}
	}
	r.Receipts[op.ID] = op.Kind + ":" + op.Digest
	metadataOf(r).ResourceVersion++
	return nil
}
func (m *ManagedController) finish(ctx context.Context, space, key, owner string, backendErr error, update func(*managedRecord)) error {
	// A canceled request must not prevent recording a known result. The operation
	// claim still protects correctness if persistence is unavailable.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return m.Store.UpdateManaged(ctx, space, func(rs store.ManagedRecords) error {
		r, err := record(rs, key)
		if err != nil {
			return err
		}
		if err = complete(r, owner, backendErr); err != nil {
			return err
		}
		update(r)
		return put(rs, key, r)
	})
}
func (m *ManagedController) read(ctx context.Context, space, key string) (*managedRecord, error) {
	rs, err := m.Store.ReadManaged(ctx, space)
	if err != nil {
		return nil, err
	}
	r, err := record(rs, key)
	if err != nil {
		return nil, err
	}
	if r.Deleted {
		return nil, status.Error(codes.NotFound, "AX resource was deleted")
	}
	return r, nil
}
func (m *ManagedController) OwnsTask(ctx context.Context, space, name string) (bool, error) {
	rs, err := m.Store.ReadManaged(ctx, space)
	if err != nil {
		return false, err
	}
	_, ok := rs["task/"+name]
	return ok, nil
}
func referenced(rs store.ManagedRecords, ref *ax.ResourceRef) error {
	for _, raw := range rs {
		var r managedRecord
		if err := json.Unmarshal(raw, &r); err != nil {
			return status.Error(codes.DataLoss, "invalid managed resource record")
		}
		if r.Deleted {
			continue
		}
		var refs []*ax.ResourceRef
		if r.Runtime != nil {
			refs = append(refs, r.Runtime.Spec.GroupRef)
		}
		if r.Task != nil {
			refs = append(refs, r.Task.Spec.GroupRef, r.Task.Spec.PreparedRuntimeRef, r.Task.Spec.RestoreFrom)
		}
		if r.Checkpoint != nil {
			refs = append(refs, r.Checkpoint.GroupRef, r.Checkpoint.RuntimeRef)
		}
		for _, other := range refs {
			if other != nil && proto.Equal(other, ref) {
				return status.Error(codes.FailedPrecondition, "AX resource is still referenced")
			}
		}
	}
	return nil
}
func (m *ManagedController) CreateGroup(ctx context.Context, req *ax.CreateTaskGroupRequest) (*ax.TaskGroup, error) {
	if req == nil || req.Group == nil {
		return nil, status.Error(codes.InvalidArgument, "task group is required")
	}
	g := proto.Clone(req.Group).(*ax.TaskGroup)
	if err := ax.ValidateTaskGroup(g); err != nil {
		return nil, invalid(err)
	}
	if err := validateID(req.RequestId); err != nil {
		return nil, err
	}
	if g.Spec.SandboxClass == "" {
		g.Spec.SandboxClass = "gvisor"
	}
	if g.Spec.Replicas == nil {
		g.Spec.Replicas = proto.Int32(1)
	}
	g.Metadata = metadata(g.Metadata)
	g.Status = &ax.TaskGroupStatus{Phase: "Pending"}
	key, space, owner := "group/"+g.Metadata.Name, g.Metadata.Atespace, uid()
	dig := digest(g.Spec)
	issued := false
	candidate := proto.Clone(g).(*ax.TaskGroup)
	err := m.Store.UpdateManaged(ctx, space, func(rs store.ManagedRecords) error {
		issued = false
		g = proto.Clone(candidate).(*ax.TaskGroup)
		for key, raw := range rs {
			if strings.HasPrefix(key, "deleted-group/") {
				var old managedRecord
				if err := json.Unmarshal(raw, &old); err != nil {
					return err
				}
				if old.Group.Metadata.Name == g.Metadata.Name && old.Receipts[req.RequestId] != "" {
					return status.Error(codes.FailedPrecondition, "request belongs to a deleted group generation")
				}
			}
		}
		r, e := record(rs, key)
		if status.Code(e) == codes.NotFound {
			r = &managedRecord{Group: g}
		} else if e != nil {
			return e
		} else if r.Deleted {
			if r.Receipts[req.RequestId] != "" {
				return status.Error(codes.FailedPrecondition, "request belongs to a deleted group generation")
			}
			if err := put(rs, "deleted-group/"+r.Group.Metadata.Uid, r); err != nil {
				return err
			}
			r = &managedRecord{Group: g}
		} else {
			if receipt := r.Receipts[req.RequestId]; receipt != "" {
				if receipt != "Create:"+dig {
					return status.Error(codes.AlreadyExists, "request ID reused with different input")
				}
				g = r.Group
				return nil
			}
			if r.Operation != nil && r.Operation.ID == req.RequestId && r.Operation.Phase != "Succeeded" {
				return status.Error(codes.FailedPrecondition, "task group creation requires recovery")
			}
			return status.Error(codes.AlreadyExists, "task group name already exists")
		}
		var err error
		issued, err = begin(r, req.RequestId, "Create", dig, owner)
		if err != nil {
			return err
		}
		return put(rs, key, r)
	})
	if err != nil {
		return nil, err
	}
	if !issued {
		return g, nil
	}
	backendUID, ready, backendErr := m.Backend.EnsureGroup(ctx, g, "")
	err = m.finish(ctx, space, key, owner, backendErr, func(r *managedRecord) {
		r.BackendUID = backendUID
		r.Group.Status.ReadyWorkers = ready
		if backendErr == nil {
			r.Group.Status.Phase = "Ready"
			if ready < r.Group.Spec.GetReplicas() {
				r.Group.Status.Phase = "Provisioning"
			}
		} else {
			r.Group.Status.Phase = "Unknown"
			r.Group.Status.Message = "operation requires recovery"
		}
		g = r.Group
	})
	if err != nil {
		return nil, err
	}
	return g, backendErr
}
func (m *ManagedController) GetGroup(ctx context.Context, space, name string) (*ax.TaskGroup, error) {
	key := "group/" + name
	r, err := m.read(ctx, space, key)
	if err != nil {
		return nil, err
	}
	if r.Operation == nil || r.Operation.Phase != "Succeeded" {
		return r.Group, nil
	}
	ready, err := m.Backend.ReadyWorkers(ctx, r.Group, r.BackendUID)
	if err != nil {
		return nil, err
	}
	phase := "Ready"
	if ready < r.Group.Spec.GetReplicas() {
		phase = "Provisioning"
	}
	if r.Group.Status.Phase == phase && r.Group.Status.ReadyWorkers == ready {
		return r.Group, nil
	}
	version := r.Group.Metadata.ResourceVersion
	err = m.Store.UpdateManaged(ctx, space, func(rs store.ManagedRecords) error {
		current, e := reference(rs, "group", ax.Ref(r.Group.Metadata))
		if e != nil {
			return e
		}
		if current.Group.Metadata.ResourceVersion != version {
			return status.Error(codes.Aborted, "task group changed during observation")
		}
		current.Group.Status.Phase = phase
		current.Group.Status.ReadyWorkers = ready
		current.Group.Metadata.ResourceVersion++
		r = current
		return put(rs, key, current)
	})
	if err != nil {
		return nil, err
	}
	return r.Group, nil
}
func (m *ManagedController) ListGroups(ctx context.Context, req *ax.ListTaskGroupsRequest) (*ax.ListTaskGroupsResponse, error) {
	if req == nil || ax.ValidateName(req.Atespace) != nil {
		return nil, status.Error(codes.InvalidArgument, "atespace is required")
	}
	rs, err := m.Store.ReadManaged(ctx, req.Atespace)
	if err != nil {
		return nil, err
	}
	names := []string{}
	for key := range rs {
		if strings.HasPrefix(key, "group/") && strings.TrimPrefix(key, "group/") > req.PageToken {
			names = append(names, key)
		}
	}
	sort.Strings(names)
	size := req.PageSize
	if size <= 0 {
		size = 100
	}
	if size > 1000 {
		size = 1000
	}
	res := &ax.ListTaskGroupsResponse{}
	for _, key := range names {
		r, err := record(rs, key)
		if err != nil {
			return nil, err
		}
		if r.Deleted {
			continue
		}
		if len(res.Groups) == int(size) {
			res.NextPageToken = res.Groups[len(res.Groups)-1].Metadata.Name
			break
		}
		res.Groups = append(res.Groups, r.Group)
	}
	return res, nil
}
func (m *ManagedController) UpdateGroup(ctx context.Context, req *ax.UpdateTaskGroupRequest) (*ax.TaskGroup, error) {
	if req == nil || req.Replicas < 0 {
		return nil, status.Error(codes.InvalidArgument, "non-negative replicas are required")
	}
	if err := ax.ValidateRef(req.Ref, true); err != nil {
		return nil, invalid(err)
	}
	// Resource version doubles as a stable operation identity for this CAS update.
	owner, key := uid(), "group/"+req.Ref.Name
	var g *ax.TaskGroup
	var backendUID string
	shrink := false
	issued := false
	err := m.Store.UpdateManaged(ctx, req.Ref.Atespace, func(rs store.ManagedRecords) error {
		issued = false
		r, err := record(rs, key)
		if err != nil {
			return err
		}
		if r.Group.Metadata.Uid != req.Ref.Uid {
			return status.Error(codes.FailedPrecondition, "group UID differs")
		}
		id := fmt.Sprintf("update-%d", req.ExpectedVersion)
		dig := digest(req)
		if r.Receipts[id] != "" {
			var e error
			issued, e = begin(r, id, "Update", dig, owner)
			g = r.Group
			return e
		}
		if r.Group.Metadata.ResourceVersion != req.ExpectedVersion {
			return status.Error(codes.Aborted, "task group version changed")
		}
		if r.Group.Spec.GetReplicas() > req.Replicas {
			if err = referenced(rs, req.Ref); err != nil {
				return err
			}
			shrink = true
		}
		issued, err = begin(r, id, "Update", dig, owner)
		if err != nil {
			return err
		}
		r.Group.Spec.Replicas = proto.Int32(req.Replicas)
		r.Group.Status.Phase = "Pending"
		g = r.Group
		backendUID = r.BackendUID
		return put(rs, key, r)
	})
	if err != nil {
		return nil, err
	}
	if !issued {
		return g, nil
	}
	var ready int32
	if shrink {
		err = m.Backend.GroupIdle(ctx, g)
	}
	if err == nil {
		backendUID, ready, err = m.Backend.EnsureGroup(ctx, g, backendUID)
	}
	backendErr := err
	err = m.finish(ctx, req.Ref.Atespace, key, owner, backendErr, func(r *managedRecord) {
		r.BackendUID = backendUID
		r.Group.Status.ReadyWorkers = ready
		if backendErr == nil {
			r.Group.Status.Phase = "Ready"
			if ready < r.Group.Spec.GetReplicas() {
				r.Group.Status.Phase = "Provisioning"
			}
		} else {
			r.Group.Status.Phase = "Unknown"
		}
		g = r.Group
	})
	if err != nil {
		return nil, err
	}
	return g, backendErr
}
func (m *ManagedController) DeleteResource(ctx context.Context, kind string, ref *ax.ResourceRef, operationID string) error {
	if err := ax.ValidateRef(ref, true); err != nil {
		return invalid(err)
	}
	if err := validateID(operationID); err != nil {
		return err
	}
	key, owner := kind+"/"+ref.Name, uid()
	var reserved *managedRecord
	issued := false
	err := m.Store.UpdateManaged(ctx, ref.Atespace, func(rs store.ManagedRecords) error {
		issued = false
		r, e := record(rs, key)
		if e != nil {
			return e
		}
		if metadataOf(r).Uid != ref.Uid {
			return status.Error(codes.FailedPrecondition, "resource UID differs")
		}
		if err := referenced(rs, ref); err != nil {
			return err
		}
		var err error
		issued, err = begin(r, operationID, "Delete", digest(ref), owner)
		if err != nil {
			return err
		}
		reserved = r
		return put(rs, key, r)
	})
	if err != nil || !issued {
		return err
	}
	switch kind {
	case "group":
		err = m.Backend.DeleteGroup(ctx, reserved.Group, reserved.BackendUID)
	case "runtime":
		err = m.Backend.DeleteRuntime(ctx, reserved.Runtime, reserved.BackendUID)
	case "checkpoint":
		err = m.Backend.DeleteCheckpoint(ctx, reserved.Checkpoint, reserved.BackendUID)
	default:
		err = status.Error(codes.InvalidArgument, "unsupported resource kind")
	}
	backendErr := err
	err = m.finish(ctx, ref.Atespace, key, owner, backendErr, func(r *managedRecord) {
		if backendErr == nil {
			r.Deleted = true
		}
	})
	if err != nil {
		return err
	}
	return backendErr
}
func (m *ManagedController) PrepareRuntime(ctx context.Context, req *ax.PrepareRuntimeRequest) (*ax.PreparedRuntime, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := ax.ValidateName(req.GetMetadata().GetAtespace()); err != nil {
		return nil, invalid(err)
	}
	if err := ax.ValidateObjectMeta(req.Metadata); err != nil {
		return nil, invalid(err)
	}
	if err := ax.ValidatePreparedRuntime(req.Spec); err != nil {
		return nil, invalid(err)
	}
	if err := validateID(req.RequestId); err != nil {
		return nil, err
	}
	r := &ax.PreparedRuntime{Metadata: metadata(req.Metadata), Spec: proto.Clone(req.Spec).(*ax.PreparedRuntimeSpec), Phase: "Preparing"}
	if r.Spec.GroupRef.Atespace != r.Metadata.Atespace {
		return nil, status.Error(codes.InvalidArgument, "runtime and group must share an atespace")
	}
	key, owner := "runtime/"+r.Metadata.Name, uid()
	var g *ax.TaskGroup
	issued := false
	err := m.Store.UpdateManaged(ctx, r.Metadata.Atespace, func(rs store.ManagedRecords) error {
		issued = false
		group, err := reference(rs, "group", r.Spec.GroupRef)
		if err != nil {
			return err
		}
		g = group.Group
		r.Digest = digest(r.Spec)
		saved, e := record(rs, key)
		if status.Code(e) == codes.NotFound {
			saved = &managedRecord{Runtime: r}
		} else if e != nil {
			return e
		} else {
			if saved.Runtime.Digest != r.Digest {
				return status.Error(codes.AlreadyExists, "runtime configuration differs")
			}
			r = saved.Runtime
			if saved.Deleted {
				return status.Error(codes.FailedPrecondition, "runtime was released")
			}
			if saved.Operation != nil && saved.Operation.Phase == "Succeeded" && saved.Receipts[req.RequestId] == "" {
				return nil
			}
		}
		issued, err = begin(saved, req.RequestId, "Prepare", r.Digest, owner)
		if err != nil {
			return err
		}
		return put(rs, key, saved)
	})
	if err != nil {
		return nil, err
	}
	if !issued {
		return r, nil
	}
	backendUID, phase, backendErr := m.Backend.Prepare(ctx, r, g)
	err = m.finish(ctx, r.Metadata.Atespace, key, owner, backendErr, func(saved *managedRecord) {
		saved.BackendUID = backendUID
		saved.Runtime.Phase = phase
		if backendErr != nil {
			saved.Runtime.Phase = "Unknown"
			saved.Runtime.Message = "preparation requires recovery"
		}
		r = saved.Runtime
	})
	if err != nil {
		return nil, err
	}
	return r, backendErr
}
func (m *ManagedController) GetRuntime(ctx context.Context, ref *ax.ResourceRef) (*ax.PreparedRuntime, error) {
	if err := ax.ValidateRef(ref, false); err != nil {
		return nil, invalid(err)
	}
	rs, err := m.Store.ReadManaged(ctx, ref.Atespace)
	if err != nil {
		return nil, err
	}
	r, err := readReference(rs, "runtime", ref)
	if err != nil {
		return nil, err
	}
	if r.Runtime.Phase == "Preparing" && r.Operation != nil && r.Operation.Phase == "Succeeded" {
		phase, err := m.Backend.ObserveRuntime(ctx, r.Runtime, r.BackendUID)
		if err != nil {
			return nil, err
		}
		if phase != r.Runtime.Phase {
			err = m.Store.UpdateManaged(ctx, ref.Atespace, func(current store.ManagedRecords) error {
				latest, e := reference(current, "runtime", ax.Ref(r.Runtime.Metadata))
				if e != nil {
					return e
				}
				if latest.Runtime.Metadata.ResourceVersion != r.Runtime.Metadata.ResourceVersion {
					return status.Error(codes.Aborted, "runtime changed while observing preparation")
				}
				latest.Runtime.Phase = phase
				latest.Runtime.Metadata.ResourceVersion++
				r = latest
				return put(current, "runtime/"+ref.Name, latest)
			})
			if err != nil {
				return nil, err
			}
		}
	}
	return r.Runtime, nil
}
func (m *ManagedController) CreateTask(ctx context.Context, req *ax.CreateTaskRequest) (*ax.Task, error) {
	if req == nil || req.Task == nil {
		return nil, status.Error(codes.InvalidArgument, "task is required")
	}
	if err := ax.ValidateName(req.Task.GetMetadata().GetAtespace()); err != nil {
		return nil, invalid(err)
	}
	input := req.Task
	if err := ax.ValidateObjectMeta(input.Metadata); err != nil {
		return nil, invalid(err)
	}
	if err := validateID(req.RequestId); err != nil {
		return nil, err
	}
	s := input.Spec
	if s == nil {
		return nil, status.Error(codes.InvalidArgument, "task spec is required")
	}
	if s.Image != "" || len(s.Command) > 0 || len(s.Env) > 0 || s.Resources != nil || len(s.Workspaces) > 0 || s.Debug {
		return nil, status.Error(codes.InvalidArgument, "managed Task uses only preparedRuntimeRef, groupRef and restoreFrom")
	}
	if err := ax.ValidateRef(s.GroupRef, true); err != nil {
		return nil, invalid(err)
	}
	if err := ax.ValidateRef(s.PreparedRuntimeRef, true); err != nil {
		return nil, invalid(err)
	}
	space := input.Metadata.Atespace
	if s.GroupRef.Atespace != space || s.PreparedRuntimeRef.Atespace != space {
		return nil, status.Error(codes.InvalidArgument, "cross-atespace runtime references are forbidden")
	}
	t := proto.Clone(input).(*ax.Task)
	t.Metadata = metadata(input.Metadata)
	t.Status = &ax.TaskStatus{RuntimeStatus: &ax.TaskRuntimeStatus{Phase: "Creating", LastOperationId: req.RequestId}}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, err
	}
	token := space + ":" + base64.RawURLEncoding.EncodeToString(secret[:])
	hash := sha256.Sum256([]byte(token))
	key, owner := "task/"+t.Metadata.Name, uid()
	var runtime *ax.PreparedRuntime
	var checkpoint *ax.TaskCheckpoint
	var runtimeUID, checkpointUID string
	issued := false
	dig := digest(t.Spec)
	err := m.Store.UpdateManaged(ctx, space, func(rs store.ManagedRecords) error {
		issued = false
		// A retry of the original request must use the original immutable identity.
		if old, e := record(rs, key); e == nil {
			if digest(old.Task.Spec) != dig {
				return status.Error(codes.AlreadyExists, "task spec differs")
			}
			var err error
			issued, err = begin(old, req.RequestId, "Create", dig, owner)
			if err != nil {
				return err
			}
			if issued {
				return status.Error(codes.AlreadyExists, "task already exists")
			}
			t = old.Task
			return nil
		} else if status.Code(e) != codes.NotFound {
			return e
		}
		group, err := reference(rs, "group", s.GroupRef)
		if err != nil {
			return err
		}
		if group.Group.Spec.GetReplicas() == 0 {
			return status.Error(codes.FailedPrecondition, "task group has zero capacity")
		}
		prepared, err := reference(rs, "runtime", s.PreparedRuntimeRef)
		if err != nil {
			return err
		}
		runtime = prepared.Runtime
		runtimeUID = prepared.BackendUID
		if !proto.Equal(runtime.Spec.GroupRef, s.GroupRef) || runtime.Phase != "Ready" {
			return status.Error(codes.FailedPrecondition, "runtime is not ready in the selected group")
		}
		if s.RestoreFrom != nil {
			if s.RestoreFrom.Atespace != space {
				return status.Error(codes.InvalidArgument, "cross-atespace restore is forbidden")
			}
			cp, err := reference(rs, "checkpoint", s.RestoreFrom)
			if err != nil {
				return err
			}
			checkpoint = cp.Checkpoint
			checkpointUID = cp.BackendUID
			if checkpoint.Phase != "Ready" || !proto.Equal(checkpoint.RuntimeRef, s.PreparedRuntimeRef) || !proto.Equal(checkpoint.GroupRef, s.GroupRef) {
				return status.Error(codes.FailedPrecondition, "checkpoint lineage differs")
			}
		}
		saved := &managedRecord{Task: t, CredentialHash: hex.EncodeToString(hash[:])}
		issued, err = begin(saved, req.RequestId, "Create", dig, owner)
		if err != nil {
			return err
		}
		return put(rs, key, saved)
	})
	if err != nil {
		return nil, err
	}
	if !issued {
		return t, nil
	}
	var observed substrate.Observation
	backendErr := m.Backend.VerifyPreparation(ctx, runtime, runtimeUID, checkpoint, checkpointUID)
	if backendErr == nil {
		observed, backendErr = m.Backend.Create(ctx, t, runtime, checkpoint, token)
	}
	err = m.finish(ctx, space, key, owner, backendErr, func(saved *managedRecord) {
		saved.BackendUID = observed.UID
		applyObservation(saved, observed, backendErr)
		t = saved.Task
	})
	if err != nil {
		return nil, err
	}
	return t, backendErr
}
func applyObservation(r *managedRecord, o substrate.Observation, err error) {
	rt := r.Task.Status.RuntimeStatus
	if err != nil {
		rt.Phase = "Unknown"
		rt.Message = "operation requires recovery"
		return
	}
	rt.Phase = o.Phase
	rt.Message = ""
	rt.ExecutionVersion++
	rt.LastOperationId = r.Operation.ID
	if o.SnapshotURI != "" && o.Phase == "Suspended" && o.Data {
		sum := sha256.Sum256([]byte(o.UID + "\x00" + o.SnapshotURI))
		rt.BoundaryRef = hex.EncodeToString(sum[:])
		rt.Restorable = true
	} else {
		rt.BoundaryRef = ""
		rt.Restorable = false
	}
}
func (m *ManagedController) GetTask(ctx context.Context, space, name string) (*ax.Task, error) {
	r, err := m.read(ctx, space, "task/"+name)
	if err != nil {
		return nil, err
	}
	return r.Task, nil
}
func (m *ManagedController) Transition(ctx context.Context, ref *ax.ResourceRef, operationID, kind string) (*ax.Task, error) {
	if err := ax.ValidateRef(ref, true); err != nil {
		return nil, invalid(err)
	}
	if kind != "Resume" && kind != "Suspend" && kind != "Pause" && kind != "Delete" {
		return nil, status.Error(codes.InvalidArgument, "invalid transition")
	}
	owner, key := uid(), "task/"+ref.Name
	var saved *managedRecord
	issued := false
	err := m.Store.UpdateManaged(ctx, ref.Atespace, func(rs store.ManagedRecords) error {
		issued = false
		r, err := record(rs, key)
		if err != nil {
			return err
		}
		if metadataOf(r).Uid != ref.Uid {
			return status.Error(codes.FailedPrecondition, "task identity changed")
		}
		issued, err = begin(r, operationID, kind, digest(ref), owner)
		if err != nil {
			return err
		}
		saved = r
		if issued {
			r.Task.Status.RuntimeStatus.Phase = "Transitioning"
			if kind == "Delete" {
				r.CredentialHash = ""
			}
		}
		return put(rs, key, r)
	})
	if err != nil {
		return nil, err
	}
	if !issued {
		return saved.Task, nil
	}
	observed, backendErr := m.Backend.Transition(ctx, saved.Task, saved.BackendUID, kind)
	err = m.finish(ctx, ref.Atespace, key, owner, backendErr, func(r *managedRecord) {
		applyObservation(r, observed, backendErr)
		if backendErr == nil && kind == "Delete" {
			r.Deleted = true
		}
		saved = r
	})
	if err != nil {
		return nil, err
	}
	return saved.Task, backendErr
}
func (m *ManagedController) Authenticate(ctx context.Context, req *ax.AuthenticateRuntimeRequest) (*ax.AuthenticateRuntimeResponse, error) {
	if req == nil || req.Credential == "" || ax.ValidateName(req.Atespace) != nil {
		return nil, status.Error(codes.Unauthenticated, "runtime credential is required")
	}
	hash := sha256.Sum256([]byte(req.Credential))
	expected := hex.EncodeToString(hash[:])
	rs, err := m.Store.ReadManaged(ctx, req.Atespace)
	if err != nil {
		return nil, err
	}
	for key := range rs {
		if !strings.HasPrefix(key, "task/") {
			continue
		}
		r, err := record(rs, key)
		if err != nil {
			return nil, err
		}
		if !r.Deleted && r.CredentialHash == expected && r.Operation != nil && ((r.Operation.Phase == "Succeeded" && r.Task.Status.RuntimeStatus.Phase == "Running") || (r.Operation.Phase == "Issued" && r.Operation.Kind == "Resume" && r.BackendUID != "")) {
			return &ax.AuthenticateRuntimeResponse{TaskRef: ax.Ref(r.Task.Metadata), Scopes: []string{"taskstore"}}, nil
		}
	}
	return nil, status.Error(codes.Unauthenticated, "invalid or inactive runtime credential")
}

// Admit verifies persistent ownership immediately before opening a data-plane
// call. No lease is held over a long stream or an inbound TaskStore callback.
func (m *ManagedController) Admit(ctx context.Context, ref *ax.ResourceRef, kind string) (*ax.Task, error) {
	if err := ax.ValidateRef(ref, true); err != nil {
		return nil, invalid(err)
	}
	rs, err := m.Store.ReadManaged(ctx, ref.Atespace)
	if err != nil {
		return nil, err
	}
	r, err := reference(rs, "task", ref)
	if err != nil {
		return nil, err
	}
	if r.Task.Status.RuntimeStatus.Phase != "Running" {
		return nil, status.Error(codes.FailedPrecondition, "task is not running")
	}
	runtime, err := reference(rs, "runtime", r.Task.Spec.PreparedRuntimeRef)
	if err != nil {
		return nil, err
	}
	if runtime.Runtime.Spec.Kind != kind {
		return nil, status.Error(codes.FailedPrecondition, "task runtime kind differs")
	}
	return r.Task, nil
}

func (m *ManagedController) CreateCheckpoint(ctx context.Context, req *ax.CreateTaskCheckpointRequest) (*ax.TaskCheckpoint, error) {
	if req == nil || ax.ValidateName(req.Name) != nil || req.ExpectedBoundaryRef == "" {
		return nil, status.Error(codes.InvalidArgument, "checkpoint name and boundaryRef are required")
	}
	if err := ax.ValidateRef(req.TaskRef, true); err != nil {
		return nil, invalid(err)
	}
	if err := validateID(req.RequestId); err != nil {
		return nil, err
	}
	key, taskKey, owner := "checkpoint/"+req.Name, "task/"+req.TaskRef.Name, uid()
	dig := digest(req)
	var c *ax.TaskCheckpoint
	var source *managedRecord
	issued := false
	err := m.Store.UpdateManaged(ctx, req.TaskRef.Atespace, func(rs store.ManagedRecords) error {
		issued = false
		if old, e := record(rs, key); e == nil {
			var err error
			issued, err = begin(old, req.RequestId, "Checkpoint", dig, owner)
			if err != nil {
				return err
			}
			if issued {
				return status.Error(codes.AlreadyExists, "checkpoint already exists")
			}
			c = old.Checkpoint
			return nil
		} else if status.Code(e) != codes.NotFound {
			return e
		}
		var err error
		source, err = reference(rs, "task", req.TaskRef)
		if err != nil {
			return err
		}
		state := source.Task.Status.RuntimeStatus
		if state.Phase != "Suspended" || !state.Restorable || state.BoundaryRef != req.ExpectedBoundaryRef {
			return status.Error(codes.FailedPrecondition, "task boundary changed or is not checkpointable")
		}
		if _, err = begin(source, "checkpoint-"+dig[:32], "Checkpoint", dig, owner); err != nil {
			return err
		}
		c = &ax.TaskCheckpoint{Metadata: metadata(&ax.ObjectMeta{Name: req.Name, Atespace: req.TaskRef.Atespace}), SourceTask: proto.Clone(req.TaskRef).(*ax.ResourceRef), RuntimeRef: source.Task.Spec.PreparedRuntimeRef, GroupRef: source.Task.Spec.GroupRef, Phase: "Creating", BoundaryRef: req.ExpectedBoundaryRef}
		saved := &managedRecord{Checkpoint: c}
		issued, err = begin(saved, req.RequestId, "Checkpoint", dig, owner)
		if err != nil {
			return err
		}
		if err = put(rs, taskKey, source); err != nil {
			return err
		}
		return put(rs, key, saved)
	})
	if err != nil {
		return nil, err
	}
	if !issued {
		return c, nil
	}
	backendUID, backendErr := m.Backend.Checkpoint(ctx, source.Task, source.BackendUID, c)
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	err = m.Store.UpdateManaged(finishCtx, req.TaskRef.Atespace, func(rs store.ManagedRecords) error {
		saved, err := record(rs, key)
		if err != nil {
			return err
		}
		task, err := record(rs, taskKey)
		if err != nil {
			return err
		}
		if err = complete(saved, owner, backendErr); err != nil {
			return err
		}
		if err = complete(task, owner, backendErr); err != nil {
			return err
		}
		saved.BackendUID = backendUID
		saved.Checkpoint.Phase = "Ready"
		if backendErr != nil {
			saved.Checkpoint.Phase = "Unknown"
			saved.Checkpoint.Message = "checkpoint operation requires recovery"
		}
		c = saved.Checkpoint
		if err = put(rs, taskKey, task); err != nil {
			return err
		}
		return put(rs, key, saved)
	})
	if err != nil {
		return nil, err
	}
	return c, backendErr
}
func (m *ManagedController) GetCheckpoint(ctx context.Context, ref *ax.ResourceRef) (*ax.TaskCheckpoint, error) {
	if err := ax.ValidateRef(ref, false); err != nil {
		return nil, invalid(err)
	}
	rs, err := m.Store.ReadManaged(ctx, ref.Atespace)
	if err != nil {
		return nil, err
	}
	r, err := readReference(rs, "checkpoint", ref)
	if err != nil {
		return nil, err
	}
	return r.Checkpoint, nil
}

func (m *ManagedController) AdmitService(ctx context.Context, ref *ax.ResourceRef) (*ax.Task, error) {
	if _, err := m.RefreshTask(ctx, ref.Atespace, ref.Name); err != nil {
		return nil, err
	}
	rs, err := m.Store.ReadManaged(ctx, ref.Atespace)
	if err != nil {
		return nil, err
	}
	r, err := reference(rs, "task", ref)
	if err != nil {
		return nil, err
	}
	runtime, err := reference(rs, "runtime", r.Task.Spec.PreparedRuntimeRef)
	if err != nil {
		return nil, err
	}
	if runtime.Runtime.Spec.Kind != "Service" {
		return nil, status.Error(codes.FailedPrecondition, "A2A requires a Service runtime")
	}
	phase := r.Task.Status.RuntimeStatus.Phase
	if phase == "Suspended" || phase == "Paused" {
		if _, err = m.Transition(ctx, ref, fmt.Sprintf("gateway-resume-%d", r.Task.Metadata.ResourceVersion), "Resume"); err != nil {
			return nil, err
		}
	}
	return m.Admit(ctx, ref, "Service")
}

// RefreshTask only observes the bound instance; a missing instance is an error,
// never permission to create a replacement. Unknown operations remain owned.
func (m *ManagedController) RefreshTask(ctx context.Context, space, name string) (*ax.Task, error) {
	key := "task/" + name
	r, err := m.read(ctx, space, key)
	if err != nil {
		return nil, err
	}
	if r.Operation == nil || r.Operation.Phase != "Succeeded" {
		return r.Task, nil
	}
	observed, err := m.Backend.Observe(ctx, r.Task)
	if err != nil {
		return nil, err
	}
	if observed.UID != r.BackendUID {
		return nil, status.Error(codes.FailedPrecondition, "bound runtime identity changed")
	}
	version := r.Task.Metadata.ResourceVersion
	oldPhase, oldBoundary := r.Task.Status.RuntimeStatus.Phase, r.Task.Status.RuntimeStatus.BoundaryRef
	applyObservation(r, observed, nil)
	if oldPhase == r.Task.Status.RuntimeStatus.Phase && oldBoundary == r.Task.Status.RuntimeStatus.BoundaryRef {
		return m.GetTask(ctx, space, name)
	}
	err = m.Store.UpdateManaged(ctx, space, func(rs store.ManagedRecords) error {
		current, e := record(rs, key)
		if e != nil {
			return e
		}
		if current.Deleted || current.Task.Metadata.ResourceVersion != version || current.Operation == nil || current.Operation.Phase != "Succeeded" {
			return status.Error(codes.Aborted, "task changed while observing runtime")
		}
		r.Task.Metadata.ResourceVersion++
		return put(rs, key, r)
	})
	if err != nil {
		return nil, err
	}
	return r.Task, nil
}

// ListTasks observes durable managed ownership, including uncertain operations.
// It never probes or repairs the backend while serving a catalog request.
func (m *ManagedController) ListTasks(ctx context.Context, req *ax.ListTasksRequest) (*ax.ListTasksResponse, error) {
	if req == nil || ax.ValidateName(req.Atespace) != nil {
		return nil, status.Error(codes.InvalidArgument, "atespace is required")
	}
	if req.Offset < 0 || req.Limit < 0 || req.Limit > 1000 {
		return nil, status.Error(codes.InvalidArgument, "limit must be 0..1000 and offset must be non-negative")
	}
	limit := req.Limit
	if limit == 0 {
		limit = 50
	}
	records, err := m.Store.ReadManaged(ctx, req.Atespace)
	if err != nil {
		return nil, err
	}
	var tasks []*ax.Task
	for key := range records {
		if !strings.HasPrefix(key, "task/") {
			continue
		}
		r, err := record(records, key)
		if err != nil {
			return nil, err
		}
		if r.Deleted {
			continue
		}
		if r.Task == nil || r.Task.Metadata == nil {
			return nil, status.Error(codes.DataLoss, "invalid managed task record")
		}
		tasks = append(tasks, r.Task)
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].Metadata.Name < tasks[j].Metadata.Name })
	if req.Offset >= int64(len(tasks)) {
		return &ax.ListTasksResponse{}, nil
	}
	tasks = tasks[int(req.Offset):]
	if int64(len(tasks)) > limit {
		tasks = tasks[:int(limit)]
	}
	return &ax.ListTasksResponse{Tasks: tasks}, nil
}
