package substrate

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"log/slog"
	"strconv"
	"strings"

	pb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Private backend evidence. None of these identities is a public Task model.
type RecoveryInput struct {
	Kind, BackendUID, RuntimeUID, CheckpointUID, TaskUID, CredentialHash string
	Group                                                                *ax.TaskGroup
	Runtime                                                              *ax.PreparedRuntime
	Task                                                                 *ax.Task
	Checkpoint                                                           *ax.TaskCheckpoint
}
type RecoveryResult struct {
	BackendUID, Phase string
	ReadyWorkers      int32
	Task              Observation
}

// RecoverObserved never creates compute, templates, pools or snapshots. Missing
// or partially installed resources retain ownership for operator repair. Delete
// retries are UID-bound and may revoke the last per-task credential.
func (b *ManagedBackend) RecoverObserved(ctx context.Context, in RecoveryInput) (RecoveryResult, error) {
	out := RecoveryResult{BackendUID: in.BackendUID}
	if in.Kind == "Delete" {
		if in.BackendUID == "" {
			return out, status.Error(codes.FailedPrecondition, "deletion has no bound backend identity")
		}
		var err error
		switch {
		case in.Task != nil:
			out.Task, err = b.Transition(ctx, in.Task, in.BackendUID, "Delete")
		case in.Checkpoint != nil:
			err = b.DeleteCheckpoint(ctx, in.Checkpoint, in.BackendUID)
		case in.Runtime != nil:
			err = b.DeleteRuntime(ctx, in.Runtime, in.BackendUID)
		case in.Group != nil:
			err = b.DeleteGroup(ctx, in.Group, in.BackendUID)
		default:
			err = status.Error(codes.InvalidArgument, "unknown recovery resource")
		}
		return out, err
	}
	switch {
	case in.Kind == "Checkpoint" && in.Checkpoint != nil:
		observed, err := b.Observe(ctx, in.Task)
		if err != nil {
			return out, err
		}
		sum := sha256.Sum256([]byte(observed.UID + "\x00" + observed.SnapshotURI))
		if observed.UID != in.TaskUID || observed.Phase != "Suspended" || !observed.Data || hex.EncodeToString(sum[:]) != in.Checkpoint.BoundaryRef {
			return out, status.Error(codes.FailedPrecondition, "checkpoint boundary changed")
		}
		tag, err := b.Client.control.GetTag(ctx, &pb.GetTagRequest{Tag: &pb.ObjectRef{Atespace: in.Checkpoint.Metadata.Atespace, Name: CheckpointName(in.Checkpoint)}})
		if err != nil {
			return out, err
		}
		if tag.GetMetadata().GetUid() == "" || in.BackendUID != "" && tag.Metadata.Uid != in.BackendUID || tag.GetScope() != pb.TagScope_TAG_SCOPE_ATESPACE || !proto.Equal(tag.GetSourceActor(), taskRef(in.Task)) || tag.GetStatus().GetActorTemplateUid() != in.RuntimeUID || tag.GetStatus().GetSnapshot().GetSnapshotUri() != observed.SnapshotURI || tag.GetStatus().GetSnapshot().GetContentScope() != pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA {
			return out, status.Error(codes.FailedPrecondition, "checkpoint evidence differs")
		}
		out.BackendUID = tag.Metadata.Uid
		out.Phase = "Ready"
		return out, nil
	case in.Task != nil:
		actor, err := b.Client.GetActor(ctx, in.Task.Metadata.Atespace, taskName(in.Task))
		if err != nil {
			return out, err
		}
		observed := observe(actor)
		if observed.UID == "" || in.BackendUID != "" && observed.UID != in.BackendUID {
			return out, status.Error(codes.FailedPrecondition, "task backend identity changed")
		}
		want := map[string]string{"Create": "Suspended", "Resume": "Running", "Pause": "Paused", "Suspend": "Suspended"}[in.Kind]
		if want == "" || observed.Phase != want {
			return out, status.Error(codes.FailedPrecondition, "task has not reached the requested boundary")
		}
		if in.Kind == "Create" {
			if err = b.VerifyPreparation(ctx, in.Runtime, in.RuntimeUID, in.Checkpoint, in.CheckpointUID); err != nil {
				return out, err
			}
			if !proto.Equal(actor.ActorTemplate, &pb.ObjectRef{Atespace: in.Runtime.Metadata.Atespace, Name: runtimeName(in.Runtime)}) {
				return out, status.Error(codes.FailedPrecondition, "task runtime differs")
			}
			var source *pb.ObjectRef
			if in.Checkpoint != nil {
				source = &pb.ObjectRef{Atespace: in.Checkpoint.Metadata.Atespace, Name: CheckpointName(in.Checkpoint)}
			}
			if !proto.Equal(actor.SourceTag, source) {
				return out, status.Error(codes.FailedPrecondition, "task restore source differs")
			}
			eg := proto.CloneOf(in.Runtime.Spec.Egress)
			callbackPorts := map[string]int{}
			if eg == nil {
				eg = &ax.EgressSpec{}
			}
			if in.Runtime.Spec.Kind == "Service" {
				p, ok := b.Platform.(interface {
					VerifyCredential(context.Context, *ax.ResourceRef, string) (*ax.CredentialSecretRef, error)
				})
				if !ok {
					return out, status.Error(codes.FailedPrecondition, "platform cannot verify credential")
				}
				ref, err := p.VerifyCredential(ctx, ax.Ref(in.Task.Metadata), in.CredentialHash)
				if err != nil {
					return out, err
				}
				callback, err := url.Parse(b.CallbackURL)
				if err != nil || callback.Scheme != "https" || callback.Hostname() == "" {
					return out, status.Error(codes.FailedPrecondition, "invalid callback configuration")
				}
				eg.Destinations = append(eg.Destinations, callback.Hostname())
				eg.Credentials = append(eg.Credentials, &ax.EgressCredential{Hostname: callback.Hostname(), Header: ax.RuntimeCredentialHeader, SecretKeyRef: ref})
				// PATCH(new-egress-schema): carry the callback port through,
				// same as ManagedBackend.Egress.
				if p := callback.Port(); p != "" {
					if n, err := strconv.Atoi(p); err == nil && n > 0 && n < 65536 {
						callbackPorts[strings.ToLower(callback.Hostname())] = n
					}
				}
			}
			desired, err := CompileEgress(in.Task.Metadata.Atespace, eg, callbackPorts)
			if err != nil {
				return out, err
			}
			actual, err := b.Client.control.GetActorEgressPolicy(ctx, &pb.GetActorEgressPolicyRequest{Actor: taskRef(in.Task)})
			if err != nil {
				// PATCH(recovery-install-egress): the stuck Create operations
				// failed at exactly this step (the previous server build
				// compiled rules under a stale schema the deployed substrate
				// rejects), leaving the actor suspended with no policy and
				// recovery dead-ended. Complete the original Create intent by
				// installing the desired policy; this only ever runs from the
				// offline, operator-fenced recovery procedure.
				if status.Code(err) != codes.NotFound {
					return out, err
				}
				if _, e := b.Client.control.CreateActorEgressPolicy(ctx, &pb.CreateActorEgressPolicyRequest{Actor: taskRef(in.Task), EgressPolicy: desired}); e != nil && status.Code(e) != codes.AlreadyExists {
					return out, e
				}
				actual, err = b.Client.control.GetActorEgressPolicy(ctx, &pb.GetActorEgressPolicyRequest{Actor: taskRef(in.Task)})
				if err != nil {
					return out, err
				}
			}
			if !proto.Equal(&pb.EgressPolicy{Rules: desired.Rules}, &pb.EgressPolicy{Rules: actual.Rules}) {
				// PATCH(recovery-install-egress): debug aid for schema drift.
				slog.Error("egress differs", "desired", desired.String(), "actual", actual.String())
				return out, status.Error(codes.FailedPrecondition, "task egress installation is incomplete or differs")
			}
		}
		out.BackendUID = observed.UID
		out.Task = observed
		return out, nil
	case in.Runtime != nil && in.Kind == "Prepare":
		desired, err := b.BuildRuntime(in.Runtime, in.Group)
		if err != nil {
			return out, err
		}
		actual, err := b.Client.GetActorTemplate(ctx, in.Runtime.Metadata.Atespace, runtimeName(in.Runtime))
		if err != nil {
			return out, err
		}
		out.BackendUID = actual.GetMetadata().GetUid()
		if out.BackendUID == "" || in.BackendUID != "" && out.BackendUID != in.BackendUID {
			return out, status.Error(codes.FailedPrecondition, "runtime identity changed")
		}
		comparable := proto.CloneOf(actual)
		comparable.Metadata = desired.Metadata
		comparable.Status = nil
		if !proto.Equal(comparable, desired) {
			return out, status.Error(codes.FailedPrecondition, "runtime configuration changed")
		}
		out.Phase, err = b.ObserveRuntime(ctx, in.Runtime, out.BackendUID)
		return out, err
	case in.Group != nil && (in.Kind == "Create" || in.Kind == "Update"):
		p, ok := b.Platform.(interface {
			InspectPool(context.Context, *ax.TaskGroup, string) (string, error)
		})
		if !ok {
			return out, status.Error(codes.FailedPrecondition, "platform cannot inspect pool")
		}
		var err error
		out.BackendUID, err = p.InspectPool(ctx, in.Group, in.BackendUID)
		if err != nil {
			return out, err
		}
		out.ReadyWorkers, err = b.ReadyWorkers(ctx, in.Group, out.BackendUID)
		out.Phase = "Ready"
		if out.ReadyWorkers < in.Group.Spec.GetReplicas() {
			out.Phase = "Provisioning"
		}
		return out, err
	}
	return out, status.Error(codes.InvalidArgument, "unsupported recovery intent")
}

func (p *KubernetesPlatform) InspectPool(ctx context.Context, g *ax.TaskGroup, expected string) (string, error) {
	var pool poolObject
	if err := p.request(ctx, http.MethodGet, poolPath(g)+"/"+poolName(g), nil, &pool); err != nil {
		return "", err
	}
	if pool.Metadata.UID == "" || expected != "" && pool.Metadata.UID != expected || pool.Metadata.DeletionTimestamp != "" || pool.Metadata.Labels[GroupLabel] != g.Metadata.Uid {
		return "", status.Error(codes.FailedPrecondition, "pool identity changed")
	}
	profile, ok := p.Profiles[g.Metadata.Atespace+"/"+g.Metadata.Name]
	if !ok {
		profile, ok = p.Profiles[g.Spec.SandboxClass]
	}
	if !ok || pool.Spec.Replicas != g.Spec.GetReplicas() || pool.Spec.WorkerImage != profile.WorkerImage || pool.Spec.SandboxClass != g.Spec.SandboxClass || !equalJSON(pool.Spec.Template, profile.Template) {
		return "", status.Error(codes.FailedPrecondition, "pool desired state differs")
	}
	for k, v := range profile.Labels {
		if pool.Metadata.Labels[k] != v {
			return "", status.Error(codes.FailedPrecondition, "pool labels differ")
		}
	}
	for k, v := range profile.Annotations {
		if pool.Metadata.Annotations[k] != v {
			return "", status.Error(codes.FailedPrecondition, "pool annotations differ")
		}
	}
	return pool.Metadata.UID, nil
}
func (p *KubernetesPlatform) VerifyCredential(ctx context.Context, ref *ax.ResourceRef, expectedHash string) (*ax.CredentialSecretRef, error) {
	var secret secretObject
	if err := p.request(ctx, http.MethodGet, secretPath(ref)+"/"+credentialName(ref), nil, &secret); err != nil {
		return nil, err
	}
	token, err := base64.StdEncoding.DecodeString(secret.Data["credential"])
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(token)
	if secret.Metadata.Labels["ax.io/task-uid"] != ref.Uid || secret.Metadata.DeletionTimestamp != "" || hex.EncodeToString(sum[:]) != expectedHash {
		return nil, status.Error(codes.FailedPrecondition, "runtime credential evidence differs")
	}
	return &ax.CredentialSecretRef{Namespace: ref.Atespace, Name: credentialName(ref), Key: "credential"}, nil
}

func equalJSON(a, b []byte) bool {
	var left, right any
	if len(a) > 0 {
		if json.Unmarshal(a, &left) != nil {
			return false
		}
	}
	if len(b) > 0 {
		if json.Unmarshal(b, &right) != nil {
			return false
		}
	}
	return reflect.DeepEqual(left, right)
}
