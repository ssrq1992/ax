package substrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	pb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const GroupLabel = "ax.io/task-group-uid"
const trustMount = "/run/ax/trust"

// Platform owns Kubernetes resources; backend metadata never enters AX's API.
type Platform interface {
	ObservePool(context.Context, *ax.TaskGroup, string) error
	EnsurePool(context.Context, *ax.TaskGroup, string) (string, error)
	DeletePool(context.Context, *ax.TaskGroup, string) error
	EnsureCredential(context.Context, *ax.ResourceRef, string) (*ax.CredentialSecretRef, error)
	DeleteCredential(context.Context, *ax.ResourceRef) error
}
type ManagedBackend struct {
	Client         *Client
	Platform       Platform
	GuestImage     string
	CallbackURL    string
	SandboxConfigs map[string]string
}
type Observation struct {
	UID         string
	Phase       string
	TemplateUID string
	SnapshotURI string
	Data        bool
}

func backendRef(r *ax.ResourceRef) *pb.ObjectRef {
	return &pb.ObjectRef{Atespace: r.Atespace, Name: r.Name}
}
func runtimeName(r *ax.PreparedRuntime) string {
	return "axr-" + strings.ReplaceAll(r.Metadata.Uid, "-", "")
}
func taskName(t *ax.Task) string { return "axt-" + strings.ReplaceAll(t.Metadata.Uid, "-", "") }
func taskRef(t *ax.Task) *pb.ObjectRef {
	return &pb.ObjectRef{Atespace: t.Metadata.Atespace, Name: taskName(t)}
}
func CheckpointName(c *ax.TaskCheckpoint) string {
	return "axc-" + strings.ReplaceAll(c.Metadata.Uid, "-", "")
}

func (b *ManagedBackend) EnsureGroup(ctx context.Context, g *ax.TaskGroup, poolUID string) (string, int32, error) {
	if err := b.Client.EnsureAtespace(ctx, g.Metadata.Atespace); err != nil {
		return "", 0, err
	}
	uid, err := b.Platform.EnsurePool(ctx, g, poolUID)
	if err != nil {
		return "", 0, err
	}
	workers, err := b.groupWorkers(ctx, g.Metadata.Uid)
	if err != nil {
		return uid, 0, err
	}
	var ready int32
	for _, w := range workers {
		if w.GetStatus().GetState() == pb.WorkerState_WORKER_STATE_ACTIVE && w.GetStatus().GetCapacity() != nil {
			ready++
		}
	}
	return uid, ready, nil
}
func (b *ManagedBackend) groupWorkers(ctx context.Context, uid string) ([]*pb.Worker, error) {
	var result []*pb.Worker
	page := ""
	for {
		res, err := b.Client.control.ListWorkers(ctx, &pb.ListWorkersRequest{PageToken: page})
		if err != nil {
			return nil, err
		}
		for _, w := range res.GetWorkers() {
			if w.GetLabels()[GroupLabel] == uid {
				result = append(result, w)
			}
		}
		page = res.GetNextPageToken()
		if page == "" {
			return result, nil
		}
	}
}
func (b *ManagedBackend) GroupIdle(ctx context.Context, g *ax.TaskGroup) error {
	workers, err := b.groupWorkers(ctx, g.Metadata.Uid)
	if err != nil {
		return err
	}
	names := map[string]bool{}
	for _, w := range workers {
		names[w.GetMetadata().GetName()] = true
	}
	page := ""
	for {
		res, err := b.Client.control.ListActors(ctx, &pb.ListActorsRequest{PageToken: page})
		if err != nil {
			return err
		}
		for _, a := range res.GetActors() {
			if names[a.GetStatus().GetWorkerAssignment().GetWorker().GetName()] {
				return status.Error(codes.FailedPrecondition, "task group has assigned workloads")
			}
		}
		page = res.GetNextPageToken()
		if page == "" {
			return nil
		}
	}
}
func (b *ManagedBackend) DeleteGroup(ctx context.Context, g *ax.TaskGroup, uid string) error {
	if err := b.GroupIdle(ctx, g); err != nil {
		return err
	}
	return b.Platform.DeletePool(ctx, g, uid)
}
func (b *ManagedBackend) BuildRuntime(r *ax.PreparedRuntime, g *ax.TaskGroup) (*pb.ActorTemplate, error) {
	s := r.Spec
	if err := ax.ValidatePreparedRuntime(s); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	class := pb.SandboxClass_SANDBOX_CLASS_GVISOR
	if g.Spec.SandboxClass == "microvm" {
		class = pb.SandboxClass_SANDBOX_CLASS_MICROVM
	}
	config := b.SandboxConfigs[g.Spec.SandboxClass]
	if config == "" {
		return nil, status.Error(codes.FailedPrecondition, "sandbox class has no configured platform environment")
	}
	location := g.Spec.SnapshotLocation
	if s.SnapshotLocationOverride != "" {
		location = s.SnapshotLocationOverride
	}
	ready := s.Readiness
	if ready == nil {
		port := int32(8081)
		if s.Kind == "Sandbox" {
			port = 80
		}
		ready = &ax.Readiness{Path: "/readyz", Port: port, TimeoutSeconds: 30}
	}
	container := &pb.Container{Name: "workload", Image: s.Image, Command: s.Command, Args: s.Args,
		WakeupProbe:  &pb.ContainerWakeupProbe{HttpGet: &pb.HTTPGetAction{Path: ready.Path, Port: ready.Port}, TimeoutSeconds: ready.TimeoutSeconds},
		VolumeMounts: []*pb.VolumeMount{{Name: "data", MountPath: "/data"}, {Name: "trust", MountPath: trustMount}}}
	for _, e := range s.Env {
		container.Env = append(container.Env, &pb.EnvVar{Name: e.Name, Value: e.Value})
	}
	for _, name := range []string{"SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "AWS_CA_BUNDLE", "NODE_EXTRA_CA_CERTS", "CURL_CA_BUNDLE", "GIT_SSL_CAINFO", "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH"} {
		container.Env = append(container.Env, &pb.EnvVar{Name: name, Value: trustMount + "/ca.pem"})
	}
	container.Env = append(container.Env, &pb.EnvVar{Name: "SSL_CERT_DIR", Value: trustMount})
	tmpl := &pb.ActorTemplate{Metadata: &pb.ResourceMetadata{Atespace: r.Metadata.Atespace, Name: runtimeName(r)},
		WorkerSelector: &pb.Selector{MatchLabels: map[string]string{GroupLabel: g.Metadata.Uid}},
		Containers:     []*pb.Container{container}, SandboxConfig: &pb.SandboxConfig{SandboxClass: class, ConfigName: config},
		SnapshotConfig: &pb.SnapshotConfig{OnPause: pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL, OnCommit: pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA, OnResume: &pb.OnResumeConfig{FromData: pb.ResumeSource_RESUME_SOURCE_GOLDEN}, StorageLocation: location},
		Volumes:         []*pb.Volume{{Name: "data", DurableDir: &pb.DurableDirVolumeSource{}}, {Name: "trust", SystemInfo: &pb.SystemInfoVolumeSource{DataSources: []*pb.SystemInfoDataSource{{TrustBundle: &pb.TrustBundleDataSource{Name: "egress-mitm.ate.dev", Path: "ca.pem"}}}}}}}
	if s.Kind == "Sandbox" {
		if !strings.Contains(b.GuestImage, "@sha256:") {
			return nil, status.Error(codes.FailedPrecondition, "AX Guest image must be pinned")
		}
		tmpl.Volumes = append(tmpl.Volumes, &pb.Volume{Name: "guest", Image: &pb.ImageVolumeSource{Reference: b.GuestImage}})
		container.VolumeMounts = append(container.VolumeMounts, &pb.VolumeMount{Name: "guest", MountPath: "/run/ax/guest"})
		container.Command = []string{"/run/ax/guest/usr/local/bin/ax-sandbox-guest"}
		container.Args = []string{"--listen=:80", "--workspace=/data/workspace", "--log-dir=/data/guest-logs"}
	}
	if s.Resources != nil {
		limits := s.Resources.GetLimits()
		requests := s.Resources.GetRequests()
		if requests != nil && !proto.Equal(requests, limits) {
			return nil, status.Error(codes.InvalidArgument, "distinct requests and limits are not supported by this runtime")
		}
		tmpl.Resources = &pb.Resources{}
		if limits.GetCpu() != "" {
			tmpl.Resources.Limits = append(tmpl.Resources.Limits, &pb.Limits{Name: "cpu", Quantity: limits.Cpu})
		}
		if limits.GetMemory() != "" {
			tmpl.Resources.Limits = append(tmpl.Resources.Limits, &pb.Limits{Name: "memory", Quantity: limits.Memory})
		}
	}
	return tmpl, nil
}
func (b *ManagedBackend) Prepare(ctx context.Context, r *ax.PreparedRuntime, g *ax.TaskGroup) (string, string, error) {
	desired, err := b.BuildRuntime(r, g)
	if err != nil {
		return "", "", err
	}
	existing, err := b.Client.GetActorTemplate(ctx, r.Metadata.Atespace, desired.Metadata.Name)
	if status.Code(err) == codes.NotFound {
		existing, err = b.Client.control.CreateActorTemplate(ctx, &pb.CreateActorTemplateRequest{ActorTemplate: desired})
	}
	if err != nil {
		return "", "", err
	}
	actual := proto.Clone(existing).(*pb.ActorTemplate)
	actual.Metadata = desired.Metadata
	actual.Status = nil
	if !proto.Equal(actual, desired) {
		return "", "", status.Error(codes.FailedPrecondition, "immutable runtime backend differs")
	}
	golden := existing.GetStatus().GetGoldenSnapshotStatus()
	if golden.GetErrorMessage() != "" {
		return existing.Metadata.Uid, "Failed", status.Error(codes.FailedPrecondition, golden.ErrorMessage)
	}
	phase := "Preparing"
	if golden.GetGoldenTag() != nil {
		phase = "Ready"
	}
	return existing.GetMetadata().GetUid(), phase, nil
}
func (b *ManagedBackend) DeleteRuntime(ctx context.Context, r *ax.PreparedRuntime, uid string) error {
	tmpl, err := b.Client.GetActorTemplate(ctx, r.Metadata.Atespace, runtimeName(r))
	if status.Code(err) == codes.NotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if tmpl.GetMetadata().GetUid() != uid {
		return status.Error(codes.FailedPrecondition, "runtime backend identity changed")
	}
	if err := b.Client.DeleteActorTemplate(ctx, r.Metadata.Atespace, runtimeName(r)); err != nil {
		return err
	}
	return waitManagedDeletion(ctx, uid, func() (string, error) {
		current, err := b.Client.GetActorTemplate(ctx, r.Metadata.Atespace, runtimeName(r))
		return current.GetMetadata().GetUid(), err
	})
}
func observe(a *pb.Actor) Observation {
	phase := strings.TrimPrefix(a.GetStatus().GetState().String(), "ACTOR_STATE_")
	phases := map[string]string{"RUNNING": "Running", "SUSPENDED": "Suspended", "PAUSED": "Paused", "CRASHED": "Crashed", "RESUMING": "Transitioning", "SUSPENDING": "Transitioning", "PAUSING": "Transitioning", "DELETING": "Deleting"}
	if p := phases[phase]; p != "" {
		phase = p
	} else {
		phase = "Unknown"
	}
	snapshot := a.GetStatus().GetExternalSnapshot()
	// PATCH(new-egress-schema): ActorStatus no longer carries the actor
	// template UID (Actor.actor_template is a name-only ObjectRef now).
	// Callers that need template lineage resolve it via b.Observe.
	return Observation{UID: a.GetMetadata().GetUid(), Phase: phase, SnapshotURI: snapshot.GetSnapshotUri(), Data: snapshot.GetContentScope() == pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA}
}
func (b *ManagedBackend) Observe(ctx context.Context, t *ax.Task) (Observation, error) {
	a, err := b.Client.GetActor(ctx, t.Metadata.Atespace, taskName(t))
	if err != nil {
		return Observation{}, err
	}
	o := observe(a)
	// PATCH(new-egress-schema): resolve the actor template UID through the
	// template ref; Checkpoint lineage checks still need it.
	if name := a.GetActorTemplate().GetName(); name != "" {
		if tmpl, err := b.Client.GetActorTemplate(ctx, a.GetActorTemplate().GetAtespace(), name); err == nil {
			o.TemplateUID = tmpl.GetMetadata().GetUid()
		}
	}
	return o, nil
}
func (b *ManagedBackend) Create(ctx context.Context, t *ax.Task, r *ax.PreparedRuntime, c *ax.TaskCheckpoint, token string) (Observation, error) {
	a := &pb.Actor{Metadata: &pb.ResourceMetadata{Atespace: t.Metadata.Atespace, Name: taskName(t)}, ActorTemplate: &pb.ObjectRef{Atespace: r.Metadata.Atespace, Name: runtimeName(r)}}
	if c != nil {
		a.SourceTag = &pb.ObjectRef{Atespace: c.Metadata.Atespace, Name: CheckpointName(c)}
	}
	res, err := b.Client.control.CreateActor(ctx, &pb.CreateActorRequest{Actor: a})
	if err != nil {
		return Observation{}, err
	}
	if res.GetStatus().GetState() != pb.ActorState_ACTOR_STATE_SUSPENDED {
		return Observation{}, status.Error(codes.FailedPrecondition, "new backend did not remain suspended")
	}
	policy, err := b.Egress(ctx, t, r, token)
	if err != nil {
		return observe(res), err
	}
	_, err = b.Client.control.CreateActorEgressPolicy(ctx, &pb.CreateActorEgressPolicyRequest{Actor: taskRef(t), EgressPolicy: policy})
	if status.Code(err) == codes.AlreadyExists {
		actual, e := b.Client.control.GetActorEgressPolicy(ctx, &pb.GetActorEgressPolicyRequest{Actor: taskRef(t)})
		if e != nil {
			return observe(res), e
		}
		if !proto.Equal(&pb.EgressPolicy{Rules: actual.Rules}, &pb.EgressPolicy{Rules: policy.Rules}) {
			return observe(res), status.Error(codes.FailedPrecondition, "runtime egress policy differs")
		}
		err = nil
	}
	return observe(res), err
}
func (b *ManagedBackend) Transition(ctx context.Context, t *ax.Task, uid, kind string) (Observation, error) {
	current, err := b.Observe(ctx, t)
	if status.Code(err) == codes.NotFound && kind == "Delete" {
		return Observation{UID: uid, Phase: "Deleted"}, b.Platform.DeleteCredential(ctx, ax.Ref(t.Metadata))
	}
	if err != nil {
		return Observation{}, err
	}
	if current.UID != uid {
		return Observation{}, status.Error(codes.FailedPrecondition, "backend identity changed")
	}
	var a *pb.Actor
	switch kind {
	case "Resume":
		res, e := b.Client.control.ResumeActor(ctx, &pb.ResumeActorRequest{Actor: taskRef(t)})
		err = e
		a = res.GetActor()
	case "Suspend":
		res, e := b.Client.control.SuspendActor(ctx, &pb.SuspendActorRequest{Actor: taskRef(t)})
		err = e
		a = res.GetActor()
	case "Pause":
		res, e := b.Client.control.PauseActor(ctx, &pb.PauseActorRequest{Actor: taskRef(t)})
		err = e
		a = res.GetActor()
	case "Delete":
		err = b.Client.DeleteActor(ctx, t.Metadata.Atespace, taskName(t))
		if err == nil {
			err = b.Platform.DeleteCredential(ctx, ax.Ref(t.Metadata))
		}
		return Observation{UID: uid, Phase: "Deleted"}, err
	default:
		return Observation{}, status.Error(codes.InvalidArgument, "unknown runtime operation")
	}
	if err != nil {
		return Observation{}, err
	}
	o := observe(a)
	if o.UID != uid {
		return Observation{}, status.Error(codes.FailedPrecondition, "backend identity changed during operation")
	}
	return o, nil
}
func (b *ManagedBackend) Egress(ctx context.Context, t *ax.Task, r *ax.PreparedRuntime, token string) (*pb.EgressPolicy, error) {
	eg := &ax.EgressSpec{}
	if r.Spec.GetEgress() != nil {
		eg = proto.Clone(r.Spec.Egress).(*ax.EgressSpec)
	}
	portsByHost := map[string]int{}
	if r.Spec.Kind == "Service" {
		u, err := url.Parse(b.CallbackURL)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" {
			return nil, status.Error(codes.FailedPrecondition, "HTTPS callback URL is required")
		}
		secret, err := b.Platform.EnsureCredential(ctx, ax.Ref(t.Metadata), token)
		if err != nil {
			return nil, err
		}
		eg.Destinations = append(eg.Destinations, u.Hostname())
		eg.Credentials = append(eg.Credentials, &ax.EgressCredential{Hostname: u.Hostname(), Header: ax.RuntimeCredentialHeader, SecretKeyRef: secret})
		// PATCH(new-egress-schema): the callback may live on a non-standard
		// HTTPS port (kagent runs its controller on 8083). The new substrate
		// egress schema matches ports per rule, so carry the callback port
		// through instead of relying on the old any-port hostname match.
		if p := u.Port(); p != "" {
			if n, err := strconv.Atoi(p); err == nil && n > 0 && n < 65536 {
				portsByHost[strings.ToLower(u.Hostname())] = n
			}
		}
	}
	return CompileEgress(t.Metadata.Atespace, eg, portsByHost)
}
// PATCH(new-egress-schema): the deployed substrate replaced the old
// hostnames/cidrs/all EgressRule union with protocol-scoped rules
// (http/https/tls_passthrough), per-rule Ports, and
// HttpRuleEffects.ReplaceHeaders (formerly EgressRuleEffects.InjectStaticHeaders).
// Agent runtimes speak HTTPS (MITM-intercepted by the egress gateway, which is
// also what injects their credentials), so destinations compile to HTTPSRule
// entries. IP-literal destinations have no equivalent matcher in the new
// schema and are rejected explicitly instead of silently failing validation.
func CompileEgress(space string, s *ax.EgressSpec, portsByHost map[string]int) (*pb.EgressPolicy, error) {
	hosts := map[string]bool{}
	for _, d := range s.GetDestinations() {
		if _, err := netip.ParseAddr(d); err == nil {
			return nil, status.Error(codes.InvalidArgument, "IP-literal egress destinations are not supported by the deployed substrate egress schema; use hostnames")
		}
		d = strings.TrimSuffix(strings.ToLower(d), ".")
		if d == "" || strings.ContainsAny(d, " /:*\\") {
			return nil, status.Error(codes.InvalidArgument, "invalid egress hostname")
		}
		hosts[d] = true
	}
	rules := map[string]*pb.HTTPSRule{}
	seen := map[string]bool{}
	for _, c := range s.GetCredentials() {
		host := strings.ToLower(c.Hostname)
		header := strings.ToLower(c.Header)
		ref := c.SecretKeyRef
		if !hosts[host] || header == "" || seen[host+"/"+header] || ref.GetNamespace() != space || ax.ValidateName(ref.GetName()) != nil || ref.GetKey() == "" || strings.ContainsAny(ref.GetKey(), "/\\") {
			return nil, status.Error(codes.InvalidArgument, "invalid or conflicting credential binding")
		}
		seen[host+"/"+header] = true
		rule := rules[host]
		if rule == nil {
			rule = &pb.HTTPSRule{Hostnames: []string{host}, Effects: &pb.HttpRuleEffects{}}
			rules[host] = rule
		}
		rule.Effects.ReplaceHeaders = append(rule.Effects.ReplaceHeaders, &pb.CredentialHeader{Header: header, Prefix: c.Prefix, CredentialUri: fmt.Sprintf("ate-secret://k8s.io/default/%s/%s/%s", ref.Namespace, ref.Name, ref.Key)})
		if len(rule.Effects.ReplaceHeaders) > 16 {
			return nil, status.Error(codes.InvalidArgument, "too many credential headers")
		}
	}
	result := &pb.EgressPolicy{Metadata: &pb.ResourceMetadata{Atespace: space, Name: "default"}}
	keys := make([]string, 0, len(rules))
	for k := range rules {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		rule := rules[k]
		// PATCH(new-egress-schema): substrate materializes the default HTTPS
		// port on read-back; set it explicitly so re-read comparisons match.
		rule.Ports = &pb.Ports{Numbers: []int32{443}}
		if p, ok := portsByHost[k]; ok {
			rule.Ports = &pb.Ports{Numbers: []int32{int32(p)}}
		}
		result.Rules = append(result.Rules, &pb.EgressRule{Https: rule})
	}
	// PATCH(new-egress-schema): the new schema forbids a hostname from
	// matching two rules on the same port ("ties" validation), so the
	// catch-all plain rule must exclude hosts already matched by their
	// credential rule above.
	keys = nil
	for k := range hosts {
		if _, covered := rules[k]; covered {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) > 0 {
		// PATCH(new-egress-schema): explicit default port, same as above.
		result.Rules = append(result.Rules, &pb.EgressRule{Https: &pb.HTTPSRule{Hostnames: keys, Ports: &pb.Ports{Numbers: []int32{443}}}})
	}
	if len(result.Rules) > 256 {
		return nil, status.Error(codes.InvalidArgument, "too many egress rules")
	}
	return result, nil
}
func (b *ManagedBackend) Target(t *ax.Task) string { return t.Metadata.Atespace + "/" + taskName(t) }

// ObserveRuntime never recreates missing infrastructure. Polling preparation is
// read-only, including after server restarts.
func (b *ManagedBackend) ObserveRuntime(ctx context.Context, r *ax.PreparedRuntime, expectedUID string) (string, error) {
	t, err := b.Client.GetActorTemplate(ctx, r.Metadata.Atespace, runtimeName(r))
	if err != nil {
		return "", err
	}
	if t.GetMetadata().GetUid() != expectedUID {
		return "", status.Error(codes.FailedPrecondition, "runtime backend identity changed")
	}
	golden := t.GetStatus().GetGoldenSnapshotStatus()
	if golden.GetErrorMessage() != "" {
		return "Failed", nil
	}
	if golden.GetGoldenTag() != nil {
		return "Ready", nil
	}
	return "Preparing", nil
}

func (b *ManagedBackend) Checkpoint(ctx context.Context, t *ax.Task, expectedUID string, c *ax.TaskCheckpoint) (string, error) {
	before, err := b.Observe(ctx, t)
	if err != nil {
		return "", err
	}
	if before.UID != expectedUID || before.Phase != "Suspended" || !before.Data || before.SnapshotURI == "" {
		return "", status.Error(codes.FailedPrecondition, "checkpoint requires the suspended data boundary")
	}
	sum := sha256.Sum256([]byte(before.UID + "\x00" + before.SnapshotURI))
	if hex.EncodeToString(sum[:]) != c.BoundaryRef {
		return "", status.Error(codes.FailedPrecondition, "checkpoint boundary changed")
	}
	tag, err := b.Client.control.CreateTag(ctx, &pb.CreateTagRequest{Tag: &pb.Tag{Metadata: &pb.ResourceMetadata{Atespace: c.Metadata.Atespace, Name: CheckpointName(c)}, Scope: pb.TagScope_TAG_SCOPE_ATESPACE, SourceActor: taskRef(t)}})
	if err != nil {
		return "", err
	}
	// PATCH(new-egress-schema): Tag.source_actor is a name-only ObjectRef now;
	// the source-actor identity is already pinned by `before.UID == expectedUID`
	// above, so the ref comparison only guards against a tag from another task.
	if !proto.Equal(tag.GetSourceActor(), taskRef(t)) || tag.GetStatus().GetActorTemplateUid() != before.TemplateUID || tag.GetStatus().GetSnapshot().GetSnapshotUri() == "" || tag.GetStatus().GetSnapshot().GetContentScope() != pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA {
		return tag.GetMetadata().GetUid(), status.Error(codes.FailedPrecondition, "checkpoint lineage or data snapshot differs")
	}
	after, err := b.Observe(ctx, t)
	if err != nil {
		return tag.Metadata.Uid, err
	}
	if before.UID != after.UID || before.SnapshotURI != after.SnapshotURI || after.Phase != "Suspended" {
		return tag.Metadata.Uid, status.Error(codes.FailedPrecondition, "source changed during checkpoint")
	}
	return tag.Metadata.Uid, nil
}
func (b *ManagedBackend) DeleteCheckpoint(ctx context.Context, c *ax.TaskCheckpoint, expectedUID string) error {
	ref := &pb.ObjectRef{Atespace: c.Metadata.Atespace, Name: CheckpointName(c)}
	tag, err := b.Client.control.GetTag(ctx, &pb.GetTagRequest{Tag: ref})
	if status.Code(err) == codes.NotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if tag.GetMetadata().GetUid() != expectedUID {
		return status.Error(codes.FailedPrecondition, "checkpoint backend identity changed")
	}
	_, err = b.Client.control.DeleteTag(ctx, &pb.DeleteTagRequest{Tag: ref})
	if err != nil {
		return err
	}
	return waitManagedDeletion(ctx, expectedUID, func() (string, error) {
		current, err := b.Client.control.GetTag(ctx, &pb.GetTagRequest{Tag: ref})
		return current.GetMetadata().GetUid(), err
	})
}

func (b *ManagedBackend) VerifyPreparation(ctx context.Context, r *ax.PreparedRuntime, runtimeUID string, c *ax.TaskCheckpoint, checkpointUID string) error {
	phase, err := b.ObserveRuntime(ctx, r, runtimeUID)
	if err != nil {
		return err
	}
	if phase != "Ready" {
		return status.Error(codes.FailedPrecondition, "prepared runtime is no longer ready")
	}
	if c == nil {
		return nil
	}
	tag, err := b.Client.control.GetTag(ctx, &pb.GetTagRequest{Tag: &pb.ObjectRef{Atespace: c.Metadata.Atespace, Name: CheckpointName(c)}})
	if err != nil {
		return err
	}
	if tag.GetMetadata().GetUid() != checkpointUID || tag.GetStatus().GetActorTemplateUid() != runtimeUID || tag.GetScope() != pb.TagScope_TAG_SCOPE_ATESPACE || tag.GetStatus().GetSnapshot().GetSnapshotUri() == "" || tag.GetStatus().GetSnapshot().GetContentScope() != pb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA {
		return status.Error(codes.FailedPrecondition, "checkpoint identity or lineage changed")
	}
	return nil
}

// A deletion acknowledgement is not proof that a referenced resource is gone.
func waitManagedDeletion(ctx context.Context, expectedUID string, read func() (string, error)) error {
	for {
		currentUID, err := read()
		if status.Code(err) == codes.NotFound {
			return nil
		}
		if err != nil {
			return err
		}
		if currentUID != expectedUID {
			return status.Error(codes.FailedPrecondition, "resource identity changed during deletion")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (b *ManagedBackend) ReadyWorkers(ctx context.Context, g *ax.TaskGroup, uid string) (int32, error) {
	if err := b.Platform.ObservePool(ctx, g, uid); err != nil {
		return 0, err
	}
	workers, err := b.groupWorkers(ctx, g.Metadata.Uid)
	if err != nil {
		return 0, err
	}
	var ready int32
	for _, w := range workers {
		if w.GetStatus().GetState() == pb.WorkerState_WORKER_STATE_ACTIVE && w.GetStatus().GetCapacity() != nil {
			ready++
		}
	}
	return ready, nil
}
