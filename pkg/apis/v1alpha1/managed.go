package v1alpha1

import (
	"encoding/hex"
	"fmt"
	"gopkg.in/yaml.v3"
	"net/url"
	"strings"
	"unicode/utf8"
)

const KindTaskGroup = "TaskGroup"
const RuntimeCredentialHeader = "x-ax-runtime-credential"
const TargetTaskHeader = "ax-target-task"
const TargetTaskUIDHeader = "ax-target-task-uid"

func (g *TaskGroup) MarshalYAML() (any, error)        { return marshalYAML(g) }
func (g *TaskGroup) UnmarshalYAML(n *yaml.Node) error { return unmarshalYAML(n, g, nil) }
func Ref(m *ObjectMeta) *ResourceRef {
	return &ResourceRef{Atespace: m.GetAtespace(), Name: m.GetName(), Uid: m.GetUid()}
}
func ValidateRef(ref *ResourceRef, uid bool) error {
	if err := ValidateName(ref.GetName()); err != nil {
		return fmt.Errorf("reference name: %w", err)
	}
	if err := ValidateName(ref.GetAtespace()); err != nil {
		return fmt.Errorf("reference atespace: %w", err)
	}
	if uid && ref.GetUid() == "" {
		return fmt.Errorf("reference uid is required")
	}
	return nil
}
func ValidateTaskGroup(g *TaskGroup) error {
	if err := ValidateObjectMeta(g.GetMetadata()); err != nil {
		return err
	}
	if ValidateName(g.GetMetadata().GetAtespace()) != nil {
		return fmt.Errorf("task group atespace is required")
	}
	s := g.GetSpec()
	if s == nil {
		return fmt.Errorf("group spec is required")
	}
	if s.GetReplicas() < 0 {
		return fmt.Errorf("replicas must not be negative")
	}
	if s.SandboxClass != "" && s.SandboxClass != "gvisor" && s.SandboxClass != "microvm" {
		return fmt.Errorf("unsupported sandboxClass %q", s.SandboxClass)
	}
	return ValidateSnapshotLocation(s.SnapshotLocation)
}
func ValidateSnapshotLocation(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "gs" && u.Scheme != "s3") {
		return fmt.Errorf("snapshotLocation must be a gs:// or s3:// bucket path without credentials")
	}
	return nil
}
func ValidatePreparedRuntime(s *PreparedRuntimeSpec) error {
	if s == nil {
		return fmt.Errorf("runtime spec is required")
	}
	if s.Kind != "Service" && s.Kind != "Sandbox" {
		return fmt.Errorf("runtime kind must be Service or Sandbox")
	}
	parts := strings.Split(s.Image, "@sha256:")
	validDigest := false
	if len(parts) == 2 && parts[0] != "" && len(parts[1]) == 64 {
		_, err := hex.DecodeString(parts[1])
		validDigest = err == nil
	}
	if !validDigest {
		return fmt.Errorf("runtime image must be pinned by sha256 digest")
	}
	if ValidateRef(s.GroupRef, true) != nil {
		return fmt.Errorf("task group reference is required")
	}
	if s.SnapshotLocationOverride != "" {
		if err := ValidateSnapshotLocation(s.SnapshotLocationOverride); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	// Eight trust variables are added by the backend constructor.
	if len(s.Env) > 24 {
		return fmt.Errorf("runtime environment exceeds the platform limit")
	}
	for _, e := range s.Env {
		if e.GetName() == "" || seen[e.Name] || utf8.RuneCountInString(e.Value) > 32768 {
			return fmt.Errorf("invalid or duplicate runtime environment variable")
		}
		if e.Name == "SSL_CERT_FILE" || e.Name == "SSL_CERT_DIR" || e.Name == "REQUESTS_CA_BUNDLE" || e.Name == "AWS_CA_BUNDLE" || e.Name == "NODE_EXTRA_CA_CERTS" || e.Name == "CURL_CA_BUNDLE" || e.Name == "GIT_SSL_CAINFO" || e.Name == "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH" {
			return fmt.Errorf("environment %q is reserved for AX trust", e.Name)
		}
		seen[e.Name] = true
	}
	if r := s.Readiness; r != nil {
		if r.Port < 1 || r.Port > 65535 || !strings.HasPrefix(r.Path, "/") || strings.ContainsAny(r.Path, "?#") || r.TimeoutSeconds < 1 || r.TimeoutSeconds > 3600 {
			return fmt.Errorf("invalid runtime readiness")
		}
	}
	return nil
}

// RuntimeCredentialAtespace extracts only an untrusted routing hint. Consumers
// must authenticate the complete opaque credential before using any identity.
func RuntimeCredentialAtespace(credential string) (string, error) {
	space, secret, ok := strings.Cut(credential, ":")
	if !ok || ValidateName(space) != nil || len(secret) != 43 {
		return "", fmt.Errorf("invalid AX runtime credential")
	}
	return space, nil
}
