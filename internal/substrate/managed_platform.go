package substrate

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PoolProfile is administrator-owned configuration, never accepted from Task clients.
// Template retains Kubernetes scheduling/resource settings when converting an
// existing installation's configuration. Profiles are immutable while in use.
type PoolProfile struct {
	WorkerImage string            `json:"workerImage"`
	Template    json.RawMessage   `json:"template,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}
type KubernetesPlatform struct {
	Endpoint  string
	HTTP      *http.Client
	TokenFile string
	Profiles  map[string]PoolProfile
}

func NewInClusterPlatform(profiles map[string]PoolProfile) (*KubernetesPlatform, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
	if host == "" || port == "" {
		return nil, fmt.Errorf("Kubernetes service endpoint is required")
	}
	ca, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("invalid Kubernetes CA")
	}
	return &KubernetesPlatform{Endpoint: "https://" + host + ":" + port, HTTP: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}, TokenFile: "/var/run/secrets/kubernetes.io/serviceaccount/token", Profiles: profiles}, nil
}

type platformMeta struct {
	Name              string            `json:"name,omitempty"`
	Namespace         string            `json:"namespace,omitempty"`
	UID               string            `json:"uid,omitempty"`
	Version           string            `json:"resourceVersion,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
}
type poolSpec struct {
	Replicas     int32           `json:"replicas"`
	WorkerImage  string          `json:"workerImage"`
	SandboxClass string          `json:"sandboxClass"`
	Template     json.RawMessage `json:"template,omitempty"`
}
type poolObject struct {
	APIVersion string       `json:"apiVersion"`
	Kind       string       `json:"kind"`
	Metadata   platformMeta `json:"metadata"`
	Spec       poolSpec     `json:"spec"`
}
type secretObject struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   platformMeta      `json:"metadata"`
	Data       map[string]string `json:"data"`
	Type       string            `json:"type"`
}

func (p *KubernetesPlatform) request(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(p.Endpoint, "/")+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.TokenFile != "" {
		token, err := os.ReadFile(p.TokenFile)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	}
	res, err := p.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		code := codes.Unavailable
		switch res.StatusCode {
		case 400, 422:
			code = codes.InvalidArgument
		case 401:
			code = codes.Unauthenticated
		case 403:
			code = codes.PermissionDenied
		case 404:
			code = codes.NotFound
		case 409:
			if method == http.MethodPost {
				code = codes.AlreadyExists
			} else {
				code = codes.Aborted
			}
		}
		// Do not copy an API error body: Secret values can occur in validation messages.
		return status.Errorf(code, "platform %s failed with HTTP %d", method, res.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(io.LimitReader(res.Body, 4<<20)).Decode(out)
	}
	_, err = io.Copy(io.Discard, io.LimitReader(res.Body, 4<<20))
	return err
}
func poolPath(g *ax.TaskGroup) string {
	return "/apis/ate.dev/v1alpha1/namespaces/" + url.PathEscape(g.Metadata.Atespace) + "/workerpools"
}
func poolName(g *ax.TaskGroup) string { return "axg-" + strings.ReplaceAll(g.Metadata.Uid, "-", "") }
func (p *KubernetesPlatform) EnsurePool(ctx context.Context, g *ax.TaskGroup, expectedUID string) (string, error) {
	profile, ok := p.Profiles[g.Metadata.Atespace+"/"+g.Metadata.Name]
	if !ok {
		profile, ok = p.Profiles[g.Spec.SandboxClass]
	}
	if !ok || profile.WorkerImage == "" {
		return "", status.Error(codes.FailedPrecondition, "no worker profile for sandbox class")
	}
	labels := make(map[string]string, len(profile.Labels)+1)
	for key, value := range profile.Labels {
		if key == GroupLabel {
			return "", status.Error(codes.FailedPrecondition, "platform profile cannot override TaskGroup identity label")
		}
		labels[key] = value
	}
	labels[GroupLabel] = g.Metadata.Uid
	desired := poolObject{APIVersion: "ate.dev/v1alpha1", Kind: "WorkerPool", Metadata: platformMeta{Name: poolName(g), Namespace: g.Metadata.Atespace, Labels: labels, Annotations: profile.Annotations}, Spec: poolSpec{Replicas: g.Spec.GetReplicas(), WorkerImage: profile.WorkerImage, SandboxClass: g.Spec.SandboxClass, Template: profile.Template}}
	path := poolPath(g) + "/" + poolName(g)
	var current poolObject
	err := p.request(ctx, http.MethodGet, path, nil, &current)
	if status.Code(err) == codes.NotFound {
		if expectedUID != "" {
			return "", status.Error(codes.FailedPrecondition, "bound task group infrastructure is missing; recovery is required")
		}
		err = p.request(ctx, http.MethodPost, poolPath(g)+"?fieldValidation=Strict", desired, &current)
		if status.Code(err) == codes.AlreadyExists {
			err = p.request(ctx, http.MethodGet, path, nil, &current)
		}
	}
	if err != nil {
		return "", err
	}
	if current.Metadata.UID == "" || (expectedUID != "" && current.Metadata.UID != expectedUID) || current.Metadata.Labels[GroupLabel] != g.Metadata.Uid || current.Metadata.DeletionTimestamp != "" {
		return "", status.Error(codes.FailedPrecondition, "task group infrastructure identity changed")
	}
	for key, value := range profile.Labels {
		if current.Metadata.Labels[key] != value {
			return "", status.Error(codes.FailedPrecondition, "immutable platform labels differ")
		}
	}
	for key, value := range profile.Annotations {
		if current.Metadata.Annotations[key] != value {
			return "", status.Error(codes.FailedPrecondition, "immutable platform annotations differ")
		}
	}
	// Reconfiguration of platform profiles must be explicit, never an incidental
	// reconcile of an already bound group. Compare JSON structurally (key order is irrelevant).
	want, have := desired.Spec, current.Spec
	want.Replicas = 0
	have.Replicas = 0
	wr, _ := json.Marshal(want)
	hr, _ := json.Marshal(have)
	var w, h any
	_ = json.Unmarshal(wr, &w)
	_ = json.Unmarshal(hr, &h)
	wr, _ = json.Marshal(w)
	hr, _ = json.Marshal(h)
	if !bytes.Equal(wr, hr) {
		return "", status.Error(codes.FailedPrecondition, "immutable task group platform profile differs")
	}
	if current.Spec.Replicas != desired.Spec.Replicas {
		// A JSON merge patch leaves server-owned metadata and future fields intact.
		patch := map[string]any{"metadata": map[string]any{"resourceVersion": current.Metadata.Version, "uid": current.Metadata.UID}, "spec": map[string]any{"replicas": desired.Spec.Replicas}}
		// PUT would discard unknown metadata; use the dedicated patch helper instead.
		if err = p.patch(ctx, path, patch, &current); err != nil {
			return "", err
		}
	}
	return current.Metadata.UID, nil
}
func (p *KubernetesPlatform) patch(ctx context.Context, path string, in, out any) error {
	// Kubernetes accepts merge patches only with their explicit media type.
	clone := *p
	clone.HTTP = &http.Client{Transport: mediaTransport{base: p.HTTP.Transport}, Timeout: p.HTTP.Timeout}
	return clone.request(ctx, http.MethodPatch, path, in, out)
}

type mediaTransport struct{ base http.RoundTripper }

func (t mediaTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r.Header.Set("Content-Type", "application/merge-patch+json")
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(r)
}
func (p *KubernetesPlatform) DeletePool(ctx context.Context, g *ax.TaskGroup, uid string) error {
	if uid == "" {
		return status.Error(codes.FailedPrecondition, "pool UID is required for deletion")
	}
	path := poolPath(g) + "/" + poolName(g)
	err := p.request(ctx, http.MethodDelete, path, map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": map[string]string{"uid": uid}}, nil)
	if status.Code(err) == codes.NotFound {
		return nil
	}
	if err != nil {
		return err
	}
	return waitManagedDeletion(ctx, uid, func() (string, error) {
		var existing poolObject
		err := p.request(ctx, http.MethodGet, path, nil, &existing)
		return existing.Metadata.UID, err
	})
}
func credentialName(ref *ax.ResourceRef) string {
	return "ax-auth-" + strings.ReplaceAll(ref.Uid, "-", "")
}
func secretPath(ref *ax.ResourceRef) string {
	return "/api/v1/namespaces/" + url.PathEscape(ref.Atespace) + "/secrets"
}
func (p *KubernetesPlatform) EnsureCredential(ctx context.Context, ref *ax.ResourceRef, token string) (*ax.CredentialSecretRef, error) {
	if err := ax.ValidateRef(ref, true); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if token == "" {
		return nil, status.Error(codes.InvalidArgument, "runtime credential is required")
	}
	desired := secretObject{APIVersion: "v1", Kind: "Secret", Type: "Opaque", Metadata: platformMeta{Name: credentialName(ref), Namespace: ref.Atespace, Labels: map[string]string{"ax.io/task-uid": ref.Uid}}, Data: map[string]string{"credential": base64.StdEncoding.EncodeToString([]byte(token))}}
	var current secretObject
	path := secretPath(ref) + "/" + desired.Metadata.Name
	err := p.request(ctx, http.MethodGet, path, nil, &current)
	if status.Code(err) == codes.NotFound {
		err = p.request(ctx, http.MethodPost, secretPath(ref), desired, &current)
		if status.Code(err) == codes.AlreadyExists {
			err = p.request(ctx, http.MethodGet, path, nil, &current)
		}
	}
	if err != nil {
		return nil, err
	}
	if current.Metadata.Labels["ax.io/task-uid"] != ref.Uid || current.Metadata.DeletionTimestamp != "" || current.Data["credential"] != desired.Data["credential"] {
		return nil, status.Error(codes.FailedPrecondition, "runtime credential identity differs")
	}
	return &ax.CredentialSecretRef{Namespace: ref.Atespace, Name: desired.Metadata.Name, Key: "credential"}, nil
}
func (p *KubernetesPlatform) DeleteCredential(ctx context.Context, ref *ax.ResourceRef) error {
	path := secretPath(ref) + "/" + credentialName(ref)
	var current secretObject
	err := p.request(ctx, http.MethodGet, path, nil, &current)
	if status.Code(err) == codes.NotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if current.Metadata.Labels["ax.io/task-uid"] != ref.Uid || current.Metadata.UID == "" {
		return status.Error(codes.FailedPrecondition, "runtime credential identity changed")
	}
	err = p.request(ctx, http.MethodDelete, path, map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": map[string]string{"uid": current.Metadata.UID}}, nil)
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return err
}

func (p *KubernetesPlatform) ObservePool(ctx context.Context, g *ax.TaskGroup, uid string) error {
	var pool poolObject
	if err := p.request(ctx, http.MethodGet, poolPath(g)+"/"+poolName(g), nil, &pool); err != nil {
		return err
	}
	if uid == "" || pool.Metadata.UID != uid || pool.Metadata.Labels[GroupLabel] != g.Metadata.Uid || pool.Metadata.DeletionTimestamp != "" || pool.Spec.Replicas != g.Spec.GetReplicas() {
		return status.Error(codes.FailedPrecondition, "bound task group infrastructure changed")
	}
	return nil
}
