package substrate

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type poolRoundTripper func(*http.Request) (*http.Response, error)

func (f poolRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestPlatformGroupSpecificProfileAndUIDGuard(t *testing.T) {
	var stored *poolObject
	writes := 0
	transport := poolRoundTripper(func(r *http.Request) (*http.Response, error) {
		code := http.StatusOK
		switch r.Method {
		case http.MethodGet:
			if stored == nil {
				code = http.StatusNotFound
			}
		case http.MethodPost:
			if r.URL.Query().Get("fieldValidation") != "Strict" {
				t.Fatal("unknown fields could be silently dropped")
			}
			stored = &poolObject{}
			if err := json.NewDecoder(r.Body).Decode(stored); err != nil {
				t.Fatal(err)
			}
			stored.Metadata.UID = "backend-uid"
			stored.Metadata.Version = "1"
			writes++
		default:
			t.Fatalf("unexpected platform mutation %s", r.Method)
		}
		raw, _ := json.Marshal(stored)
		return &http.Response{StatusCode: code, Body: io.NopCloser(bytes.NewReader(raw)), Header: http.Header{}}, nil
	})
	p := &KubernetesPlatform{Endpoint: "https://kube.test", HTTP: &http.Client{Transport: transport}, Profiles: map[string]PoolProfile{
		"gvisor":     {WorkerImage: "default"},
		"team/group": {WorkerImage: "custom", Template: json.RawMessage(`{"nodeSelector":{"pool":"dedicated"},"resources":{"requests":{"memory":"8Gi"}}}`), Labels: map[string]string{"owner": "team"}, Annotations: map[string]string{"scheduling.example/policy": "dedicated"}},
	}}
	one := int32(1)
	group := &ax.TaskGroup{Metadata: &ax.ObjectMeta{Atespace: "team", Name: "group", Uid: "group-uid"}, Spec: &ax.TaskGroupSpec{SandboxClass: "gvisor", Replicas: &one}}
	uid, err := p.EnsurePool(t.Context(), group, "")
	if err != nil {
		t.Fatal(err)
	}
	if uid != "backend-uid" || stored.Spec.WorkerImage != "custom" || stored.Metadata.Labels[GroupLabel] != "group-uid" || stored.Metadata.Labels["owner"] != "team" {
		t.Fatal(stored)
	}
	if stored.Metadata.Annotations["scheduling.example/policy"] != "dedicated" {
		t.Fatal("lost annotations")
	}
	if _, err = p.EnsurePool(t.Context(), group, uid); err != nil {
		t.Fatal(err)
	}
	if writes != 1 {
		t.Fatal("idempotent observation wrote pool")
	}
	if _, exists := p.Profiles["team/group"].Labels[GroupLabel]; exists {
		t.Fatal("mutated operator profile")
	}
	stored.Metadata.UID = "replacement"
	if _, err = p.EnsurePool(t.Context(), group, uid); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("accepted replacement: %v", err)
	}
	stored = nil
	if _, err = p.EnsurePool(t.Context(), group, uid); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("recreated lost pool: %v", err)
	}
	if writes != 1 {
		t.Fatal("unexpected recreation")
	}
}
