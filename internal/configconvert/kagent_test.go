package configconvert

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestKagentResources(t *testing.T) {
	source := `apiVersion: api.kagent.dev/v1alpha3
kind: Harness
metadata: {name: test, namespace: team}
spec:
  kagent: {memory: {ttlDays: 7}}
  workload: {image: unchanged}
  substrate:
    workerPoolRef: {name: agents}
    snapshotPolicy: {location: 's3://snapshots/team'}
`
	var out bytes.Buffer
	if err := KagentResources(strings.NewReader(source), &out); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"taskGroupRef:", "snapshotLocationOverride: s3://snapshots/team", "ttlDays: 7", "image: unchanged"} {
		if !strings.Contains(out.String(), text) {
			t.Fatalf("missing %q: %s", text, &out)
		}
	}
	for _, bad := range []string{
		strings.Replace(source, "workerPoolRef: {name: agents}", "workerPoolRef: {name: agents, namespace: other}", 1),
		strings.Replace(source, "  substrate:", "  ax: {}\n  substrate:", 1),
		source + "---\nkind: Session\nspec: {}\n",
		strings.Replace(source, "metadata: {name: test, namespace: team}", "metadata: {name: test, uid: live}", 1),
		strings.Replace(source, "  substrate:", "  substrate:\n    unknown: true", 1),
	} {
		out.Reset()
		if err := KagentResources(strings.NewReader(bad), &out); err == nil || out.Len() != 0 {
			t.Fatalf("unsupported input must fail without output: %v", err)
		}
	}
}
func TestKagentValuesPreservesNonDefaultCapacity(t *testing.T) {
	source := `controller:
  substrate: {enabled: true, ateApiEndpoint: api:443}
  sandbox: {cpu: '2', memory: 4Gi}
substrate: {enabled: true, credentialProvider: {namespacePolicies: [{atespace: team, allowedNamespaces: [team]}]}}
substrateWorkerPool:
  create: true
  name: agents
  replicas: 0
  workerImage: image:pinned
  sandboxClass: microvm
  template: {nodeSelector: {disk: ssd}, resources: {requests: {cpu: '4'}}}
ui: {replicas: 2}
`
	tls := KagentTLS{Endpoint: "ax:8443", ServerName: "ax", CASecretName: "ax-ca", ClientSecretName: "client", APITLSSecret: "api", APICASecret: "api-ca"}
	var out bytes.Buffer
	if err := KagentValues(strings.NewReader(source), &out, "team", "s3://snapshots/team", tls); err != nil {
		t.Fatal(err)
	}
	var bundle ValuesBundle
	if err := json.Unmarshal(out.Bytes(), &bundle); err != nil {
		t.Fatal(err)
	}
	if bundle.Capacity == nil || len(bundle.Capacity.Groups) != 1 || bundle.Capacity.PoolProfiles["team/agents"].WorkerImage != "image:pinned" {
		t.Fatalf("lost capacity: %s", &out)
	}
	if !strings.Contains(string(bundle.Capacity.PoolProfiles["team/agents"].Template), "ssd") {
		t.Fatal("lost scheduling")
	}
	if _, exists := bundle.KagentValues["substrate"]; exists {
		t.Fatal("backend remains in kagent")
	}
	if bundle.BackendValues == nil || bundle.KagentValues["ui"] == nil {
		t.Fatal("lost values")
	}
	out.Reset()
	if err := KagentValues(strings.NewReader(strings.Replace(source, "  template:", "  unsupported: yes\n  template:", 1)), &out, "team", "s3://snapshots/team", tls); err == nil || out.Len() != 0 {
		t.Fatal("unknown capacity must fail atomically")
	}
}

func TestKagentGuestImageAndTLSConversion(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	image := map[string]any{"registry": "old.registry", "repository": "agents/guest", "digest": digest}
	values := map[string]any{"registry": "default.registry", "global": map[string]any{"imageRegistry": "mirror.registry/"}}
	actual, err := resolveGuestImage(image, values)
	if err != nil || actual != "mirror.registry/agents/guest@"+digest {
		t.Fatalf("image=%q err=%v", actual, err)
	}
	image["tag"] = "latest"
	if _, err = resolveGuestImage(image, values); err == nil {
		t.Fatal("ignored tag")
	}
	tls := KagentTLS{Endpoint: "ax:8443", ServerName: "ax", CASecretName: "ca", ClientSecretName: "client", APITLSSecret: "tls", APICASecret: "api-ca"}
	for _, source := range []string{"controller: {tls: {secretName: existing}}", "controller: {a2aGatewayUrl: 'http://old:8083'}"} {
		var out bytes.Buffer
		if err := KagentValues(strings.NewReader(source), &out, "team", "s3://snapshots/team", tls); err == nil || out.Len() != 0 {
			t.Fatalf("unsafe overwrite accepted: %s", source)
		}
	}
}
