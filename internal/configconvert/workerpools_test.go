package configconvert

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
)

const poolYAML = `apiVersion: ate.dev/v1alpha1
kind: WorkerPool
metadata:
  name: agents
  namespace: team
  labels:
    owner: agents
  annotations:
    scheduling.example/policy: dedicated
spec:
  replicas: 0
  workerImage: registry/worker@sha256:example
  sandboxClass: gvisor
  template:
    nodeSelector:
      kubernetes.io/arch: amd64
    tolerations:
      - key: agents
        operator: Exists
    resources:
      requests:
        memory: 8Gi
`

func TestWorkerPoolConversionPreservesDistinctProfiles(t *testing.T) {
	second := strings.ReplaceAll(strings.ReplaceAll(poolYAML, "name: agents", "name: other"), "memory: 8Gi", "memory: 16Gi")
	bundle, err := WorkerPools(strings.NewReader(poolYAML+"---\n"+second), "s3://bucket/checkpoints")
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Groups) != 2 || len(bundle.PoolProfiles) != 2 {
		t.Fatal(bundle)
	}
	var group ax.TaskGroup
	if err := protojson.Unmarshal(bundle.Groups[0], &group); err != nil {
		t.Fatal(err)
	}
	if group.Spec.Replicas == nil || group.Spec.GetReplicas() != 0 || group.Spec.SnapshotLocation != "s3://bucket/checkpoints" {
		t.Fatal(&group)
	}
	p := bundle.PoolProfiles["team/agents"]
	if p.Labels["owner"] != "agents" || p.Annotations["scheduling.example/policy"] != "dedicated" {
		t.Fatal(p)
	}
	var template map[string]any
	if err := json.Unmarshal(p.Template, &template); err != nil {
		t.Fatal(err)
	}
	if template["nodeSelector"].(map[string]any)["kubernetes.io/arch"] != "amd64" {
		t.Fatal(template)
	}
	if !bytes.Contains(bundle.PoolProfiles["team/other"].Template, []byte("16Gi")) {
		t.Fatal("lost second pool resource configuration")
	}
}
func TestWorkerPoolConversionRejectsLossAndPartialOutput(t *testing.T) {
	for name, input := range map[string]string{
		"unknown spec":      strings.Replace(poolYAML, "  replicas: 0", "  futurePolicy: nondefault\n  replicas: 0", 1),
		"live UID":          strings.Replace(poolYAML, "  name: agents", "  uid: live-instance\n  name: agents", 1),
		"unknown sandbox":   strings.Replace(poolYAML, "gvisor", "other", 1),
		"duplicate":         poolYAML + "---\n" + poolYAML,
		"last invalid":      poolYAML + "---\nkind: Unknown\n",
		"negative replicas": strings.Replace(poolYAML, "replicas: 0", "replicas: -1", 1),
		"unknown template":  strings.Replace(poolYAML, "    nodeSelector:", "    unsupported: value\n    nodeSelector:", 1),
		"missing replicas":  strings.Replace(poolYAML, "  replicas: 0\n", "", 1),
		"empty":             "",
	} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			if err := Encode(strings.NewReader(input), &out, "gs://bucket/checkpoints"); err == nil {
				t.Fatal("accepted unsupported configuration")
			}
			if out.Len() != 0 {
				t.Fatal("emitted partial configuration")
			}
		})
	}
}
