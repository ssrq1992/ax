// Package configconvert translates installation configuration, never live
// objects or historical runtime state. It performs no cluster writes.
package configconvert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/google/ax/internal/substrate"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/yaml.v3"
)

type Bundle struct {
	Version      int                              `json:"version"`
	Groups       []json.RawMessage                `json:"groups"`
	PoolProfiles map[string]substrate.PoolProfile `json:"poolProfiles"`
}
type legacyPool struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name        string            `yaml:"name"`
		Namespace   string            `yaml:"namespace"`
		Labels      map[string]string `yaml:"labels"`
		Annotations map[string]string `yaml:"annotations"`
	} `yaml:"metadata"`
	Spec struct {
		Replicas     *int32         `yaml:"replicas"`
		SandboxClass string         `yaml:"sandboxClass"`
		WorkerImage  string         `yaml:"workerImage"`
		Template     map[string]any `yaml:"template"`
	} `yaml:"spec"`
}

// WorkerPools requires source manifests, not kubectl exports carrying UID,
// resourceVersion, status, or owner references. Unknown configuration is an
// error, rather than being silently discarded. A profile keyed by space/name
// preserves distinct worker specifications without widening TaskGroup.spec.
func WorkerPools(input io.Reader, snapshotLocation string) (*Bundle, error) {
	if err := ax.ValidateSnapshotLocation(snapshotLocation); err != nil {
		return nil, err
	}
	decoder := yaml.NewDecoder(input)
	decoder.KnownFields(true)
	bundle := &Bundle{Version: 1, Groups: []json.RawMessage{}, PoolProfiles: map[string]substrate.PoolProfile{}}
	for index := 1; ; index++ {
		var pool legacyPool
		err := decoder.Decode(&pool)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("document %d: %w", index, err)
		}
		if pool.APIVersion != "ate.dev/v1alpha1" || pool.Kind != "WorkerPool" {
			return nil, fmt.Errorf("document %d: expected ate.dev/v1alpha1 WorkerPool", index)
		}
		if pool.Spec.WorkerImage == "" {
			return nil, fmt.Errorf("document %d: workerImage is required", index)
		}
		if _, exists := pool.Metadata.Labels[substrate.GroupLabel]; exists {
			return nil, fmt.Errorf("document %d: reserved AX identity label", index)
		}
		class := pool.Spec.SandboxClass
		if class == "" {
			class = "gvisor"
		}
		replicas := pool.Spec.Replicas
		if replicas == nil {
			return nil, fmt.Errorf("document %d: replicas is required by the source WorkerPool contract", index)
		}
		group := &ax.TaskGroup{ApiVersion: ax.APIVersion, Kind: ax.KindTaskGroup, Metadata: &ax.ObjectMeta{Name: pool.Metadata.Name, Atespace: pool.Metadata.Namespace}, Spec: &ax.TaskGroupSpec{Replicas: replicas, SandboxClass: class, SnapshotLocation: snapshotLocation}}
		if err := ax.ValidateTaskGroup(group); err != nil {
			return nil, fmt.Errorf("document %d: %w", index, err)
		}
		key := pool.Metadata.Namespace + "/" + pool.Metadata.Name
		if _, exists := bundle.PoolProfiles[key]; exists {
			return nil, fmt.Errorf("duplicate WorkerPool %s", key)
		}
		profile := substrate.PoolProfile{WorkerImage: pool.Spec.WorkerImage, Labels: pool.Metadata.Labels, Annotations: pool.Metadata.Annotations}
		if pool.Spec.Template != nil {
			for key := range pool.Spec.Template {
				switch key {
				case "labels", "annotations", "nodeSelector", "tolerations", "priorityClassName", "nodeAffinity", "resources":
				default:
					return nil, fmt.Errorf("document %d: unsupported worker template field %q", index, key)
				}
			}
			profile.Template, err = json.Marshal(pool.Spec.Template)
			if err != nil {
				return nil, fmt.Errorf("%s: worker template cannot be represented in JSON: %w", key, err)
			}
		}
		raw, err := protojson.Marshal(group)
		if err != nil {
			return nil, err
		}
		bundle.Groups = append(bundle.Groups, raw)
		bundle.PoolProfiles[key] = profile
	}
	if len(bundle.Groups) == 0 {
		return nil, fmt.Errorf("no WorkerPool manifests")
	}
	return bundle, nil
}

// Encode finishes validation before emitting any output, including when the
// last document contains unsupported settings.
func Encode(input io.Reader, output io.Writer, snapshotLocation string) error {
	bundle, err := WorkerPools(input, snapshotLocation)
	if err != nil {
		return err
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(bundle); err != nil {
		return err
	}
	_, err = io.Copy(output, &buffer)
	return err
}
