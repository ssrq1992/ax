package configconvert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"gopkg.in/yaml.v3"
)

// KagentResources converts declarative inputs only. Unrelated spec fields are
// preserved verbatim; unknown legacy runtime fields cannot be silently dropped.
func KagentResources(input io.Reader, output io.Writer) error {
	decoder := yaml.NewDecoder(input)
	var documents []map[string]any
	for index := 1; ; index++ {
		var resource map[string]any
		if err := decoder.Decode(&resource); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if resource == nil {
			continue
		}
		kind, _ := resource["kind"].(string)
		meta, _ := resource["metadata"].(map[string]any)
		if _, ok := resource["status"]; ok {
			return fmt.Errorf("document %d: use source manifests, not live status", index)
		}
		for _, field := range []string{"uid", "resourceVersion", "managedFields", "ownerReferences", "deletionTimestamp"} {
			if _, ok := meta[field]; ok {
				return fmt.Errorf("document %d: live metadata %s is not supported", index, field)
			}
		}
		spec, ok := resource["spec"].(map[string]any)
		if !ok {
			return fmt.Errorf("document %d: spec is required", index)
		}
		switch kind {
		case "Harness", "SandboxTemplate":
			if err := convertRuntimePolicy(spec); err != nil {
				return fmt.Errorf("document %d: %w", index, err)
			}
		case "Agent":
			if inline, ok := spec["harness"].(map[string]any); ok {
				if err := convertRuntimePolicy(inline); err != nil {
					return fmt.Errorf("document %d inline harness: %w", index, err)
				}
			}
		default:
			return fmt.Errorf("document %d: expected Harness, SandboxTemplate or Agent", index)
		}
		documents = append(documents, resource)
	}
	if len(documents) == 0 {
		return fmt.Errorf("no kagent resources")
	}
	var buffer bytes.Buffer
	encoder := yaml.NewEncoder(&buffer)
	encoder.SetIndent(2)
	for _, resource := range documents {
		if err := encoder.Encode(resource); err != nil {
			return err
		}
	}
	if err := encoder.Close(); err != nil {
		return err
	}
	_, err := io.Copy(output, &buffer)
	return err
}
func convertRuntimePolicy(spec map[string]any) error {
	legacy, exists := spec["substrate"]
	if !exists {
		if _, ok := spec["ax"]; ok {
			return nil
		}
		return fmt.Errorf("runtime policy is required")
	}
	if _, exists := spec["ax"]; exists {
		return fmt.Errorf("both AX and legacy runtime policies are present")
	}
	policy, ok := legacy.(map[string]any)
	if !ok {
		return fmt.Errorf("invalid legacy runtime policy")
	}
	for key := range policy {
		if key != "workerPoolRef" && key != "snapshotPolicy" {
			return fmt.Errorf("unsupported legacy runtime field %q", key)
		}
	}
	ref, ok := policy["workerPoolRef"].(map[string]any)
	if !ok || len(ref) != 1 {
		return fmt.Errorf("workerPoolRef must contain only name")
	}
	name, ok := ref["name"].(string)
	if !ok || ax.ValidateName(name) != nil {
		return fmt.Errorf("invalid workerPoolRef name")
	}
	converted := map[string]any{"taskGroupRef": map[string]any{"name": name}}
	if snapshot, exists := policy["snapshotPolicy"]; exists {
		values, ok := snapshot.(map[string]any)
		if !ok || len(values) != 1 {
			return fmt.Errorf("snapshotPolicy must contain only location")
		}
		location, ok := values["location"].(string)
		if !ok {
			return fmt.Errorf("snapshot location must be a string")
		}
		if err := ax.ValidateSnapshotLocation(location); err != nil {
			return err
		}
		converted["snapshotLocationOverride"] = location
	}
	delete(spec, "substrate")
	spec["ax"] = converted
	return nil
}

type KagentTLS struct {
	Endpoint         string `json:"endpoint"`
	ServerName       string `json:"serverName"`
	CASecretName     string `json:"caSecretName"`
	ClientSecretName string `json:"clientSecretName"`
	APITLSSecret     string `json:"apiTLSSecret"`
	APICASecret      string `json:"apiCASecret"`
}

// ValuesBundle keeps backend installation values outside kagent. The operator
// must apply them to the separately installed, version-pinned backend, and put
// poolProfiles/guestImage in AX's platform configuration.
type ValuesBundle struct {
	Version           int            `json:"version"`
	KagentValues      map[string]any `json:"kagentValues"`
	Capacity          *Bundle        `json:"capacity,omitempty"`
	BackendValues     any            `json:"backendValues,omitempty"`
	BackendConnection map[string]any `json:"backendConnection,omitempty"`
	GuestImage        string         `json:"guestImage,omitempty"`
}

func KagentValues(input io.Reader, output io.Writer, namespace, location string, tls KagentTLS) error {
	if tls.Endpoint == "" || tls.ServerName == "" || tls.CASecretName == "" || tls.ClientSecretName == "" || tls.APITLSSecret == "" || tls.APICASecret == "" {
		return fmt.Errorf("explicit AX endpoint, server name and four TLS secret names are required")
	}
	if ax.ValidateName(namespace) != nil {
		return fmt.Errorf("namespace is required")
	}
	decoder := yaml.NewDecoder(input)
	var values map[string]any
	if err := decoder.Decode(&values); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("exactly one values document is required")
	}
	if values == nil {
		return fmt.Errorf("empty values")
	}
	controller, ok := values["controller"].(map[string]any)
	if !ok {
		return fmt.Errorf("controller values are required")
	}
	if _, exists := controller["ax"]; exists {
		return fmt.Errorf("AX values already present; refusing to overwrite")
	}
	if _, exists := controller["tls"]; exists {
		return fmt.Errorf("controller TLS values already present; refusing to overwrite")
	}
	if value, exists := controller["a2aGatewayUrl"]; exists && value != "" {
		endpoint, ok := value.(string)
		parsed, err := url.Parse(endpoint)
		if !ok || err != nil || parsed.Scheme != "https" || parsed.Host == "" {
			return fmt.Errorf("controller.a2aGatewayUrl must use HTTPS; supply the new controller endpoint explicitly")
		}
	}
	bundle := ValuesBundle{Version: 1, KagentValues: values, BackendValues: values["substrate"]}
	if legacy, exists := controller["substrate"]; exists {
		connection, ok := legacy.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid controller.substrate")
		}
		for key := range connection {
			if key != "enabled" && key != "ateApiEndpoint" && key != "atenetRouterURL" {
				return fmt.Errorf("unsupported backend connection field %q", key)
			}
		}
		bundle.BackendConnection = connection
	}
	if raw, exists := values["substrateWorkerPool"]; exists {
		pool, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid substrateWorkerPool")
		}
		for key := range pool {
			switch key {
			case "create", "name", "replicas", "workerImage", "sandboxClass", "labels", "annotations", "template":
			default:
				return fmt.Errorf("unsupported WorkerPool value %q", key)
			}
		}
		create, ok := pool["create"].(bool)
		if !ok {
			return fmt.Errorf("WorkerPool create must be explicit")
		}
		if create {
			spec := map[string]any{}
			for _, key := range []string{"replicas", "workerImage", "sandboxClass", "template"} {
				if v, ok := pool[key]; ok {
					spec[key] = v
				}
			}
			manifest := map[string]any{"apiVersion": "ate.dev/v1alpha1", "kind": "WorkerPool", "metadata": map[string]any{"name": pool["name"], "namespace": namespace, "labels": pool["labels"], "annotations": pool["annotations"]}, "spec": spec}
			raw, err := yaml.Marshal(manifest)
			if err != nil {
				return err
			}
			bundle.Capacity, err = WorkerPools(bytes.NewReader(raw), location)
			if err != nil {
				return err
			}
		} else {
			// An external pool is deliberately not inferred or created from defaults.
			for _, key := range []string{"workerImage", "template", "labels", "annotations"} {
				if v := pool[key]; v != nil && v != "" {
					if m, ok := v.(map[string]any); !ok || len(m) > 0 {
						return fmt.Errorf("external WorkerPool %s needs its source manifest converted separately", key)
					}
				}
			}
		}
	}
	if sandbox, ok := controller["sandbox"].(map[string]any); ok {
		if guest, exists := sandbox["guestImage"]; exists {
			image, ok := guest.(map[string]any)
			if !ok {
				return fmt.Errorf("invalid guestImage")
			}
			var err error
			bundle.GuestImage, err = resolveGuestImage(image, values)
			if err != nil {
				return err
			}
			delete(sandbox, "guestImage")
		}
	}
	delete(values, "substrate")
	delete(values, "substrateWorkerPool")
	delete(controller, "substrate")
	controller["ax"] = map[string]any{"endpoint": tls.Endpoint, "serverName": tls.ServerName, "caSecretName": tls.CASecretName, "clientSecretName": tls.ClientSecretName}
	controller["tls"] = map[string]any{"secretName": tls.APITLSSecret, "caSecretName": tls.APICASecret}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(bundle); err != nil {
		return err
	}
	_, err := io.Copy(output, &buffer)
	return err
}

// Match the previous chart's digest/registry precedence, without silently
// accepting a tag-only image or dropping an unknown image setting.
func resolveGuestImage(image, values map[string]any) (string, error) {
	for key := range image {
		if key != "registry" && key != "repository" && key != "digest" {
			return "", fmt.Errorf("unsupported guestImage field %q", key)
		}
	}
	read := func(m map[string]any, key string) (string, error) {
		v, ok := m[key]
		if !ok || v == nil {
			return "", nil
		}
		s, ok := v.(string)
		if !ok {
			return "", fmt.Errorf("image %s must be a string", key)
		}
		return s, nil
	}
	digest, err := read(image, "digest")
	if err != nil {
		return "", err
	}
	if !regexp.MustCompile(`^sha256:[a-fA-F0-9]{64}$`).MatchString(digest) {
		return "", fmt.Errorf("guestImage requires an explicit sha256 digest for the paired AX Guest image")
	}
	repository, err := read(image, "repository")
	if err != nil {
		return "", err
	}
	if repository == "" || strings.ContainsAny(repository, " @:\t\r\n") || strings.HasPrefix(repository, "/") || strings.HasSuffix(repository, "/") {
		return "", fmt.Errorf("invalid guestImage repository")
	}
	registry, err := read(image, "registry")
	if err != nil {
		return "", err
	}
	if registry == "" {
		registry, err = read(values, "registry")
		if err != nil {
			return "", err
		}
	}
	if raw, exists := values["global"]; exists && raw != nil {
		global, ok := raw.(map[string]any)
		if !ok {
			return "", fmt.Errorf("invalid global image configuration")
		}
		mirror, err := read(global, "imageRegistry")
		if err != nil {
			return "", err
		}
		if mirror != "" {
			registry = strings.TrimSuffix(mirror, "/")
		}
	}
	if strings.Contains(registry, "://") || strings.ContainsAny(registry, " @\t\r\n") || strings.HasSuffix(registry, "/") {
		return "", fmt.Errorf("invalid guestImage registry")
	}
	if registry != "" {
		repository = registry + "/" + repository
	}
	return repository + "@" + digest, nil
}
