package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
)

type groupCLIProbe struct {
	ax.AXClient
	create *ax.CreateTaskGroupRequest
	update *ax.UpdateTaskGroupRequest
	remove *ax.DeleteTaskGroupRequest
}

func (p *groupCLIProbe) CreateTaskGroup(_ context.Context, r *ax.CreateTaskGroupRequest, _ ...grpc.CallOption) (*ax.TaskGroup, error) {
	p.create = r
	return r.Group, nil
}
func (p *groupCLIProbe) UpdateTaskGroup(_ context.Context, r *ax.UpdateTaskGroupRequest, _ ...grpc.CallOption) (*ax.TaskGroup, error) {
	p.update = r
	return &ax.TaskGroup{}, nil
}
func (p *groupCLIProbe) DeleteTaskGroup(_ context.Context, r *ax.DeleteTaskGroupRequest, _ ...grpc.CallOption) (*ax.DeleteTaskGroupResponse, error) {
	p.remove = r
	return &ax.DeleteTaskGroupResponse{}, nil
}
func TestGroupCLIRequiresExplicitIdentityAndVersion(t *testing.T) {
	p := &groupCLIProbe{}
	for _, args := range [][]string{{"scale", "--name", "group", "--replicas", "0"}, {"scale", "--name", "group", "--uid", "uid", "--replicas", "0"}, {"delete", "--name", "group", "--uid", "uid"}, {"delete", "--name", "group", "--request-id", "delete"}} {
		if err := executeGroup(t.Context(), p, "team", args, io.Discard); err == nil {
			t.Fatalf("accepted unsafe args: %v", args)
		}
	}
	if p.update != nil || p.remove != nil {
		t.Fatal("invalid operation called AX")
	}
	if err := executeGroup(t.Context(), p, "team", []string{"scale", "--name", "group", "--uid", "uid", "--version", "12", "--replicas", "0"}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if p.update.ExpectedVersion != 12 || p.update.Replicas != 0 || p.update.Ref.Uid != "uid" {
		t.Fatal(p.update)
	}
	if err := executeGroup(t.Context(), p, "team", []string{"delete", "--name", "group", "--uid", "uid", "--request-id", "delete"}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if p.remove.OperationId != "delete" || p.remove.Ref.Uid != "uid" {
		t.Fatal(p.remove)
	}
}
func TestGroupCLICreateSingleManifest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "group.yaml")
	manifest := "apiVersion: ax.dev/v1alpha1\nkind: TaskGroup\nmetadata:\n  name: group\nspec:\n  replicas: 0\n  sandboxClass: gvisor\n  snapshotLocation: gs://snapshots/test\n"
	if err := os.WriteFile(path, []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	p := &groupCLIProbe{}
	args := []string{"create", "--file", path, "--request-id", "create"}
	if err := executeGroup(t.Context(), p, "team", args, io.Discard); err != nil {
		t.Fatal(err)
	}
	if p.create.RequestId != "create" || p.create.Group.Spec.Replicas == nil || *p.create.Group.Spec.Replicas != 0 || p.create.Group.Metadata.Atespace != "team" {
		t.Fatal(p.create)
	}
	if err := os.WriteFile(path, []byte(manifest+"---\n"+manifest), 0600); err != nil {
		t.Fatal(err)
	}
	p.create = nil
	if err := executeGroup(t.Context(), p, "team", args, io.Discard); err == nil {
		t.Fatal("accepted multiple documents")
	}
	if p.create != nil {
		t.Fatal("partially applied invalid file")
	}
}
