package substrate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	pb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type recoveryControl struct {
	pb.ControlClient
	actor *pb.Actor
	err   error
}

func (c *recoveryControl) GetActor(context.Context, *pb.GetActorRequest, ...grpc.CallOption) (*pb.Actor, error) {
	return c.actor, c.err
}
func TestRecoveryOnlyAcceptsBoundTaskAndRequestedPhase(t *testing.T) {
	control := &recoveryControl{actor: &pb.Actor{Metadata: &pb.ResourceMetadata{Uid: "uid"}, Status: &pb.ActorStatus{State: pb.ActorState_ACTOR_STATE_RUNNING}}}
	backend := &ManagedBackend{Client: &Client{control: control}}
	in := RecoveryInput{Kind: "Resume", BackendUID: "uid", Task: &ax.Task{Metadata: &ax.ObjectMeta{Atespace: "test", Name: "task", Uid: "public-uid"}}}
	result, err := backend.RecoverObserved(t.Context(), in)
	if err != nil || result.Task.Phase != "Running" {
		t.Fatalf("result=%v err=%v", result, err)
	}
	control.actor.Metadata.Uid = "replacement"
	if _, err = backend.RecoverObserved(t.Context(), in); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("replacement accepted", err)
	}
	control.actor.Metadata.Uid = "uid"
	control.actor.Status.State = pb.ActorState_ACTOR_STATE_RESUMING
	if _, err = backend.RecoverObserved(t.Context(), in); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("unfinished transition accepted", err)
	}
	control.err = status.Error(codes.NotFound, "absent")
	if _, err = backend.RecoverObserved(t.Context(), in); status.Code(err) != codes.NotFound {
		t.Fatal("missing task did not remain unresolved", err)
	}
	// Any mutation calls the nil embedded ControlClient and fails this test.
}
func TestRecoveryVerifiesCredentialWithoutWriting(t *testing.T) {
	token := []byte("opaque-credential")
	sum := sha256.Sum256(token)
	secret := secretObject{Metadata: platformMeta{UID: "secret-uid", Labels: map[string]string{"ax.io/task-uid": "task-uid"}}, Data: map[string]string{"credential": base64.StdEncoding.EncodeToString(token)}}
	platform := &KubernetesPlatform{Endpoint: "https://kube.test", HTTP: &http.Client{Transport: poolRoundTripper(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodGet {
			t.Fatal("recovery mutated credentials")
		}
		raw, _ := json.Marshal(secret)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(raw))}, nil
	})}}
	ref := &ax.ResourceRef{Atespace: "test", Name: "task", Uid: "task-uid"}
	actual, err := platform.VerifyCredential(t.Context(), ref, hex.EncodeToString(sum[:]))
	if err != nil || actual.Name != credentialName(ref) {
		t.Fatal(actual, err)
	}
	secret.Data["credential"] = base64.StdEncoding.EncodeToString([]byte("replacement"))
	if _, err = platform.VerifyCredential(t.Context(), ref, hex.EncodeToString(sum[:])); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("credential replacement accepted", err)
	}
}
