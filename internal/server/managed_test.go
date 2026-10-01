package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/env/guest"
	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type transportBackend struct {
	controller.ManagedBackend
	mu    sync.Mutex
	phase map[string]string
}

func (b *transportBackend) EnsureGroup(context.Context, *ax.TaskGroup, string) (string, int32, error) {
	return "pool", 1, nil
}
func (b *transportBackend) Prepare(context.Context, *ax.PreparedRuntime, *ax.TaskGroup) (string, string, error) {
	return "template", "Ready", nil
}
func (b *transportBackend) Create(_ context.Context, t *ax.Task, _ *ax.PreparedRuntime, _ *ax.TaskCheckpoint, _ string) (substrate.Observation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.phase[t.Metadata.Uid] = "Suspended"
	return substrate.Observation{UID: t.Metadata.Uid, Phase: "Suspended"}, nil
}
func (b *transportBackend) Observe(_ context.Context, t *ax.Task) (substrate.Observation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return substrate.Observation{UID: t.Metadata.Uid, Phase: b.phase[t.Metadata.Uid]}, nil
}
func (b *transportBackend) Transition(_ context.Context, t *ax.Task, _ string, kind string) (substrate.Observation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.phase[t.Metadata.Uid] = map[string]string{"Resume": "Running", "Suspend": "Suspended", "Delete": "Deleted", "Pause": "Paused"}[kind]
	return substrate.Observation{UID: t.Metadata.Uid, Phase: b.phase[t.Metadata.Uid]}, nil
}

func testCertificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identity, _ := url.Parse("spiffe://ax.test/kagent")
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "local test"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{identity}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	privatePEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})
	cert, err := tls.X509KeyPair(certificatePEM, privatePEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certificatePEM)
	return cert, roots
}
func managedTransport(t *testing.T, dialer RuntimeDialer) (ax.AXClient, ax.TaskExecutionServiceClient, *grpc.ClientConn, *Server) {
	t.Helper()
	memoryStore := memory.NewStore()
	backend := &transportBackend{phase: map[string]string{}}
	server := NewServer(memoryStore, Options{Managed: &controller.ManagedController{Store: memoryStore, Backend: backend}, ManagedClients: map[string][]string{"spiffe://ax.test/kagent": {"test"}}, RuntimeDialer: dialer})
	cert, roots := testCertificate(t)
	httpServer := httptest.NewUnstartedServer(server.Handler())
	httpServer.EnableHTTP2 = true
	httpServer.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}
	httpServer.StartTLS()
	t.Cleanup(httpServer.Close)
	conn, err := grpc.NewClient(strings.TrimPrefix(httpServer.URL, "https://"), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return ax.NewAXClient(conn), ax.NewTaskExecutionServiceClient(conn), conn, server
}
func createManagedTask(t *testing.T, client ax.AXClient, kind string) *ax.Task {
	t.Helper()
	ctx := t.Context()
	g, err := client.CreateTaskGroup(ctx, &ax.CreateTaskGroupRequest{RequestId: "group", Group: &ax.TaskGroup{Metadata: &ax.ObjectMeta{Name: "group", Atespace: "test"}, Spec: &ax.TaskGroupSpec{SnapshotLocation: "gs://test/snapshots"}}})
	if err != nil {
		t.Fatal(err)
	}
	r, err := client.PrepareRuntime(ctx, &ax.PrepareRuntimeRequest{RequestId: "runtime", Metadata: &ax.ObjectMeta{Name: "runtime", Atespace: "test"}, Spec: &ax.PreparedRuntimeSpec{GroupRef: ax.Ref(g.Metadata), Kind: kind, Image: "test@sha256:" + strings.Repeat("a", 64)}})
	if err != nil {
		t.Fatal(err)
	}
	task, err := client.CreateTask(ctx, &ax.CreateTaskRequest{RequestId: "task", Task: &ax.Task{Metadata: &ax.ObjectMeta{Name: "task", Atespace: "test"}, Spec: &ax.TaskSpec{GroupRef: ax.Ref(g.Metadata), PreparedRuntimeRef: ax.Ref(r.Metadata)}}})
	if err != nil {
		t.Fatal(err)
	}
	return task
}
func TestManagedMTLSOwnership(t *testing.T) {
	client, _, _, server := managedTransport(t, nil)
	task := createManagedTask(t, client, "Service")
	_, err := client.GetTaskGroup(t.Context(), &ax.GetTaskGroupRequest{Atespace: "another", Name: "group"})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-atespace request: %v", err)
	}
	// Legacy unauthenticated entry and a restart with the managed option removed
	// both consult persistent ownership, even when no managed fields are sent.
	for _, candidate := range []*Server{server, NewServer(server.store)} {
		_, err = candidate.CreateTask(t.Context(), &ax.CreateTaskRequest{Task: &ax.Task{Metadata: &ax.ObjectMeta{Name: task.Metadata.Name, Atespace: "test"}}})
		if err == nil {
			t.Fatal("legacy create bypassed managed ownership")
		}
		_, err = candidate.DeleteTask(t.Context(), &ax.DeleteTaskRequest{Atespace: "test", Name: task.Metadata.Name})
		if err == nil {
			t.Fatal("legacy delete bypassed managed ownership")
		}
	}
}
func TestManagedExecutionRealGuest(t *testing.T) {
	guestServer, cleanup, err := guest.NewServer(guest.Config{Workspace: t.TempDir(), LogDir: t.TempDir(), EnableProcess: true, EnableFileSystem: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	t.Cleanup(guestServer.Stop)
	listener := bufconn.Listen(1 << 20)
	t.Cleanup(func() { listener.Close() })
	go guestServer.Serve(listener)
	conn, err := grpc.NewClient("passthrough:///guest", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client, execution, _, _ := managedTransport(t, func(ctx context.Context, _ *ax.Task) (context.Context, grpc.ClientConnInterface, error) {
		return ctx, conn, nil
	})
	task := createManagedTask(t, client, "Sandbox")
	ref := ax.Ref(task.Metadata)
	_, err = client.ResumeTask(t.Context(), &ax.ResumeTaskRequest{Atespace: ref.Atespace, Name: ref.Name, ExpectedUid: ref.Uid, OperationId: "resume"})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := execution.WriteFile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Send(&ax.WriteFileRequest{TaskRef: ref, Path: "binary", Mode: 0600, Chunk: []byte{0, 128, 255}}); err != nil {
		t.Fatal(err)
	}
	result, err := writer.CloseAndRecv()
	if err != nil || result.GetBytesWritten() != 3 {
		t.Fatalf("write: %v %v", result, err)
	}
	reader, err := execution.ReadFile(t.Context(), &ax.ReadFileRequest{TaskRef: ref, Path: "binary"})
	if err != nil {
		t.Fatal(err)
	}
	var content []byte
	for {
		chunk, err := reader.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		content = append(content, chunk.Data...)
	}
	if string(content) != string([]byte{0, 128, 255}) {
		t.Fatalf("file changed: %v", content)
	}
	started, err := execution.StartProcess(t.Context(), &ax.StartProcessRequest{TaskRef: ref, Command: []string{"sh", "-c", "printf output; printf error >&2"}})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := execution.StreamProcessOutputs(t.Context(), &ax.StreamProcessOutputsRequest{TaskRef: ref, ProcessId: started.ProcessId, Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr string
	var exit *ax.Process
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		switch chunk.Source {
		case ax.OutputSource_OUTPUT_SOURCE_STDOUT:
			stdout += string(chunk.Data)
		case ax.OutputSource_OUTPUT_SOURCE_STDERR:
			stderr += string(chunk.Data)
		}
		if chunk.Exit != nil {
			exit = chunk.Exit
		}
	}
	if stdout != "output" || stderr != "error" || exit == nil || exit.ExitCode != 0 {
		t.Fatalf("output=%q stderr=%q exit=%v", stdout, stderr, exit)
	}
	running, err := execution.StartProcess(t.Context(), &ax.StartProcessRequest{TaskRef: ref, Command: []string{"sh", "-c", "sleep 60 & wait"}})
	if err != nil {
		t.Fatal(err)
	}
	killed, err := execution.KillProcess(t.Context(), &ax.KillProcessRequest{TaskRef: ref, ProcessId: running.ProcessId})
	if err != nil || killed.GetExitCode() != 137 {
		t.Fatalf("kill: %v %v", killed, err)
	}
	wrong := &ax.ResourceRef{Atespace: ref.Atespace, Name: ref.Name, Uid: "wrong"}
	_, err = execution.GetProcess(t.Context(), &ax.GetProcessRequest{TaskRef: wrong, ProcessId: running.ProcessId})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("wrong UID: %v", err)
	}
}
func (b *transportBackend) VerifyPreparation(context.Context, *ax.PreparedRuntime, string, *ax.TaskCheckpoint, string) error {
	return nil
}
func (b *transportBackend) ReadyWorkers(context.Context, *ax.TaskGroup, string) (int32, error) {
	return 1, nil
}

func TestManagedTaskListRequiresMTLSAndKeepsLegacyScope(t *testing.T) {
	client, _, _, server := managedTransport(t, nil)
	task := createManagedTask(t, client, "Service")
	response, err := client.ListTasks(t.Context(), &ax.ListTasksRequest{Atespace: "test", ManagedOnly: true, Limit: 1})
	if err != nil || len(response.GetTasks()) != 1 || response.Tasks[0].Metadata.Uid != task.Metadata.Uid {
		t.Fatalf("managed list: %v %v", response, err)
	}
	response, err = client.ListTasks(t.Context(), &ax.ListTasksRequest{Atespace: "test"})
	if err != nil || len(response.GetTasks()) != 0 {
		t.Fatalf("ordinary listing changed: %v %v", response, err)
	}
	response, err = client.ListTasks(t.Context(), &ax.ListTasksRequest{Atespace: "test", ManagedOnly: true, Offset: 1})
	if err != nil || len(response.GetTasks()) != 0 {
		t.Fatalf("offset: %v %v", response, err)
	}
	_, err = client.ListTasks(t.Context(), &ax.ListTasksRequest{Atespace: "another", ManagedOnly: true})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-atespace list: %v", err)
	}
	_, err = server.ListTasks(t.Context(), &ax.ListTasksRequest{Atespace: "test", ManagedOnly: true})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated managed list: %v", err)
	}
	_, err = client.ListTasks(t.Context(), &ax.ListTasksRequest{Atespace: "test", ManagedOnly: true, Limit: 1001})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unbounded listing: %v", err)
	}
}
