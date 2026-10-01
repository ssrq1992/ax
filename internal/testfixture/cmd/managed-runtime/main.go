// managed-runtime is a local protocol-test fixture, never a deployment server.
// AX handlers and the pinned Guest are real; compute placement is controllable
// in-memory state. It is built in AX's module to retain the pinned backend API.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/agent-substrate/env/guest"
	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type compute struct {
	controller.ManagedBackend
	mu          sync.Mutex
	tasks       map[string]string
	generations map[string]int
	checkpoints map[string]*ax.TaskCheckpoint
}

func (*compute) EnsureGroup(context.Context, *ax.TaskGroup, string) (string, int32, error) {
	return "pool-uid", 1, nil
}
func (*compute) ReadyWorkers(context.Context, *ax.TaskGroup, string) (int32, error) { return 1, nil }
func (*compute) Prepare(context.Context, *ax.PreparedRuntime, *ax.TaskGroup) (string, string, error) {
	return "runtime-uid", "Ready", nil
}
func (*compute) VerifyPreparation(context.Context, *ax.PreparedRuntime, string, *ax.TaskCheckpoint, string) error {
	return nil
}
func (*compute) ObserveRuntime(context.Context, *ax.PreparedRuntime, string) (string, error) {
	return "Ready", nil
}
func (b *compute) Create(_ context.Context, t *ax.Task, _ *ax.PreparedRuntime, _ *ax.TaskCheckpoint, _ string) (substrate.Observation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tasks[t.Metadata.Uid] = "Suspended"
	return substrate.Observation{UID: t.Metadata.Uid, Phase: "Suspended"}, nil
}
func (b *compute) Observe(_ context.Context, t *ax.Task) (substrate.Observation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	phase, ok := b.tasks[t.Metadata.Uid]
	if !ok {
		return substrate.Observation{}, status.Error(codes.NotFound, "test compute missing")
	}
	return b.observation(t, phase), nil
}
func (b *compute) Transition(_ context.Context, t *ax.Task, _ string, kind string) (substrate.Observation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	phase := map[string]string{"Resume": "Running", "Suspend": "Suspended", "Pause": "Paused", "Delete": "Deleted"}[kind]
	if kind == "Resume" {
		b.generations[t.Metadata.Uid]++
	}
	if kind == "Delete" {
		delete(b.tasks, t.Metadata.Uid)
	} else {
		b.tasks[t.Metadata.Uid] = phase
	}
	return b.observation(t, phase), nil
}

func (b *compute) observation(t *ax.Task, phase string) substrate.Observation {
	result := substrate.Observation{UID: t.Metadata.Uid, Phase: phase, TemplateUID: "runtime-uid"}
	if phase == "Suspended" && b.generations[t.Metadata.Uid] > 0 {
		result.SnapshotURI = fmt.Sprintf("memory://%s/%d", t.Metadata.Uid, b.generations[t.Metadata.Uid])
		result.Data = true
	}
	return result
}
func (b *compute) Checkpoint(_ context.Context, task *ax.Task, expected string, checkpoint *ax.TaskCheckpoint) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if expected != task.Metadata.Uid || b.tasks[expected] != "Suspended" {
		return "", status.Error(codes.FailedPrecondition, "test boundary changed")
	}
	b.checkpoints[checkpoint.Metadata.Uid] = checkpoint
	return checkpoint.Metadata.Uid, nil
}
func (b *compute) DeleteCheckpoint(_ context.Context, checkpoint *ax.TaskCheckpoint, expected string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if expected != checkpoint.Metadata.Uid {
		return status.Error(codes.FailedPrecondition, "test checkpoint identity changed")
	}
	delete(b.checkpoints, expected)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	root := flag.String("root", "", "isolated fixture workspace and temporary certificate directory")
	flag.Parse()
	if *root == "" {
		return fmt.Errorf("--root is required")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := os.MkdirAll(*root, 0700); err != nil {
		return err
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	identity, _ := url.Parse("spiffe://ax.test/kagent")
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{identity}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, pub, key)
	if err != nil {
		return err
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})
	certFile, keyFile := filepath.Join(*root, "client.pem"), filepath.Join(*root, "client-key.pem")
	if err = os.WriteFile(certFile, certPEM, 0600); err != nil {
		return err
	}
	if err = os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		return err
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	workspace, logs := filepath.Join(*root, "workspace"), filepath.Join(*root, "logs")
	for _, path := range []string{workspace, logs} {
		if err = os.MkdirAll(path, 0700); err != nil {
			return err
		}
	}
	guestServer, closeGuest, err := guest.NewServer(guest.Config{Workspace: workspace, LogDir: logs, EnableProcess: true, EnableFileSystem: true})
	if err != nil {
		return err
	}
	defer closeGuest()
	defer guestServer.Stop()
	guestListener := bufconn.Listen(1 << 20)
	go guestServer.Serve(guestListener)
	guestConn, err := grpc.NewClient("passthrough:///guest", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return guestListener.Dial() }))
	if err != nil {
		return err
	}
	defer guestConn.Close()
	store := memory.NewStore()
	backend := &compute{tasks: map[string]string{}, generations: map[string]int{}, checkpoints: map[string]*ax.TaskCheckpoint{}}
	api := server.NewServer(store, server.Options{Managed: &controller.ManagedController{Store: store, Backend: backend}, ManagedClients: map[string][]string{identity.String(): {"team-a"}}, RuntimeDialer: func(ctx context.Context, _ *ax.Task) (context.Context, grpc.ClientConnInterface, error) {
		return ctx, guestConn, nil
	}})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	httpServer := &http.Server{Handler: api.Handler(), TLSConfig: &tls.Config{Certificates: []tls.Certificate{pair}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}}
	failures := make(chan error, 1)
	go func() { failures <- httpServer.ServeTLS(listener, "", "") }()
	if err = json.NewEncoder(os.Stdout).Encode(map[string]string{"endpoint": listener.Addr().String(), "caFile": certFile, "clientCertFile": certFile, "clientKeyFile": keyFile}); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		return httpServer.Shutdown(shutdown)
	case err := <-failures:
		return err
	}
}
