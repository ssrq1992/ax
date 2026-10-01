// ax-sandbox-guest packages AX's pinned private Guest implementation. The public
// TaskExecutionService is served by AX Server, not by this in-sandbox listener.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/agent-substrate/env/guest"
)

func main() {
	config := guest.Config{EnableProcess: true, EnableFileSystem: true}
	flag.StringVar(&config.ListenAddr, "listen", ":80", "Guest listen address")
	flag.StringVar(&config.Workspace, "workspace", "/data/workspace", "Process working directory and file root")
	flag.StringVar(&config.LogDir, "log-dir", "/data/guest-logs", "Durable process output directory")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, config); err != nil {
		slog.Error("AX Guest stopped", "error", err)
		os.Exit(1)
	}
}
func run(ctx context.Context, config guest.Config) error {
	for _, dir := range []string{config.Workspace, config.LogDir} {
		if dir == "" {
			return fmt.Errorf("workspace and log directory are required")
		}
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	server, cleanup, err := guest.NewServer(config)
	if err != nil {
		return err
	}
	defer cleanup()
	defer server.Stop()
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", config.ListenAddr)
	if err != nil {
		return err
	}
	defer listener.Close()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.Handle("/", server)
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	httpServer.Protocols = new(http.Protocols)
	httpServer.Protocols.SetHTTP1(true)
	httpServer.Protocols.SetUnencryptedHTTP2(true)
	done := make(chan error, 1)
	go func() { done <- httpServer.Serve(listener) }()
	select {
	case err := <-done:
		if err != http.ErrServerClosed {
			return err
		}
		return nil
	case <-ctx.Done():
	}
	server.Stop() // Cancel streams before cleanup waits for request goroutines.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return httpServer.Shutdown(shutdownCtx)
}
