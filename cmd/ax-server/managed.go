package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/substrate"
)

// Managed configuration is operator-owned. Public TaskGroup configuration stays
// limited to replicas, sandboxClass and snapshotLocation.
type managedConfig struct {
	Clients        map[string][]string              `json:"clients"`
	PoolProfiles   map[string]substrate.PoolProfile `json:"poolProfiles"`
	SandboxConfigs map[string]string                `json:"sandboxConfigs"`
	GuestImage     string                           `json:"guestImage"`
	CallbackURL    string                           `json:"callbackURL"`
	Router         struct {
		Endpoint  string `json:"endpoint"`
		Authority string `json:"authority"`
		CAFile    string `json:"caFile"`
		TokenFile string `json:"tokenFile"`
	} `json:"router"`
}

func managedOptions(file string, s store.ManagedStore, client *substrate.Client) (server.Options, func(), error) {
	f, err := os.Open(file)
	if err != nil {
		return server.Options{}, nil, err
	}
	defer f.Close()
	var cfg managedConfig
	decoder := json.NewDecoder(io.LimitReader(f, 1<<20))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&cfg); err != nil {
		return server.Options{}, nil, err
	}
	var trailing any
	if err = decoder.Decode(&trailing); err != io.EOF {
		return server.Options{}, nil, fmt.Errorf("managed configuration must contain one JSON object")
	}
	if len(cfg.Clients) == 0 || len(cfg.PoolProfiles) == 0 || cfg.Router.Endpoint == "" {
		return server.Options{}, nil, fmt.Errorf("managed clients, worker profiles and router endpoint are required")
	}
	platform, err := substrate.NewInClusterPlatform(cfg.PoolProfiles)
	if err != nil {
		return server.Options{}, nil, err
	}
	backend := &substrate.ManagedBackend{Client: client, Platform: platform, GuestImage: cfg.GuestImage, CallbackURL: cfg.CallbackURL, SandboxConfigs: cfg.SandboxConfigs}
	router, err := substrate.NewClientWithOptions(substrate.ClientOptions{Target: cfg.Router.Endpoint, Authority: cfg.Router.Authority, CAFile: cfg.Router.CAFile, TokenFile: cfg.Router.TokenFile})
	if err != nil {
		return server.Options{}, nil, err
	}
	transport := &substrate.RuntimeTransport{Router: router, Backend: backend}
	return server.Options{Managed: &controller.ManagedController{Store: s, Backend: backend}, ManagedClients: cfg.Clients, RuntimeDialer: transport.Dial}, func() { _ = router.Close() }, nil
}
func managedTLS(certFile, keyFile, clientCAFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" || clientCAFile == "" {
		return nil, fmt.Errorf("managed listener requires TLS certificate, key and client CA")
	}
	if _, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(clientCAFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(raw) {
		return nil, fmt.Errorf("managed client CA contains no certificates")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}, nil
}
