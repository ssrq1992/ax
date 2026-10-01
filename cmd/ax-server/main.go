// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/redis"
	"github.com/google/ax/internal/substrate"
	goredis "github.com/redis/go-redis/v9"
)

func main() {
	var (
		listenAddr                                                     string
		managedFile, managedAddr, tlsCertFile, tlsKeyFile, tlsClientCA string
		redisAddr                                                      string
		redisPassword                                                  string
		managedEpoch                                                   string
		initializeLedger                                               bool
		recoverySpace, recoveryFile                                    string
		recoveryFenced                                                 bool
		substrateEndpoint                                              string
		substrateAuthority                                             string
		substrateTokenFile                                             string
		substrateCAFile                                                string
		substrateInsecureTLS                                           bool
		substratePlaintext                                             bool
		defaultTemplate                                                string
		defaultTemplateAtespace                                        string
	)

	flag.StringVar(&listenAddr, "addr", ":8080", "HTTP listen address")
	flag.StringVar(&redisAddr, "redis-addr", "localhost:6379", "Redis server address")
	flag.StringVar(&redisPassword, "redis-password", "", "Redis password")
	flag.StringVar(&substrateEndpoint, "substrate-endpoint", "api.ate-system.svc.cluster.local:443", "Agent Substrate Control API endpoint")
	flag.StringVar(&substrateAuthority, "substrate-authority", "api.ate-system.svc", "Authority / TLS ServerName for Substrate endpoint")
	flag.StringVar(&substrateTokenFile, "substrate-token-file", "", "Path to bearer token file for Substrate auth")
	flag.StringVar(&substrateCAFile, "substrate-ca-file", "", "Path to CA PEM file for Substrate TLS")
	flag.BoolVar(&substrateInsecureTLS, "substrate-insecure-tls", false, "Skip Substrate TLS verification")
	flag.BoolVar(&substratePlaintext, "substrate-plaintext", false, "Use insecure plaintext gRPC connection to Substrate")
	flag.StringVar(&defaultTemplate, "template", "default-template", "Default Substrate ActorTemplate name")
	flag.StringVar(&defaultTemplateAtespace, "template-atespace", "ax-system", "Default Substrate ActorTemplate atespace")
	flag.StringVar(&managedFile, "managed-config", "", "Operator configuration for managed runtimes")
	flag.StringVar(&managedAddr, "managed-addr", ":8443", "mTLS managed API and TaskGateway listen address")
	flag.StringVar(&tlsCertFile, "tls-cert-file", "", "Managed listener certificate")
	flag.StringVar(&tlsKeyFile, "tls-key-file", "", "Managed listener private key")
	flag.StringVar(&tlsClientCA, "tls-client-ca-file", "", "Trusted managed client CA")
	flag.StringVar(&managedEpoch, "managed-ledger-epoch", "", "Externally retained managed ledger epoch")
	flag.BoolVar(&initializeLedger, "initialize-managed-ledger", false, "Explicitly bootstrap an empty managed ledger and exit; never use after data loss")
	flag.StringVar(&recoverySpace, "recover-atespace", "", "Offline: list unresolved operations in this atespace and exit")
	flag.StringVar(&recoveryFile, "recover-operation-file", "", "Offline: apply one inspected recovery plan object and exit")
	flag.BoolVar(&recoveryFenced, "confirm-executors-fenced", false, "Operator attestation: all AX executors stopped and backend requests drained")
	flag.Parse()
	if (recoverySpace != "" && (managedFile == "" || initializeLedger)) || (recoveryFile != "" && recoverySpace == "") || (recoveryFenced && recoveryFile == "") {
		slog.Error("recovery requires managed-config, recover-atespace and an existing ledger; fencing is only used with an operation file")
		os.Exit(1)
	}

	if envAddr := os.Getenv("ADDR"); envAddr != "" {
		listenAddr = envAddr
	}
	if envRedis := os.Getenv("REDIS_ADDR"); envRedis != "" {
		redisAddr = envRedis
	}
	if envPass := os.Getenv("REDIS_PASSWORD"); envPass != "" {
		redisPassword = envPass
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	slog.Info("starting ax-server",
		"listenAddr", listenAddr,
		"redisAddr", redisAddr,
		"substrateEndpoint", substrateEndpoint,
		"template", defaultTemplate,
	)

	rClient := goredis.NewClient(&goredis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
	})
	defer rClient.Close()

	rStore := redis.NewStore(rClient, redis.Options{ManagedEpoch: managedEpoch})
	if initializeLedger {
		if err := rStore.InitializeManaged(context.Background()); err != nil {
			slog.Error("initialize managed ledger", "error", err)
			os.Exit(1)
		}
		slog.Info("managed ledger initialized; retain the epoch outside Redis")
		return
	}
	if managedFile != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := rStore.ManagedReady(ctx)
		cancel()
		if err != nil {
			slog.Error("managed ledger is not ready", "error", err)
			os.Exit(1)
		}
	}
	rLocker := lock.NewRedisLocker(rClient, lock.RedisLockerOptions{})

	var reconciler server.Reconciler
	subClient, err := substrate.NewClientWithOptions(substrate.ClientOptions{
		Target:      substrateEndpoint,
		Authority:   substrateAuthority,
		TokenFile:   substrateTokenFile,
		CAFile:      substrateCAFile,
		InsecureTLS: substrateInsecureTLS,
		Plaintext:   substratePlaintext,
	})
	if err != nil {
		slog.Warn("could not initialize substrate client; running without substrate reconciliation", "error", err)
	} else {
		defer subClient.Close()
		reconciler = controller.NewTaskReconciler(subClient, defaultTemplate, defaultTemplateAtespace)
	}

	options := server.Options{Locker: rLocker, Reconciler: reconciler}
	var secureServer *http.Server
	if managedFile != "" {
		if subClient == nil || substratePlaintext || substrateInsecureTLS {
			slog.Error("managed runtime requires a verified Substrate connection")
			os.Exit(1)
		}
		managed, closeManaged, err := managedOptions(managedFile, rStore, subClient)
		if err != nil {
			slog.Error("invalid managed configuration", "error", err)
			os.Exit(1)
		}
		defer closeManaged()
		options.Managed = managed.Managed
		options.ManagedReady = rStore.ManagedReady
		options.ManagedClients = managed.ManagedClients
		options.RuntimeDialer = managed.RuntimeDialer
		if recoverySpace != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			err := runRecovery(ctx, options.Managed, recoverySpace, recoveryFile, recoveryFenced)
			cancel()
			if err != nil {
				slog.Error("offline recovery failed; ownership retained", "error", err)
				os.Exit(1)
			}
			return
		}
		tlsConfig, err := managedTLS(tlsCertFile, tlsKeyFile, tlsClientCA)
		if err != nil {
			slog.Error("invalid managed TLS configuration", "error", err)
			os.Exit(1)
		}
		secureServer = &http.Server{Addr: managedAddr, TLSConfig: tlsConfig, ReadHeaderTimeout: 10 * time.Second}
	}
	srv := server.NewServer(rStore, options)
	if secureServer != nil {
		secureServer.Handler = srv.Handler()
	}

	httpServer := &http.Server{
		Addr:    listenAddr,
		Handler: srv.Handler(),
	}
	httpServer.Protocols = new(http.Protocols)
	httpServer.Protocols.SetHTTP1(true)
	httpServer.Protocols.SetUnencryptedHTTP2(true)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()

	if secureServer != nil {
		go func() {
			if err := secureServer.ListenAndServeTLS(tlsCertFile, tlsKeyFile); err != nil && err != http.ErrServerClosed {
				slog.Error("managed server failed", "error", err)
				cancel()
			}
		}()
	}
	<-ctx.Done()
	slog.Info("shutting down ax-server")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	if secureServer != nil {
		_ = secureServer.Shutdown(shutdownCtx)
	}
}
