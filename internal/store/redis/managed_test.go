package redis

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/store"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This test deliberately requires real Redis. No embedded emulator or -short
// fallback is accepted as evidence for WATCH/MULTI and persistence semantics.
func TestManagedRedisCAS(t *testing.T) {
	address := os.Getenv("AX_TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("real Redis not configured: set AX_TEST_REDIS_ADDR")
	}
	client := goredis.NewClient(&goredis.Options{Addr: address, Password: os.Getenv("AX_TEST_REDIS_PASSWORD")})
	t.Cleanup(func() { client.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	s := NewStore(client, Options{KeyPrefix: "ax-test-" + rand.Text()})
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		client.Del(cleanupCtx, s.managedKey("test"))
	})
	var workers sync.WaitGroup
	failures := make(chan error, 8)
	for range 8 {
		workers.Go(func() {
			for range 20 {
				for {
					err := s.UpdateManaged(ctx, "test", func(records store.ManagedRecords) error {
						var count int
						if raw := records["counter"]; raw != nil {
							if err := json.Unmarshal(raw, &count); err != nil {
								return err
							}
						}
						count++
						raw, err := json.Marshal(count)
						records["counter"] = raw
						return err
					})
					if status.Code(err) == codes.Aborted {
						continue
					}
					if err != nil {
						failures <- err
					}
					break
				}
			}
		})
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	records, err := s.ReadManaged(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err = json.Unmarshal(records["counter"], &count); err != nil {
		t.Fatal(err)
	}
	if count != 160 {
		t.Fatalf("lost CAS updates: %d", count)
	}
	ttl, err := client.TTL(ctx, s.managedKey("test")).Result()
	if err != nil || ttl != -1 {
		t.Fatalf("managed ledger must not expire: ttl=%v error=%v", ttl, err)
	}
	rejected := status.Error(codes.FailedPrecondition, "referenced")
	if err = s.UpdateManaged(ctx, "test", func(records store.ManagedRecords) error { delete(records, "counter"); return rejected }); status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	records, err = s.ReadManaged(ctx, "test")
	if err != nil || string(records["counter"]) != "160" {
		t.Fatalf("rejected transaction committed: %v %v", records, err)
	}
}

func TestManagedLedgerGuardRealRedis(t *testing.T) {
	address := os.Getenv("AX_TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("real Redis not configured: set AX_TEST_REDIS_ADDR")
	}
	client := goredis.NewClient(&goredis.Options{Addr: address, Password: os.Getenv("AX_TEST_REDIS_PASSWORD")})
	defer client.Close()
	s := NewStore(client, Options{KeyPrefix: "ax-guard-test-" + rand.Text(), ManagedEpoch: rand.Text()})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	t.Cleanup(func() { client.Del(context.Background(), s.guardKey(), s.managedKey("team")) })
	if err := s.ManagedReady(ctx); err == nil {
		t.Fatal("uninitialized ledger ready")
	}
	if err := s.UpdateManaged(ctx, "team", func(store.ManagedRecords) error { return nil }); err == nil {
		t.Fatal("server bootstrapped itself")
	}
	if err := s.InitializeManaged(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.InitializeManaged(ctx); err == nil {
		t.Fatal("repeat bootstrap allowed")
	}
	if err := s.ManagedReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateManaged(ctx, "team", func(rs store.ManagedRecords) error { rs["record"] = json.RawMessage(`{"uid":"original"}`); return nil }); err != nil {
		t.Fatal(err)
	}
	original, err := client.Get(ctx, s.managedKey("team")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Del(ctx, s.managedKey("team")).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.ManagedReady(ctx); status.Code(err) != codes.DataLoss {
		t.Fatalf("missing namespace: %v", err)
	}
	if _, err := s.ReadManaged(ctx, "team"); status.Code(err) != codes.DataLoss {
		t.Fatalf("missing read: %v", err)
	}
	called := false
	if err := s.UpdateManaged(ctx, "team", func(store.ManagedRecords) error { called = true; return nil }); status.Code(err) != codes.DataLoss || called {
		t.Fatalf("lost namespace recreated: %v", err)
	}
	if err := client.Set(ctx, s.managedKey("team"), original, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.ManagedReady(ctx); err != nil {
		t.Fatal(err)
	}
	other := NewStore(client, Options{KeyPrefix: s.opts.KeyPrefix, ManagedEpoch: rand.Text()})
	if _, err := other.ReadManaged(ctx, "team"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("epoch mismatch: %v", err)
	}
	if err := client.Del(ctx, s.guardKey()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.InitializeManaged(ctx); err == nil {
		t.Fatal("existing records adopted by bootstrap")
	}
	if _, err := s.ReadManaged(ctx, "team"); err == nil {
		t.Fatal("guard loss ignored")
	}
}
