package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type managedGuard struct {
	Epoch  string          `json:"epoch"`
	Spaces map[string]bool `json:"spaces"`
}

func (s *Store) guardKey() string { return s.opts.KeyPrefix + ":managed:guard" }
func (s *Store) decodeGuard(raw string, err error) (*managedGuard, error) {
	if err != nil {
		if err == redis.Nil {
			return nil, status.Error(codes.FailedPrecondition, "managed ledger guard missing; bootstrap a new installation or restore its ledger")
		}
		return nil, err
	}
	var guard managedGuard
	if json.Unmarshal([]byte(raw), &guard) != nil || guard.Epoch != s.opts.ManagedEpoch || guard.Spaces == nil {
		return nil, status.Error(codes.FailedPrecondition, "managed ledger epoch mismatch or corrupt guard")
	}
	return &guard, nil
}

// InitializeManaged is an explicit operator bootstrap, never a server-startup
// fallback. The epoch must also be retained outside Redis in operator config.
func (s *Store) InitializeManaged(ctx context.Context) error {
	if len(s.opts.ManagedEpoch) < 16 {
		return fmt.Errorf("managed epoch must contain at least 16 characters")
	}
	return s.client.Watch(ctx, func(tx *redis.Tx) error {
		if _, err := tx.Get(ctx, s.guardKey()).Result(); err != redis.Nil {
			if err == nil {
				return fmt.Errorf("managed ledger already initialized")
			}
			return err
		}
		var cursor uint64
		for {
			keys, next, err := tx.Scan(ctx, cursor, s.opts.KeyPrefix+":managed:v1:*", 100).Result()
			if err != nil {
				return err
			}
			if len(keys) > 0 {
				return fmt.Errorf("existing managed records require recovery, not bootstrap")
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		raw, _ := json.Marshal(managedGuard{Epoch: s.opts.ManagedEpoch, Spaces: map[string]bool{}})
		_, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error { p.Set(ctx, s.guardKey(), raw, 0); return nil })
		return err
	}, s.guardKey())
}
func (s *Store) ManagedReady(ctx context.Context) error {
	if s.opts.ManagedEpoch == "" {
		return fmt.Errorf("managed ledger epoch is required")
	}
	guard, err := s.decodeGuard(s.client.Get(ctx, s.guardKey()).Result())
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(guard.Spaces))
	for space := range guard.Spaces {
		keys = append(keys, s.managedKey(space))
	}
	if len(keys) == 0 {
		return nil
	}
	count, err := s.client.Exists(ctx, keys...).Result()
	if err != nil {
		return err
	}
	if count != int64(len(keys)) {
		return status.Error(codes.DataLoss, "managed namespace ledger missing; restore required")
	}
	return nil
}
