package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/ax/internal/store"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Separate keys and no TTL prevent legacy SaveTask and its optional TTL from
// overwriting or expiring managed bindings. Schema mismatches fail closed.
type managedDocument struct {
	Schema   int                  `json:"schema"`
	Revision uint64               `json:"revision"`
	Records  store.ManagedRecords `json:"records"`
}

func decodeManaged(raw string, err error) (*managedDocument, error) {
	if errors.Is(err, redis.Nil) {
		return &managedDocument{Schema: 1, Records: make(store.ManagedRecords)}, nil
	}
	if err != nil {
		return nil, err
	}
	var d managedDocument
	if err = json.Unmarshal([]byte(raw), &d); err != nil {
		return nil, fmt.Errorf("decode managed ledger: %w", err)
	}
	if d.Schema != 1 || d.Records == nil {
		return nil, status.Error(codes.FailedPrecondition, "managed ledger requires recovery or a compatible server")
	}
	return &d, nil
}
func (s *Store) managedKey(space string) string { return s.opts.KeyPrefix + ":managed:v1:" + space }
func (s *Store) ReadManaged(ctx context.Context, space string) (store.ManagedRecords, error) {
	var raw string
	var readErr error
	if s.opts.ManagedEpoch != "" {
		values, err := s.client.MGet(ctx, s.guardKey(), s.managedKey(space)).Result()
		if err != nil {
			return nil, err
		}
		guardRaw, ok := values[0].(string)
		if !ok {
			return nil, status.Error(codes.FailedPrecondition, "managed ledger guard missing; restore required")
		}
		guard, err := s.decodeGuard(guardRaw, nil)
		if err != nil {
			return nil, err
		}
		if values[1] == nil {
			if guard.Spaces[space] {
				return nil, status.Error(codes.DataLoss, "managed namespace ledger missing; restore required")
			}
			readErr = redis.Nil
		} else {
			raw, ok = values[1].(string)
			if !ok {
				return nil, status.Error(codes.DataLoss, "invalid managed ledger value")
			}
			if !guard.Spaces[space] {
				return nil, status.Error(codes.DataLoss, "unregistered managed namespace; restore required")
			}
		}
	} else {
		raw, readErr = s.client.Get(ctx, s.managedKey(space)).Result()
	}
	d, err := decodeManaged(raw, readErr)
	if err != nil {
		return nil, err
	}
	return d.Records, nil
}
func (s *Store) UpdateManaged(ctx context.Context, space string, fn func(store.ManagedRecords) error) error {
	key := s.managedKey(space)
	keys := []string{key}
	if s.opts.ManagedEpoch != "" {
		keys = append(keys, s.guardKey())
	}
	for attempt := 0; attempt < 16; attempt++ {
		err := s.client.Watch(ctx, func(tx *redis.Tx) error {
			var guard *managedGuard
			var err error
			if s.opts.ManagedEpoch != "" {
				guard, err = s.decodeGuard(tx.Get(ctx, s.guardKey()).Result())
				if err != nil {
					return err
				}
			}
			rawValue, readErr := tx.Get(ctx, key).Result()
			if guard != nil && ((guard.Spaces[space] && errors.Is(readErr, redis.Nil)) || (!guard.Spaces[space] && readErr == nil)) {
				return status.Error(codes.DataLoss, "managed namespace ledger and guard disagree; restore required")
			}
			d, err := decodeManaged(rawValue, readErr)
			if err != nil {
				return err
			}
			if err = fn(d.Records); err != nil {
				return err
			}
			d.Revision++
			raw, err := json.Marshal(d)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.Set(ctx, key, raw, 0)
				if guard != nil && !guard.Spaces[space] {
					guard.Spaces[space] = true
					guardRaw, err := json.Marshal(guard)
					if err != nil {
						return err
					}
					p.Set(ctx, s.guardKey(), guardRaw, 0)
				}
				return nil
			})
			return err
		}, keys...)
		if !errors.Is(err, redis.TxFailedErr) {
			return err
		}
	}
	return status.Error(codes.Aborted, "managed resource transaction contention; retry the same request")
}
