package memory

import (
	"context"
	"encoding/json"
	"github.com/google/ax/internal/store"
)

func copyRecords(in store.ManagedRecords) store.ManagedRecords {
	out := make(store.ManagedRecords, len(in))
	for k, v := range in {
		out[k] = append(json.RawMessage(nil), v...)
	}
	return out
}
func (s *MemoryStore) ReadManaged(ctx context.Context, space string) (store.ManagedRecords, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return copyRecords(s.managed[space]), nil
}
func (s *MemoryStore) UpdateManaged(ctx context.Context, space string, fn func(store.ManagedRecords) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	records := copyRecords(s.managed[space])
	if err := fn(records); err != nil {
		return err
	}
	if s.managed == nil {
		s.managed = make(map[string]store.ManagedRecords)
	}
	s.managed[space] = copyRecords(records)
	return nil
}
