package store

import (
	"context"
	"encoding/json"
)

// ManagedRecords is the private, versioned ledger for one atespace. Transactions
// are short and never call a backend. Keeping related records in one atomic
// document prevents a delete from racing a reference reservation. Different
// atespaces do not contend. This deliberately targets a single logical Redis,
// not cross-slot transactions in a sharded Redis Cluster.
type ManagedRecords map[string]json.RawMessage

type ManagedStore interface {
	ReadManaged(context.Context, string) (ManagedRecords, error)
	UpdateManaged(context.Context, string, func(ManagedRecords) error) error
}
