package substrate

import (
	"context"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// RuntimeTransport owns the private router connection. Routing metadata is
// reconstructed from the admitted Task UID; caller routing/auth headers cannot
// override the platform identity installed on this connection.
type RuntimeTransport struct {
	Router  *Client
	Backend *ManagedBackend
}

func (t *RuntimeTransport) Dial(ctx context.Context, task *ax.Task) (context.Context, grpc.ClientConnInterface, error) {
	md := metadata.MD{}
	incoming, _ := metadata.FromIncomingContext(ctx)
	for _, key := range []string{"traceparent", "tracestate", "baggage", "a2a-extensions", "a2a-version", "x-kagent-dispatch-id"} {
		if values := incoming.Get(key); len(values) > 0 {
			md[key] = append([]string(nil), values...)
		}
	}
	md.Set("ate-target-actor", t.Backend.Target(task))
	return metadata.NewOutgoingContext(ctx, md), t.Router.conn, nil
}
