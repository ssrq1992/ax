package substrate

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestManagedDeletionWaitsForAbsence(t *testing.T) {
	calls := 0
	err := waitManagedDeletion(t.Context(), "uid", func() (string, error) {
		calls++
		if calls < 3 {
			return "uid", nil
		}
		return "", status.Error(codes.NotFound, "gone")
	})
	if err != nil || calls != 3 {
		t.Fatalf("deletion released a live reference: calls=%d err=%v", calls, err)
	}
}
func TestManagedDeletionRejectsReplacement(t *testing.T) {
	err := waitManagedDeletion(context.Background(), "original", func() (string, error) { return "replacement", nil })
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("replacement accepted: %v", err)
	}
}
