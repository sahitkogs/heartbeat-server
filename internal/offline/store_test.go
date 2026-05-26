package offline

import (
	"context"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestEnqueueIncreasesDepth(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.Enqueue(ctx, "alice", "bob", []byte("env-1")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	n, err := s.Depth(ctx, "alice")
	if err != nil {
		t.Fatalf("Depth: %v", err)
	}
	if n != 1 {
		t.Fatalf("Depth = %d, want 1", n)
	}
}

func TestDepthIsolatedPerRecipient(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_ = s.Enqueue(ctx, "alice", "bob", []byte("a"))
	_ = s.Enqueue(ctx, "alice", "bob", []byte("b"))
	_ = s.Enqueue(ctx, "carol", "bob", []byte("c"))
	if n, _ := s.Depth(ctx, "alice"); n != 2 {
		t.Fatalf("alice depth %d, want 2", n)
	}
	if n, _ := s.Depth(ctx, "carol"); n != 1 {
		t.Fatalf("carol depth %d, want 1", n)
	}
}

func TestEnqueueRejectsOversizeEnvelope(t *testing.T) {
	s := newTestStore(t)
	big := make([]byte, MaxEnvelopeBytes+1)
	if err := s.Enqueue(context.Background(), "alice", "bob", big); err != ErrEnvelopeTooLarge {
		t.Fatalf("got err %v, want ErrEnvelopeTooLarge", err)
	}
}
