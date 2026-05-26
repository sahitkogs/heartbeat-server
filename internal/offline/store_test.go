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

func TestLoadForReturnsFIFO(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_ = s.Enqueue(ctx, "alice", "bob", []byte("first"))
	_ = s.Enqueue(ctx, "alice", "bob", []byte("second"))
	_ = s.Enqueue(ctx, "alice", "carol", []byte("third"))
	entries, err := s.LoadFor(ctx, "alice")
	if err != nil {
		t.Fatalf("LoadFor: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}
	if string(entries[0].Envelope) != "first" ||
		string(entries[1].Envelope) != "second" ||
		string(entries[2].Envelope) != "third" {
		t.Fatalf("envelopes out of order: %v", entries)
	}
	if entries[0].Sender != "bob" || entries[2].Sender != "carol" {
		t.Fatalf("sender field lost: %v", entries)
	}
}

func TestLoadForDoesNotDelete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_ = s.Enqueue(ctx, "alice", "bob", []byte("env"))
	_, _ = s.LoadFor(ctx, "alice")
	if n, _ := s.Depth(ctx, "alice"); n != 1 {
		t.Fatalf("LoadFor should not delete; depth=%d", n)
	}
}

func TestLoadForUnknownRecipient(t *testing.T) {
	s := newTestStore(t)
	entries, err := s.LoadFor(context.Background(), "nobody")
	if err != nil {
		t.Fatalf("LoadFor: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
}

func TestDeleteRemovesRow(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_ = s.Enqueue(ctx, "alice", "bob", []byte("env"))
	entries, _ := s.LoadFor(ctx, "alice")
	if err := s.Delete(ctx, entries[0].ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if n, _ := s.Depth(ctx, "alice"); n != 0 {
		t.Fatalf("expected depth=0 after Delete, got %d", n)
	}
}

func TestDeleteMissingIDNoError(t *testing.T) {
	s := newTestStore(t)
	if err := s.Delete(context.Background(), 9999); err != nil {
		t.Fatalf("Delete of missing id should not error, got %v", err)
	}
}

func TestEnqueueEvictsOldestAtCap(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for i := 0; i < MaxPerRecipient; i++ {
		payload := []byte{byte(i), byte(i >> 8)}
		if err := s.Enqueue(ctx, "alice", "bob", payload); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	if n, _ := s.Depth(ctx, "alice"); n != MaxPerRecipient {
		t.Fatalf("expected depth=%d at fill, got %d", MaxPerRecipient, n)
	}
	if err := s.Enqueue(ctx, "alice", "bob", []byte("overflow")); err != nil {
		t.Fatalf("overflow Enqueue: %v", err)
	}
	if n, _ := s.Depth(ctx, "alice"); n != MaxPerRecipient {
		t.Fatalf("expected depth held at %d post-eviction, got %d", MaxPerRecipient, n)
	}
	entries, _ := s.LoadFor(ctx, "alice")
	first := entries[0].Envelope
	if first[0] != 1 || first[1] != 0 {
		t.Fatalf("oldest entry not evicted; first remaining=%v", first)
	}
	last := entries[len(entries)-1].Envelope
	if string(last) != "overflow" {
		t.Fatalf("newest entry missing; last=%q", string(last))
	}
}
