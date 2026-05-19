package phonebook

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

func TestUpsertAndLookup(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const pub = "aa11"
	const tok = "fcm-token-1"

	if err := s.Upsert(ctx, pub, tok, "android"); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got, err := s.Lookup(ctx, pub)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if got.FCMToken != tok {
		t.Fatalf("got token %q, want %q", got.FCMToken, tok)
	}
	if got.Platform != "android" {
		t.Fatalf("got platform %q, want android", got.Platform)
	}
}

func TestUpsertReplacesExisting(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_ = s.Upsert(ctx, "aa11", "old", "android")
	_ = s.Upsert(ctx, "aa11", "new", "android")
	got, _ := s.Lookup(ctx, "aa11")
	if got.FCMToken != "new" {
		t.Fatalf("expected token replaced; got %q", got.FCMToken)
	}
}

func TestDeleteRemovesEntry(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_ = s.Upsert(ctx, "aa11", "tok", "android")
	if err := s.Delete(ctx, "aa11"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Lookup(ctx, "aa11"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestLookupMissingReturnsErrNotFound(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Lookup(context.Background(), "missing"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
