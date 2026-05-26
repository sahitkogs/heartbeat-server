# Phase 1 — Server Offline Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a SQLite-backed per-recipient offline queue to heartbeat-server so messages sent while the recipient is offline are persisted server-side and flushed to the recipient on next WS connect, with delete-on-push semantics.

**Architecture:** New `internal/offline` package mirroring the existing `internal/phonebook` pattern (concrete `Store` struct over `database/sql` + `modernc.org/sqlite`). Two integration points in `internal/signaling/handlers.go`: enqueue on `DeliverTo` failure, async flush on WS connect. Sweeper goroutine deletes rows older than 7 days. Reuses the existing `hb-data:/var/lib/heartbeat` Docker volume.

**Tech Stack:** Go 1.26, `modernc.org/sqlite` (pure-Go driver — already an indirect dep, this plan promotes it to direct), `nhooyr.io/websocket` (no change). No new external dependencies.

**Spec:** `heart-beat-v3/docs/2026-05-26-message-delivery-guarantees-design.md` §6 (Server-side changes). This plan implements Phase 1 of §10 Rollout.

---

## File Structure

**New files:**

| File | Responsibility |
|---|---|
| `internal/offline/store.go` | `Store` type + `Open`, `Close`, `Enqueue`, `LoadFor`, `Delete`, `Sweep`, `Depth` — the SQLite-backed queue. Mirrors `internal/phonebook/store.go` style. |
| `internal/offline/store_test.go` | Unit tests against `file::memory:` SQLite. |
| `internal/offline/sweeper.go` | `RunSweeper` goroutine — periodic Sweep ticker. |
| `internal/offline/sweeper_test.go` | Sweeper unit tests with injected clock-ish behavior. |

**Modified files:**

| File | Change |
|---|---|
| `go.mod` | Promote `modernc.org/sqlite` from indirect to direct. |
| `internal/signaling/handlers.go` | Add `Offline *offline.Store` field on `Handlers`; new `enqueueOffline` + `flushOffline` helpers; one extra line in `DeliverTo` false branch; one extra line after `Hub.Add` in `Signal`. |
| `internal/signaling/handlers_test.go` (new) | Handler-level integration tests against fake WS sessions. |
| `internal/health/handlers.go` | Optional dependency on offline store for `offline_queue_total` in `/healthz` JSON. |
| `cmd/heartbeat-server/main.go` | Instantiate `offline.Store`, start `RunSweeper`, pass to `NewHandlers`, add `-offline-db` flag. Bump `version` constant. |

**Docker / persistence:**

The existing `hb-data:/var/lib/heartbeat` volume already survives container redeploys (it holds `phonebook.db`). The new `offline-queue.db` lives in the same volume — **no docker-compose changes required.**

---

## Task 1: Promote `modernc.org/sqlite` to a direct dependency

**Files:**
- Modify: `go.mod`

The package is already in `go.mod` as `// indirect` (phonebook imports it via blank import). We add a direct import in the offline package later, so promote it now so `go mod tidy` doesn't churn the file.

- [ ] **Step 1: Inspect current entry**

Run:
```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server grep -n "modernc.org/sqlite" go.mod
```

Expected: one line, ending in `// indirect`.

- [ ] **Step 2: Add a require block for direct deps if not present**

Inspect the top of `go.mod`. If a non-indirect `require (...)` block exists, add `modernc.org/sqlite v1.50.1` to it. If only the single indirect block exists, leave it for now — `go mod tidy` after Task 2 will reclassify.

- [ ] **Step 3: Commit (only if go.mod actually changed)**

```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server status --short
```

If `go.mod` modified, commit:
```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server add go.mod
git -C C:\Users\Lambda\Documents\heartbeat-server commit -m "deps: prepare modernc.org/sqlite for direct import"
```

Otherwise skip — `go mod tidy` after Task 4 will reclassify automatically.

---

## Task 2: Create the `offline` package skeleton

**Files:**
- Create: `internal/offline/store.go`

- [ ] **Step 1: Write the file**

```go
// Package offline persists ciphertext envelopes destined for recipients
// who are not currently connected via WebSocket. Rows are inserted by the
// signaling handler when DeliverTo returns false and deleted as each row
// is successfully pushed to a freshly-connected recipient.
//
// Storage is a SQLite file (modernc.org/sqlite, no CGO). The server treats
// each envelope as opaque bytes — it never parses or inspects content.
package offline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Entry is one queued envelope ready to flush.
type Entry struct {
	ID          int64
	Sender      string // pubkey hex of the original sender
	Envelope    []byte // opaque ciphertext
	EnqueuedAt  time.Time
}

// Store is the SQLite-backed offline queue.
type Store struct {
	db *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS offline_queue (
	id                INTEGER PRIMARY KEY AUTOINCREMENT,
	recipient_pubkey  TEXT    NOT NULL,
	sender_pubkey     TEXT    NOT NULL,
	envelope          BLOB    NOT NULL,
	enqueued_at       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_offline_recipient
	ON offline_queue(recipient_pubkey, enqueued_at);`

// Open opens or creates the offline-queue DB at the given DSN.
func Open(dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open offline db: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init offline schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Close releases DB resources.
func (s *Store) Close() error { return s.db.Close() }

// ErrEnvelopeTooLarge is returned by Enqueue when the envelope exceeds the
// per-envelope size cap (defense in depth — the WS frame size already
// bounds this).
var ErrEnvelopeTooLarge = errors.New("offline: envelope exceeds size cap")

const (
	// MaxPerRecipient is the hard cap on queued rows per recipient pubkey.
	// On insert when the cap is hit, the oldest row for that recipient is
	// evicted. At ~10 users with normal usage this is unreachable; the cap
	// exists as a defense against a stuck-offline recipient accumulating
	// unbounded state.
	MaxPerRecipient = 500

	// MaxEnvelopeBytes bounds the size of a single queued envelope.
	MaxEnvelopeBytes = 64 * 1024
)
```

- [ ] **Step 2: Build and tidy**

```powershell
cd C:\Users\Lambda\Documents\heartbeat-server; go build ./...; go mod tidy
```

Expected: clean build. `go mod tidy` removes the `// indirect` marker on `modernc.org/sqlite` since the offline package imports it directly.

- [ ] **Step 3: Commit**

```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server add internal/offline/store.go go.mod go.sum
git -C C:\Users\Lambda\Documents\heartbeat-server commit -m "offline: skeleton + schema"
```

---

## Task 3: TDD — `Enqueue` and `Depth`

**Files:**
- Create: `internal/offline/store_test.go`
- Modify: `internal/offline/store.go` (add Enqueue + Depth methods)

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run the test to verify it fails**

```powershell
cd C:\Users\Lambda\Documents\heartbeat-server; go test ./internal/offline/ -run TestEnqueue -v
```

Expected: `undefined: s.Enqueue` (or similar build error).

- [ ] **Step 3: Implement `Enqueue` and `Depth`**

Add to `internal/offline/store.go`:

```go
// Enqueue appends an envelope for recipient. If the per-recipient cap is
// hit, the oldest row for that recipient is evicted before insert.
// ErrEnvelopeTooLarge if envelope exceeds MaxEnvelopeBytes.
func (s *Store) Enqueue(ctx context.Context, recipient, sender string, envelope []byte) error {
	if len(envelope) > MaxEnvelopeBytes {
		return ErrEnvelopeTooLarge
	}
	// Cap eviction: if at-or-over the cap, delete the oldest before insert.
	var n int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM offline_queue WHERE recipient_pubkey = ?`, recipient,
	).Scan(&n); err != nil {
		return fmt.Errorf("count for cap: %w", err)
	}
	if n >= MaxPerRecipient {
		if _, err := s.db.ExecContext(ctx,
			`DELETE FROM offline_queue WHERE id = (
				SELECT id FROM offline_queue
				WHERE recipient_pubkey = ?
				ORDER BY enqueued_at ASC LIMIT 1)`,
			recipient,
		); err != nil {
			return fmt.Errorf("evict oldest: %w", err)
		}
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO offline_queue (recipient_pubkey, sender_pubkey, envelope, enqueued_at)
		 VALUES (?, ?, ?, ?)`,
		recipient, sender, envelope, time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}
	return nil
}

// Depth returns the number of queued rows for recipient.
func (s *Store) Depth(ctx context.Context, recipient string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM offline_queue WHERE recipient_pubkey = ?`, recipient,
	).Scan(&n)
	return n, err
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```powershell
go test ./internal/offline/ -run TestEnqueue -v; go test ./internal/offline/ -run TestDepth -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server add internal/offline/
git -C C:\Users\Lambda\Documents\heartbeat-server commit -m "offline: Enqueue + Depth"
```

---

## Task 4: TDD — `LoadFor` (ordered, read-only)

**Files:**
- Modify: `internal/offline/store_test.go`
- Modify: `internal/offline/store.go`

- [ ] **Step 1: Write failing tests**

Append to `store_test.go`:

```go
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
```

- [ ] **Step 2: Run to confirm failure**

```powershell
go test ./internal/offline/ -run TestLoadFor -v
```

Expected: build error or undefined.

- [ ] **Step 3: Implement `LoadFor`**

Add to `store.go`:

```go
// LoadFor returns all rows for recipient ordered by enqueued_at ASC.
// READ-ONLY: callers delete each row by ID via Delete after successful push.
// This split lets an abandoned flush leave un-pushed rows in place.
func (s *Store) LoadFor(ctx context.Context, recipient string) ([]Entry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, sender_pubkey, envelope, enqueued_at
		   FROM offline_queue
		  WHERE recipient_pubkey = ?
		  ORDER BY enqueued_at ASC, id ASC`,
		recipient)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		var e Entry
		var ts int64
		if err := rows.Scan(&e.ID, &e.Sender, &e.Envelope, &ts); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		e.EnqueuedAt = time.UnixMilli(ts)
		out = append(out, e)
	}
	return out, rows.Err()
}
```

- [ ] **Step 4: Run to verify pass**

```powershell
go test ./internal/offline/ -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server add internal/offline/
git -C C:\Users\Lambda\Documents\heartbeat-server commit -m "offline: LoadFor"
```

---

## Task 5: TDD — `Delete`

**Files:**
- Modify: `internal/offline/store_test.go`
- Modify: `internal/offline/store.go`

- [ ] **Step 1: Write failing test**

Append to `store_test.go`:

```go
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
```

- [ ] **Step 2: Run to confirm failure**

```powershell
go test ./internal/offline/ -run TestDelete -v
```

Expected: undefined.

- [ ] **Step 3: Implement `Delete`**

Add to `store.go`:

```go
// Delete removes the row with the given ID. No error if it doesn't exist.
func (s *Store) Delete(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM offline_queue WHERE id = ?`, id)
	return err
}
```

- [ ] **Step 4: Run to verify**

```powershell
go test ./internal/offline/ -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server add internal/offline/
git -C C:\Users\Lambda\Documents\heartbeat-server commit -m "offline: Delete"
```

---

## Task 6: TDD — cap eviction (the 501st insert evicts the 1st)

**Files:**
- Modify: `internal/offline/store_test.go`

The implementation already supports this from Task 3 — this task validates the behavior end-to-end.

- [ ] **Step 1: Write failing test**

Append to `store_test.go`:

```go
func TestEnqueueEvictsOldestAtCap(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	// Fill to cap with distinguishable payloads.
	for i := 0; i < MaxPerRecipient; i++ {
		payload := []byte{byte(i), byte(i >> 8)}
		if err := s.Enqueue(ctx, "alice", "bob", payload); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	if n, _ := s.Depth(ctx, "alice"); n != MaxPerRecipient {
		t.Fatalf("expected depth=%d at fill, got %d", MaxPerRecipient, n)
	}
	// Insert one more — should evict the first.
	if err := s.Enqueue(ctx, "alice", "bob", []byte("overflow")); err != nil {
		t.Fatalf("overflow Enqueue: %v", err)
	}
	if n, _ := s.Depth(ctx, "alice"); n != MaxPerRecipient {
		t.Fatalf("expected depth held at %d post-eviction, got %d", MaxPerRecipient, n)
	}
	entries, _ := s.LoadFor(ctx, "alice")
	// First remaining entry should be index 1 (index 0 was evicted).
	first := entries[0].Envelope
	if first[0] != 1 || first[1] != 0 {
		t.Fatalf("oldest entry not evicted; first remaining=%v", first)
	}
	// Newest entry should be "overflow".
	last := entries[len(entries)-1].Envelope
	if string(last) != "overflow" {
		t.Fatalf("newest entry missing; last=%q", string(last))
	}
}
```

- [ ] **Step 2: Run — should already pass given Task 3's implementation**

```powershell
go test ./internal/offline/ -run TestEnqueueEvictsOldestAtCap -v
```

Expected: PASS.

- [ ] **Step 3: Commit**

```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server add internal/offline/store_test.go
git -C C:\Users\Lambda\Documents\heartbeat-server commit -m "offline: test cap eviction end-to-end"
```

---

## Task 7: TDD — `Sweep` (age-based deletion)

**Files:**
- Modify: `internal/offline/store_test.go`
- Modify: `internal/offline/store.go`

- [ ] **Step 1: Write failing test**

Append to `store_test.go`:

```go
func TestSweepDeletesOldRows(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	// Insert two rows then back-date one of them so it's older than the cutoff.
	_ = s.Enqueue(ctx, "alice", "bob", []byte("old"))
	_ = s.Enqueue(ctx, "alice", "bob", []byte("new"))
	// Back-date the first row to 10 days ago.
	tenDaysAgo := time.Now().Add(-10 * 24 * time.Hour).UnixMilli()
	if _, err := s.db.Exec(
		`UPDATE offline_queue SET enqueued_at = ? WHERE envelope = ?`,
		tenDaysAgo, []byte("old"),
	); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	deleted, err := s.Sweep(ctx, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deleted, got %d", deleted)
	}
	if n, _ := s.Depth(ctx, "alice"); n != 1 {
		t.Fatalf("expected depth=1 after Sweep, got %d", n)
	}
	entries, _ := s.LoadFor(ctx, "alice")
	if string(entries[0].Envelope) != "new" {
		t.Fatalf("wrong row survived: %q", string(entries[0].Envelope))
	}
}

func TestSweepNoOpWhenAllFresh(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_ = s.Enqueue(ctx, "alice", "bob", []byte("env"))
	deleted, err := s.Sweep(ctx, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("expected 0 deleted, got %d", deleted)
	}
}
```

- [ ] **Step 2: Run to confirm failure**

```powershell
go test ./internal/offline/ -run TestSweep -v
```

Expected: undefined.

- [ ] **Step 3: Implement `Sweep`**

Add to `store.go`:

```go
// Sweep deletes all rows older than maxAge. Returns the number of rows
// deleted. Intended to be called periodically by RunSweeper.
func (s *Store) Sweep(ctx context.Context, maxAge time.Duration) (int, error) {
	cutoff := time.Now().Add(-maxAge).UnixMilli()
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM offline_queue WHERE enqueued_at < ?`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("sweep: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}
```

- [ ] **Step 4: Run to verify**

```powershell
go test ./internal/offline/ -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server add internal/offline/
git -C C:\Users\Lambda\Documents\heartbeat-server commit -m "offline: Sweep"
```

---

## Task 8: `RunSweeper` goroutine

**Files:**
- Create: `internal/offline/sweeper.go`
- Create: `internal/offline/sweeper_test.go`

- [ ] **Step 1: Write failing test**

```go
package offline

import (
	"context"
	"testing"
	"time"
)

func TestRunSweeperRunsOnTickAndExitsOnCtx(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_ = s.Enqueue(ctx, "alice", "bob", []byte("old"))
	tenDaysAgo := time.Now().Add(-10 * 24 * time.Hour).UnixMilli()
	if _, err := s.db.Exec(
		`UPDATE offline_queue SET enqueued_at = ? WHERE envelope = ?`,
		tenDaysAgo, []byte("old"),
	); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunSweeper(runCtx, s, 20*time.Millisecond, 7*24*time.Hour)
		close(done)
	}()

	// Give the sweeper at least one tick.
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		if n, _ := s.Depth(ctx, "alice"); n == 0 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("sweeper did not delete old row within deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("RunSweeper did not exit on ctx cancel")
	}
}
```

- [ ] **Step 2: Run to confirm failure**

```powershell
go test ./internal/offline/ -run TestRunSweeper -v
```

Expected: undefined.

- [ ] **Step 3: Implement `RunSweeper`**

Create `internal/offline/sweeper.go`:

```go
package offline

import (
	"context"
	"log"
	"time"
)

// RunSweeper periodically calls Sweep until ctx is cancelled. Logs each
// sweep that deleted any rows. Intended to be launched as `go RunSweeper(...)`
// from main.
func RunSweeper(ctx context.Context, s *Store, interval, maxAge time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := s.Sweep(ctx, maxAge)
			if err != nil {
				log.Printf("[offline] sweep_err: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("[offline] sweep_deleted=%d maxAge=%s", n, maxAge)
			}
		}
	}
}
```

- [ ] **Step 4: Run to verify**

```powershell
go test ./internal/offline/ -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server add internal/offline/
git -C C:\Users\Lambda\Documents\heartbeat-server commit -m "offline: RunSweeper goroutine"
```

---

## Task 9: TDD — wire enqueue into `signaling.Handlers`

**Files:**
- Create: `internal/signaling/handlers_test.go`
- Modify: `internal/signaling/handlers.go`

This task adds the `Offline *offline.Store` field on `Handlers`, the `enqueueOffline` helper, and wires it into the `DeliverTo` false branch of `handleFrame`.

- [ ] **Step 1: Write failing test**

Create `internal/signaling/handlers_test.go`:

```go
package signaling

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/sahitkogs/heartbeat-server/internal/offline"
)

// helper: build a "send" frame as the WS reader would parse it.
func sendFrame(t *testing.T, to string, env []byte) []byte {
	t.Helper()
	b64 := base64.StdEncoding.EncodeToString(env)
	return []byte(`{"type":"send","to":"` + to + `","envelope":"` + b64 + `"}`)
}

func TestHandleFrameEnqueuesWhenRecipientOffline(t *testing.T) {
	hub := NewHub()
	q, err := offline.Open("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("offline.Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	h := &Handlers{Hub: hub, Offline: q}

	sender := &fakeConn{}
	const fromPub = "aaaa"
	const toPub = "bbbb"

	// Note: handleFrame writes errors back via sess; using a fakeConn
	// captures those writes too, but we only assert on the queue here.
	sess := &fakeSession{conn: sender}
	h.handleFrame(context.Background(), sess, fromPub, sendFrame(t, toPub, []byte("payload")))

	n, _ := q.Depth(context.Background(), toPub)
	if n != 1 {
		t.Fatalf("expected 1 row queued for %s, got %d", toPub, n)
	}
	entries, _ := q.LoadFor(context.Background(), toPub)
	if string(entries[0].Envelope) != "payload" {
		t.Fatalf("envelope round-trip lost: %q", string(entries[0].Envelope))
	}
	if entries[0].Sender != fromPub {
		t.Fatalf("sender lost: %q", entries[0].Sender)
	}
}

func TestHandleFrameDoesNotEnqueueWhenRecipientOnline(t *testing.T) {
	hub := NewHub()
	q, err := offline.Open("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("offline.Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	// Recipient is online via the Hub.
	recipientConn := &fakeConn{}
	hub.Add("bbbb", recipientConn)

	h := &Handlers{Hub: hub, Offline: q}
	sess := &fakeSession{conn: &fakeConn{}}
	h.handleFrame(context.Background(), sess, "aaaa",
		sendFrame(t, "bbbb", []byte("live")))

	if n, _ := q.Depth(context.Background(), "bbbb"); n != 0 {
		t.Fatalf("expected 0 rows queued when recipient online, got %d", n)
	}
	if len(recipientConn.received) != 1 {
		t.Fatalf("expected recipient to receive 1 push, got %d", len(recipientConn.received))
	}
}

// fakeSession adapts a fakeConn to the wsSession-shaped contract used by
// handleFrame. We only need `write(ctx, []byte) error` for the error path.
type fakeSession struct {
	conn *fakeConn
}

func (f *fakeSession) write(_ context.Context, b []byte) error {
	f.conn.received = append(f.conn.received, b)
	return nil
}
```

NOTE: `handleFrame` currently takes `*wsSession` concretely. To make this test
work without refactoring the whole WS layer, we extract a tiny interface in
the implementation step.

- [ ] **Step 2: Run to confirm failure**

```powershell
go test ./internal/signaling/ -run TestHandleFrame -v
```

Expected: build error — `Handlers` has no `Offline` field, and `handleFrame` signature is mismatched.

- [ ] **Step 3: Refactor `handleFrame` to take an interface for the session**

Edit `internal/signaling/handlers.go`. Add an interface and change the
`handleFrame` parameter type:

```go
// frameWriter is the subset of wsSession needed by handleFrame for replies.
// Extracted so tests can drive handleFrame with a fake.
type frameWriter interface {
	write(ctx context.Context, b []byte) error
}

func (h *Handlers) handleFrame(ctx context.Context, sess frameWriter, fromPub string, data []byte) {
	// ... existing body unchanged ...
}
```

`*wsSession` already has the matching `write` method, so callers don't need to change.

- [ ] **Step 4: Add `Offline` field + `enqueueOffline` helper + wire into the false branch**

Edit the `Handlers` struct:

```go
type Handlers struct {
	Hub     *Hub
	Book    *phonebook.Store
	Sender  *wake.Sender
	Offline *offline.Store  // NEW — nil disables persistence (tests / local dev)
}
```

Update the constructor signature:

```go
// NewHandlers constructs Handlers around an existing Hub. Book + Sender +
// Offline are optional — pass nil to disable each.
func NewHandlers(h *Hub, book *phonebook.Store, sender *wake.Sender, off *offline.Store) *Handlers {
	return &Handlers{Hub: h, Book: book, Sender: sender, Offline: off}
}
```

Add the helper:

```go
// enqueueOffline persists a ciphertext envelope for later flush when the
// recipient's WS reconnects. Best-effort — failures log and are swallowed
// so a sick disk doesn't take down message routing.
func (h *Handlers) enqueueOffline(ctx context.Context, fromPub, toPub string, env []byte) {
	if h.Offline == nil {
		return
	}
	if err := h.Offline.Enqueue(ctx, toPub, fromPub, env); err != nil {
		log.Printf("[offline] enqueue_fail to=%s err=%v", shortPub(toPub), err)
		return
	}
	log.Printf("[offline] enqueued to=%s envBytes=%d", shortPub(toPub), len(env))
}
```

Add the one-line wire-in inside `handleFrame`'s `send` case (look for the
existing `if !h.Hub.DeliverTo(...)` block):

```go
if !h.Hub.DeliverTo(f.To, env, fromPub) {
    log.Printf("[ws] deliver_offline from=%s to=%s", shortPub(fromPub), shortPub(f.To))
    _ = sess.write(ctx, BuildErrorFrameForPeer("recipient_offline", f.To))
    h.wakeOfflineRecipient(ctx, fromPub, f.To, env)
    h.enqueueOffline(ctx, fromPub, f.To, env)   // NEW
}
```

- [ ] **Step 5: Update the existing call site in `cmd/heartbeat-server/main.go`**

Find `signaling.NewHandlers(hub, book, &sender)` and append `nil` for the new
`Offline` parameter (we wire a real Store in Task 13):

```go
sigHandlers := signaling.NewHandlers(hub, book, &sender, nil)
```

- [ ] **Step 6: Run tests**

```powershell
go test ./internal/signaling/ -v
```

Expected: all existing tests still pass; the two new `TestHandleFrame*` tests pass.

- [ ] **Step 7: Commit**

```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server add internal/signaling/ cmd/heartbeat-server/main.go
git -C C:\Users\Lambda\Documents\heartbeat-server commit -m "signaling: enqueue to offline store on deliver_offline"
```

---

## Task 10: TDD — `flushOffline` (push + delete per row)

**Files:**
- Modify: `internal/signaling/handlers.go`
- Modify: `internal/signaling/handlers_test.go`

- [ ] **Step 1: Write failing tests**

Append to `handlers_test.go`:

```go
func TestFlushOfflineDeliversAndDeletes(t *testing.T) {
	q, _ := offline.Open("file::memory:?cache=shared")
	t.Cleanup(func() { _ = q.Close() })
	ctx := context.Background()
	const pub = "cccc"
	_ = q.Enqueue(ctx, pub, "sender1", []byte("env-a"))
	_ = q.Enqueue(ctx, pub, "sender2", []byte("env-b"))

	h := &Handlers{Hub: NewHub(), Offline: q}
	recipient := &fakeConn{}
	sess := &wsSessionFakeOnPush{conn: recipient}
	h.flushOffline(ctx, sess, pub)

	if len(recipient.received) != 2 {
		t.Fatalf("expected 2 pushed, got %d", len(recipient.received))
	}
	if n, _ := q.Depth(ctx, pub); n != 0 {
		t.Fatalf("expected queue drained, depth=%d", n)
	}
}

func TestFlushOfflineStopsOnPushFailure(t *testing.T) {
	q, _ := offline.Open("file::memory:?cache=shared")
	t.Cleanup(func() { _ = q.Close() })
	ctx := context.Background()
	const pub = "dddd"
	_ = q.Enqueue(ctx, pub, "s", []byte("a"))
	_ = q.Enqueue(ctx, pub, "s", []byte("b"))
	_ = q.Enqueue(ctx, pub, "s", []byte("c"))

	h := &Handlers{Hub: NewHub(), Offline: q}
	// Recipient fails after the first push.
	recipient := &fakeConn{failAfter: 1}
	sess := &wsSessionFakeOnPush{conn: recipient}
	h.flushOffline(ctx, sess, pub)

	if len(recipient.received) != 1 {
		t.Fatalf("expected 1 push before failure, got %d", len(recipient.received))
	}
	// The two un-pushed rows should remain.
	if n, _ := q.Depth(ctx, pub); n != 2 {
		t.Fatalf("expected 2 rows still queued after abandoned flush, got %d", n)
	}
}

// wsSessionFakeOnPush implements the same `Push(envelope, from)` shape that
// flushOffline needs (we'll narrow the signature in the impl step). It
// proxies to a fakeConn so we can observe deliveries + simulate failures.
type wsSessionFakeOnPush struct{ conn *fakeConn }

func (s *wsSessionFakeOnPush) Push(env []byte, from string) error {
	return s.conn.Push(env, from)
}
```

Extend `fakeConn` in the same file (or in a shared place — the existing one
in `hub_test.go` doesn't support `failAfter`). Add this small extension at
the top of `handlers_test.go`:

```go
// We can't extend the unexported fakeConn from hub_test.go cleanly — copy
// the minimal shape we need here. (Test files are independently compiled
// within the same package, but redefining a name would conflict, so use a
// distinct name `fakeConn` here with a failAfter field; remove the type
// from hub_test.go in a later refactor if duplication bites.)
```

**IMPORTANT:** since `fakeConn` already exists in `hub_test.go` and test
files in the same package share scope, redefining it will conflict. **Add
the `failAfter` field to the existing `fakeConn` in `hub_test.go` instead**:

```go
type fakeConn struct {
	received   [][]byte
	closed     bool
	failAfter  int  // if > 0, Push returns an error after N successful pushes
}

func (f *fakeConn) Push(env []byte, from string) error {
	if f.failAfter > 0 && len(f.received) >= f.failAfter {
		return errors.New("fakeConn: failAfter reached")
	}
	f.received = append(f.received, env)
	return nil
}
```

Don't forget `import "errors"` at the top of `hub_test.go`.

- [ ] **Step 2: Run to confirm failure**

```powershell
go test ./internal/signaling/ -run TestFlushOffline -v
```

Expected: undefined — `Handlers.flushOffline` doesn't exist yet.

- [ ] **Step 3: Implement `flushOffline`**

Add to `internal/signaling/handlers.go`:

```go
// pusher is the subset of wsSession needed by flushOffline.
type pusher interface {
	Push(envelope []byte, from string) error
}

// flushOffline drains the recipient's offline queue into the freshly-
// connected session, deleting each row after a successful push. Aborts
// on the first push failure; un-pushed rows remain queued for the next
// connect (the next Hub.Add for this pubkey triggers another flush).
func (h *Handlers) flushOffline(ctx context.Context, sess pusher, pubHex string) {
	if h.Offline == nil {
		return
	}
	entries, err := h.Offline.LoadFor(ctx, pubHex)
	if err != nil {
		log.Printf("[offline] load_fail pub=%s err=%v", shortPub(pubHex), err)
		return
	}
	if len(entries) == 0 {
		return
	}
	log.Printf("[offline] flush_start pub=%s count=%d", shortPub(pubHex), len(entries))
	for _, e := range entries {
		if err := sess.Push(e.Envelope, e.Sender); err != nil {
			log.Printf("[offline] flush_abandon pub=%s err=%v", shortPub(pubHex), err)
			return
		}
		if err := h.Offline.Delete(ctx, e.ID); err != nil {
			log.Printf("[offline] delete_fail id=%d err=%v", e.ID, err)
			// Push already succeeded — keep going. Recipient dedup at the
			// inner-envelope layer will absorb the duplicate on next flush.
		}
	}
	log.Printf("[offline] flush_done pub=%s", shortPub(pubHex))
}
```

- [ ] **Step 4: Run tests**

```powershell
go test ./internal/signaling/ -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server add internal/signaling/
git -C C:\Users\Lambda\Documents\heartbeat-server commit -m "signaling: flushOffline helper with delete-after-push"
```

---

## Task 11: Wire `flushOffline` into `Signal` (WS upgrade handler)

**Files:**
- Modify: `internal/signaling/handlers.go`

The flush should fire on every successful WS connect, in its own goroutine
so it doesn't block the read loop.

- [ ] **Step 1: Edit `Signal`**

Find the existing block (around `handlers.go:71`):

```go
sess := &wsSession{conn: conn}
h.Hub.Add(pubHex, sess)
log.Printf("[ws] connect pub=%s", shortPub(pubHex))
defer func() {
    h.Hub.Remove(pubHex, sess)
    log.Printf("[ws] disconnect pub=%s", shortPub(pubHex))
}()
```

Add the flush kick right after `Hub.Add`:

```go
sess := &wsSession{conn: conn}
h.Hub.Add(pubHex, sess)
log.Printf("[ws] connect pub=%s", shortPub(pubHex))
go h.flushOffline(r.Context(), sess, pubHex)   // NEW
defer func() {
    h.Hub.Remove(pubHex, sess)
    log.Printf("[ws] disconnect pub=%s", shortPub(pubHex))
}()
```

Use `r.Context()` (the HTTP request context) so the flush is cancelled if
the WS upgrade aborts; the goroutine's pushes will then fail fast and the
remaining rows stay queued for the next connect.

- [ ] **Step 2: Build**

```powershell
cd C:\Users\Lambda\Documents\heartbeat-server; go build ./...
```

Expected: clean build.

- [ ] **Step 3: Run the full test suite**

```powershell
go test ./...
```

Expected: all PASS. The new flush is exercised by the unit tests in Task 10;
the integration through `Signal` is exercised by the manual E2E in Task 16.

- [ ] **Step 4: Commit**

```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server add internal/signaling/handlers.go
git -C C:\Users\Lambda\Documents\heartbeat-server commit -m "signaling: kick flushOffline on WS connect"
```

---

## Task 12: `offline_queue_total` in `/healthz`

**Files:**
- Modify: `internal/health/handlers.go`
- Modify: `cmd/heartbeat-server/main.go`

- [ ] **Step 1: Add a global-depth helper to the offline package**

Append to `internal/offline/store.go`:

```go
// TotalDepth returns the total number of queued rows across all recipients.
// Used by /healthz for a coarse operational signal.
func (s *Store) TotalDepth(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM offline_queue`).Scan(&n)
	return n, err
}
```

- [ ] **Step 2: Test it**

Append to `internal/offline/store_test.go`:

```go
func TestTotalDepth(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_ = s.Enqueue(ctx, "alice", "x", []byte("1"))
	_ = s.Enqueue(ctx, "bob", "x", []byte("2"))
	_ = s.Enqueue(ctx, "bob", "x", []byte("3"))
	n, err := s.TotalDepth(ctx)
	if err != nil {
		t.Fatalf("TotalDepth: %v", err)
	}
	if n != 3 {
		t.Fatalf("got %d, want 3", n)
	}
}
```

```powershell
go test ./internal/offline/ -run TestTotalDepth -v
```

Expected: PASS.

- [ ] **Step 3: Extend `health.Handler` to include queue total**

Rewrite `internal/health/handlers.go`:

```go
// Package health exposes a minimal liveness endpoint.
package health

import (
	"encoding/json"
	"net/http"

	"github.com/sahitkogs/heartbeat-server/internal/offline"
)

// Handler returns 200 with version info + offline queue total (if a queue
// is provided). Pass nil for `q` to skip the queue field.
func Handler(version string, q *offline.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		body := map[string]any{
			"ok":      true,
			"version": version,
		}
		if q != nil {
			if n, err := q.TotalDepth(r.Context()); err == nil {
				body["offline_queue_total"] = n
			}
		}
		_ = json.NewEncoder(w).Encode(body)
	}
}
```

- [ ] **Step 4: Update the `health.Handler` call site**

In `cmd/heartbeat-server/main.go`, the existing line is:

```go
mux.HandleFunc("/healthz", health.Handler(version))
```

Change to (we wire the actual store in Task 13; nil for now keeps the
build green):

```go
mux.HandleFunc("/healthz", health.Handler(version, nil))
```

- [ ] **Step 5: Build + test**

```powershell
go build ./...; go test ./...
```

Expected: clean build, all tests pass.

- [ ] **Step 6: Commit**

```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server add internal/offline/ internal/health/ cmd/heartbeat-server/main.go
git -C C:\Users\Lambda\Documents\heartbeat-server commit -m "health: expose offline_queue_total via /healthz"
```

---

## Task 13: Wire `offline.Store` + sweeper into `main.go`

**Files:**
- Modify: `cmd/heartbeat-server/main.go`

- [ ] **Step 1: Add `-offline-db` flag and instantiate the store**

Inside `main()`, near the existing `-db` flag:

```go
addr := flag.String("addr", ":8080", "listen address")
dbPath := flag.String("db", "/var/lib/heartbeat/phonebook.db", "phonebook SQLite path")
offDBPath := flag.String("offline-db", "/var/lib/heartbeat/offline-queue.db", "offline-queue SQLite path") // NEW
fcmCreds := flag.String("fcm-creds", os.Getenv("HB_FCM_CREDENTIALS"), "Firebase service-account JSON path")
dryFCM := flag.Bool("fcm-disabled", false, "if true, skip Firebase init and use a stub FCM client (refuses real sends)")
flag.Parse()
```

After the `phonebook.Open` block, add:

```go
offQ, err := offline.Open(*offDBPath)
if err != nil {
    log.Fatalf("open offline queue: %v", err)
}
defer offQ.Close()

// Sweep rows older than 7 days every hour.
go offline.RunSweeper(ctx, offQ, 1*time.Hour, 7*24*time.Hour)
```

Add the import:

```go
import (
    // ... existing imports ...
    "github.com/sahitkogs/heartbeat-server/internal/offline"
)
```

- [ ] **Step 2: Pass the store into handlers + health**

```go
mux.HandleFunc("/healthz", health.Handler(version, offQ))     // changed
// ... existing wiring ...
sigHandlers := signaling.NewHandlers(hub, book, &sender, offQ) // changed
```

- [ ] **Step 3: Bump version**

```go
const version = "0.2.0-offline-queue"
```

- [ ] **Step 4: Build + test**

```powershell
cd C:\Users\Lambda\Documents\heartbeat-server; go build ./...; go test ./...
```

Expected: clean build, all tests pass.

- [ ] **Step 5: Commit**

```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server add cmd/heartbeat-server/main.go
git -C C:\Users\Lambda\Documents\heartbeat-server commit -m "main: wire offline.Store + sweeper, bump to 0.2.0-offline-queue"
```

---

## Task 14: End-to-end test via the existing smoketest harness

**Files:**
- Modify: `cmd/hb-smoketest/main.go` (small extension)

The repo already has an `hb-smoketest` binary. We extend it with one new
case that exercises the round trip: connect A, disconnect A, B sends to A,
reconnect A, expect A receives the queued envelope.

- [ ] **Step 1: Inspect what smoketest currently does**

```powershell
cd C:\Users\Lambda\Documents\heartbeat-server
```

Use Read on `cmd/hb-smoketest/main.go`. Identify the existing flow for
opening a WS, sending a frame, and receiving one.

- [ ] **Step 2: Add an offline-queue scenario**

Append a function `runOfflineQueueScenario(ctx context.Context, relayURL string)` that:

1. Generates two ephemeral keypairs A and B.
2. Opens A's WS, registers, closes A.
3. Opens B's WS, sends a frame addressed to A's pubkey with envelope `[]byte("hello-while-offline")`.
4. Receives the `recipient_offline` error frame on B (existing behavior).
5. Closes B.
6. Re-opens A's WS.
7. Waits up to 3 seconds for a `deliver` frame with the matching envelope.
8. Asserts the envelope bytes match and the `from` field is B.
9. Asserts that subsequent opens of A do NOT re-deliver (queue drained).

Use the existing key-generation, signing, and frame-builder helpers in the
same file. Mirror the existing scenario style (named function, hard-fails
on mismatch with a descriptive `log.Fatalf`).

- [ ] **Step 3: Wire it into the smoketest CLI**

If the smoketest accepts a `-scenario` flag, add `"offline-queue"` as a
recognized value. If it runs all scenarios sequentially, just call it from
`main`.

- [ ] **Step 4: Run locally**

Start the server in another window:
```powershell
go run ./cmd/heartbeat-server -fcm-disabled -db ./.tmp/phonebook.db -offline-db ./.tmp/offline.db -addr :8081
```

Run the smoketest against it:
```powershell
go run ./cmd/hb-smoketest -relay ws://localhost:8081/v1/signal -scenario offline-queue
```

Expected: smoketest exits 0 with a "PASS offline-queue" log line.

- [ ] **Step 5: Commit**

```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server add cmd/hb-smoketest/main.go
git -C C:\Users\Lambda\Documents\heartbeat-server commit -m "smoketest: offline-queue round-trip scenario"
```

---

## Task 15: Manual verification against staging

This is a smoke test against a real Docker build with persistence, mirroring
production as closely as possible. Skip if you don't have Docker locally.

- [ ] **Step 1: Build the image**

```powershell
cd C:\Users\Lambda\Documents\heartbeat-server
docker build -f deploy/Dockerfile -t heartbeat-server:offline-queue .
```

Expected: image built.

- [ ] **Step 2: Run with a mounted volume**

```powershell
docker run --rm -d --name hb-staging `
  -p 8082:8080 `
  -v hb-staging-data:/var/lib/heartbeat `
  heartbeat-server:offline-queue `
  -fcm-disabled `
  -db /var/lib/heartbeat/phonebook.db `
  -offline-db /var/lib/heartbeat/offline-queue.db
```

- [ ] **Step 3: Hit `/healthz` and confirm the new field**

```powershell
curl http://localhost:8082/healthz
```

Expected:
```json
{"ok":true,"version":"0.2.0-offline-queue","offline_queue_total":0}
```

- [ ] **Step 4: Run the smoketest against the container**

```powershell
go run ./cmd/hb-smoketest -relay ws://localhost:8082/v1/signal -scenario offline-queue
```

Expected: PASS.

- [ ] **Step 5: Restart the container and verify persistence**

```powershell
docker restart hb-staging
```

Open A's WS, send msg from B while A offline, **stop the container before
A reconnects**, restart it, then connect A:

```powershell
docker stop hb-staging; docker start hb-staging
```

Connect A → expect the queued message arrives. This validates Docker-volume
persistence end-to-end.

- [ ] **Step 6: Clean up**

```powershell
docker stop hb-staging
docker volume rm hb-staging-data
```

---

## Task 16: Tag release

- [ ] **Step 1: Run the full test suite one more time**

```powershell
cd C:\Users\Lambda\Documents\heartbeat-server; go test ./...
```

Expected: all PASS.

- [ ] **Step 2: Tag**

```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server tag -a v0.2.0-offline-queue -m "Phase 1: server-side offline queue (delete-on-push, 7-day retention)"
```

- [ ] **Step 3: Push (when ready to deploy)**

```powershell
git -C C:\Users\Lambda\Documents\heartbeat-server push origin master --tags
```

- [ ] **Step 4: Deploy**

Use the existing deploy path documented for heartbeat-server. After deploy,
verify the live `/healthz`:

```powershell
curl http://34.42.231.29:8080/healthz
```

Expected: `"version":"0.2.0-offline-queue"` and `"offline_queue_total":0`
(assuming no current backlog).

- [ ] **Step 5: Smoke test the live relay**

Take one of the test devices offline, send from the other, reconnect, verify
the message arrives. Also check `docker logs heartbeat-relay 2>&1 | grep '\[offline\]'`
on the server for the expected log lines (`enqueued`, `flush_start`, `flush_done`).

---

## Plan Self-Review

**Spec coverage:**

| Spec section | Implemented in |
|---|---|
| §6a new package | Tasks 2, 3, 4, 5, 7, 8 |
| §6a schema | Task 2 |
| §6a public API | Tasks 3–8, 12 (TotalDepth) |
| §6b wire-in (Handlers field) | Task 9 |
| §6b wire-in (handleFrame DeliverTo false) | Task 9 |
| §6b wire-in (Signal flush kick) | Task 11 |
| §6c flushOffline (delete-after-push, abandon on push fail) | Task 10 |
| §6d caps (500 / 7 days / 64 KiB) | Tasks 3 (cap), 6 (eviction test), 7 (sweep), 2 (size constants) |
| §6e main.go wiring | Task 13 |
| §6f Docker / persistence (existing volume) | Note in File Structure — no compose changes needed |
| §6g `/healthz` + structured logs | Tasks 12 (healthz), 9/10/11 (logs) |
| §6h test surface | Tasks 3–8 (unit), 9–10 (integration), 14 (E2E smoketest) |
| §6i migration / rollout | `CREATE TABLE IF NOT EXISTS` in Task 2; no client-version gate needed |

**Adjustments from spec:**

- DB path is `/var/lib/heartbeat/offline-queue.db` (in the existing
  `hb-data` volume), not `/data/heartbeat-relay.db`. Avoids a second volume.
- `offline.Store` (concrete struct mirroring `phonebook.Store`), not the
  `offline.Queue` interface from the spec. Same shape; tests use the real
  store against in-memory SQLite, mirroring the phonebook test pattern.

**Placeholder scan:** No TBDs. Each step contains the code or command needed.

**Type consistency:** `Store.Enqueue(ctx, recipient, sender, env)`,
`Store.LoadFor`, `Store.Delete`, `Store.Sweep`, `Store.Depth`, `Store.TotalDepth`
— used identically everywhere. `Entry` struct shape (`ID`, `Sender`,
`Envelope`, `EnqueuedAt`) is referenced consistently in Tasks 4, 10.
