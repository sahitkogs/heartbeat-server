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
	ID         int64
	Sender     string // pubkey hex of the original sender
	Envelope   []byte // opaque ciphertext
	EnqueuedAt time.Time
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

// Enqueue appends an envelope for recipient. If the per-recipient cap is
// hit, the oldest row for that recipient is evicted before insert.
// ErrEnvelopeTooLarge if envelope exceeds MaxEnvelopeBytes.
func (s *Store) Enqueue(ctx context.Context, recipient, sender string, envelope []byte) error {
	if len(envelope) > MaxEnvelopeBytes {
		return ErrEnvelopeTooLarge
	}
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
				ORDER BY enqueued_at ASC, id ASC LIMIT 1)`,
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
