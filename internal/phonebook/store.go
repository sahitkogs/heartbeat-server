// Package phonebook persists (pubkey -> FCM token) entries in SQLite.
// Alongside the offline queue (see internal/offline), this is one of the
// two pieces of durable state the server holds.
package phonebook

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// ErrNotFound is returned by Lookup when no entry exists for the pubkey.
var ErrNotFound = errors.New("phonebook: not found")

// Entry is one row in the phonebook.
type Entry struct {
	PubkeyHex string
	FCMToken  string
	Platform  string
	UpdatedAt time.Time
}

// Store is the SQLite-backed phonebook.
type Store struct {
	db *sql.DB
}

// Open opens or creates a phonebook DB at the given DSN.
func Open(dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return &Store{db: db}, nil
}

const schema = `
CREATE TABLE IF NOT EXISTS phonebook (
	pubkey     TEXT PRIMARY KEY,
	fcm_token  TEXT NOT NULL,
	platform   TEXT NOT NULL,
	updated_at INTEGER NOT NULL
);`

// Close releases DB resources.
func (s *Store) Close() error {
	return s.db.Close()
}

// Upsert inserts or replaces the (pubkey -> token, platform) entry.
func (s *Store) Upsert(ctx context.Context, pubkeyHex, fcmToken, platform string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO phonebook (pubkey, fcm_token, platform, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(pubkey) DO UPDATE SET
			fcm_token=excluded.fcm_token,
			platform=excluded.platform,
			updated_at=excluded.updated_at`,
		pubkeyHex, fcmToken, platform, time.Now().Unix())
	return err
}

// Lookup returns the entry for pubkeyHex, or ErrNotFound.
func (s *Store) Lookup(ctx context.Context, pubkeyHex string) (*Entry, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT pubkey, fcm_token, platform, updated_at FROM phonebook WHERE pubkey = ?`,
		pubkeyHex)
	e := &Entry{}
	var ts int64
	if err := row.Scan(&e.PubkeyHex, &e.FCMToken, &e.Platform, &ts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	e.UpdatedAt = time.Unix(ts, 0)
	return e, nil
}

// Delete removes the entry for pubkeyHex. No error if it does not exist.
func (s *Store) Delete(ctx context.Context, pubkeyHex string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM phonebook WHERE pubkey = ?`, pubkeyHex)
	return err
}
