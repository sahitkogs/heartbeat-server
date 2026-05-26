package signaling

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/sahitkogs/heartbeat-server/internal/offline"
)

// sendFrame builds a "send" frame as the WS reader would receive it.
func sendFrame(t *testing.T, to string, env []byte) []byte {
	t.Helper()
	b64 := base64.StdEncoding.EncodeToString(env)
	return []byte(`{"type":"send","to":"` + to + `","envelope":"` + b64 + `"}`)
}

// fakeSession captures write() calls so handleFrame's error-reply path can
// be exercised without a real WebSocket. It satisfies the frameWriter
// interface that handleFrame takes.
type fakeSession struct {
	written [][]byte
}

func (f *fakeSession) write(_ context.Context, b []byte) error {
	f.written = append(f.written, b)
	return nil
}

func TestHandleFrameEnqueuesWhenRecipientOffline(t *testing.T) {
	hub := NewHub()
	q, err := offline.Open("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("offline.Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	h := &Handlers{Hub: hub, Offline: q}

	const fromPub = "aaaa"
	const toPub = "bbbb"

	sess := &fakeSession{}
	h.handleFrame(context.Background(), sess, fromPub, sendFrame(t, toPub, []byte("payload")))

	ctx := context.Background()
	n, _ := q.Depth(ctx, toPub)
	if n != 1 {
		t.Fatalf("expected 1 row queued for %s, got %d", toPub, n)
	}
	entries, _ := q.LoadFor(ctx, toPub)
	if string(entries[0].Envelope) != "payload" {
		t.Fatalf("envelope round-trip lost: %q", string(entries[0].Envelope))
	}
	if entries[0].Sender != fromPub {
		t.Fatalf("sender lost: %q", entries[0].Sender)
	}
}

// pushSession adapts a fakeConn to the pusher interface flushOffline takes.
type pushSession struct{ conn *fakeConn }

func (p *pushSession) Push(env []byte, from string) error { return p.conn.Push(env, from) }

func TestFlushOfflineDeliversAndDeletes(t *testing.T) {
	q, err := offline.Open("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("offline.Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	ctx := context.Background()
	const pub = "cccc"
	_ = q.Enqueue(ctx, pub, "sender1", []byte("env-a"))
	_ = q.Enqueue(ctx, pub, "sender2", []byte("env-b"))

	h := &Handlers{Hub: NewHub(), Offline: q}
	recipient := &fakeConn{}
	sess := &pushSession{conn: recipient}
	h.flushOffline(ctx, sess, pub)

	if len(recipient.received) != 2 {
		t.Fatalf("expected 2 pushed, got %d", len(recipient.received))
	}
	if n, _ := q.Depth(ctx, pub); n != 0 {
		t.Fatalf("expected queue drained, depth=%d", n)
	}
}

func TestFlushOfflineStopsOnPushFailure(t *testing.T) {
	q, err := offline.Open("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("offline.Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	ctx := context.Background()
	const pub = "dddd"
	_ = q.Enqueue(ctx, pub, "s", []byte("a"))
	_ = q.Enqueue(ctx, pub, "s", []byte("b"))
	_ = q.Enqueue(ctx, pub, "s", []byte("c"))

	h := &Handlers{Hub: NewHub(), Offline: q}
	recipient := &fakeConn{failAfter: 1}
	sess := &pushSession{conn: recipient}
	h.flushOffline(ctx, sess, pub)

	if len(recipient.received) != 1 {
		t.Fatalf("expected 1 push before failure, got %d", len(recipient.received))
	}
	if n, _ := q.Depth(ctx, pub); n != 2 {
		t.Fatalf("expected 2 rows still queued after abandoned flush, got %d", n)
	}
}

func TestHandleFrameDoesNotEnqueueWhenRecipientOnline(t *testing.T) {
	hub := NewHub()
	q, err := offline.Open("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("offline.Open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	recipientConn := &fakeConn{}
	hub.Add("bbbb", recipientConn)

	h := &Handlers{Hub: hub, Offline: q}
	sess := &fakeSession{}
	h.handleFrame(context.Background(), sess, "aaaa",
		sendFrame(t, "bbbb", []byte("live")))

	if n, _ := q.Depth(context.Background(), "bbbb"); n != 0 {
		t.Fatalf("expected 0 rows queued when recipient online, got %d", n)
	}
	if len(recipientConn.received) != 1 {
		t.Fatalf("expected recipient to receive 1 push, got %d", len(recipientConn.received))
	}
}
