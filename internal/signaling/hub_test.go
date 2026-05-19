package signaling

import "testing"

func TestHubTracksConnections(t *testing.T) {
	h := NewHub()
	conn := &fakeConn{}
	h.Add("aa11", conn)
	if !h.IsOnline("aa11") {
		t.Fatal("expected aa11 online")
	}
	h.Remove("aa11", conn)
	if h.IsOnline("aa11") {
		t.Fatal("expected aa11 offline after Remove")
	}
}

func TestHubDeliversToOnlineRecipient(t *testing.T) {
	h := NewHub()
	conn := &fakeConn{}
	h.Add("bob", conn)
	delivered := h.DeliverTo("bob", []byte("envelope"), "alice")
	if !delivered {
		t.Fatal("expected delivered=true")
	}
	if len(conn.received) != 1 || string(conn.received[0]) != "envelope" {
		t.Fatalf("unexpected received: %v", conn.received)
	}
}

func TestHubReportsUndeliverableWhenRecipientOffline(t *testing.T) {
	h := NewHub()
	if h.DeliverTo("nobody", []byte("envelope"), "alice") {
		t.Fatal("expected delivered=false when recipient offline")
	}
}

func TestHubRemoveIgnoresStaleConnection(t *testing.T) {
	h := NewHub()
	oldConn := &fakeConn{}
	newConn := &fakeConn{}
	h.Add("aa11", oldConn)
	h.Add("aa11", newConn) // Add closes oldConn and replaces map entry

	// Simulate the stale goroutine unwinding: it tries to Remove with the old conn ref.
	h.Remove("aa11", oldConn)

	if !h.IsOnline("aa11") {
		t.Fatal("expected aa11 STILL online after stale Remove (new conn should remain)")
	}
}

func TestHubAddClosesOldConnection(t *testing.T) {
	h := NewHub()
	oldConn := &fakeConn{}
	newConn := &fakeConn{}
	h.Add("aa11", oldConn)
	if oldConn.closed {
		t.Fatal("oldConn should not be closed yet")
	}
	h.Add("aa11", newConn)
	if !oldConn.closed {
		t.Fatal("expected oldConn to be closed when replaced")
	}
}

type fakeConn struct {
	received [][]byte
	closed   bool
}

func (f *fakeConn) Push(env []byte, from string) error {
	f.received = append(f.received, env)
	return nil
}
func (f *fakeConn) Close() { f.closed = true }
