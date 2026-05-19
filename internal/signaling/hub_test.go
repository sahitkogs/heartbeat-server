package signaling

import "testing"

func TestHubTracksConnections(t *testing.T) {
	h := NewHub()
	h.Add("aa11", &fakeConn{})
	if !h.IsOnline("aa11") {
		t.Fatal("expected aa11 online")
	}
	h.Remove("aa11")
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

type fakeConn struct {
	received [][]byte
	closed   bool
}

func (f *fakeConn) Push(env []byte, from string) error {
	f.received = append(f.received, env)
	return nil
}
func (f *fakeConn) Close() { f.closed = true }
