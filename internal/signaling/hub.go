// Package signaling exchanges ephemeral WebSocket messages between online
// peers. Holds no message content beyond the moment of delivery.
package signaling

import "sync"

// Connection is the interface the hub needs from a WebSocket session.
type Connection interface {
	Push(envelope []byte, from string) error
	Close()
}

// Hub tracks online connections by pubkey-hex and relays send/deliver frames.
type Hub struct {
	mu    sync.RWMutex
	conns map[string]Connection
}

// NewHub constructs an empty Hub.
func NewHub() *Hub {
	return &Hub{conns: make(map[string]Connection)}
}

// Add registers a Connection for pubkey-hex. Replaces any existing connection.
func (h *Hub) Add(pubkey string, c Connection) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if old, ok := h.conns[pubkey]; ok {
		old.Close()
	}
	h.conns[pubkey] = c
}

// Remove unregisters c as the connection for pubkey-hex. It only deletes
// the map entry if it currently points at c — so a stale goroutine
// unwinding after being replaced does NOT remove its successor.
func (h *Hub) Remove(pubkey string, c Connection) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cur, ok := h.conns[pubkey]; ok && cur == c {
		delete(h.conns, pubkey)
	}
}

// IsOnline returns whether pubkey-hex has a registered connection.
func (h *Hub) IsOnline(pubkey string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.conns[pubkey]
	return ok
}

// DeliverTo pushes envelope to the recipient's connection if online.
// Returns true if delivered.
func (h *Hub) DeliverTo(recipientPubkey string, envelope []byte, fromPubkey string) bool {
	h.mu.RLock()
	c, ok := h.conns[recipientPubkey]
	h.mu.RUnlock()
	if !ok {
		return false
	}
	if err := c.Push(envelope, fromPubkey); err != nil {
		// On push error, treat connection as dead and remove (only if it's still us).
		h.Remove(recipientPubkey, c)
		return false
	}
	return true
}
