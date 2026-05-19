package signaling

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"net/http"
	"sync"
	"time"

	"nhooyr.io/websocket"

	"github.com/sahitkogs/heartbeat-server/internal/auth"
	"github.com/sahitkogs/heartbeat-server/internal/keys"
)

// Handlers serves the /v1/signal WebSocket endpoint.
type Handlers struct {
	Hub *Hub
}

// NewHandlers constructs Handlers around an existing Hub.
func NewHandlers(h *Hub) *Handlers {
	return &Handlers{Hub: h}
}

// Signal upgrades to WebSocket and runs the session loop.
// Authentication: the upgrade HTTP request carries Heartbeat headers with a
// signature over "WS-CONNECT:<timestamp>".
func (h *Handlers) Signal(w http.ResponseWriter, r *http.Request) {
	pub, err := authenticateWSUpgrade(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	pubHex := hex.EncodeToString(pub)

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusInternalError, "session ended")

	sess := &wsSession{conn: conn}
	h.Hub.Add(pubHex, sess)
	defer h.Hub.Remove(pubHex, sess)

	ctx := r.Context()
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		h.handleFrame(ctx, sess, pubHex, data)
	}
}

func (h *Handlers) handleFrame(ctx context.Context, sess *wsSession, fromPub string, data []byte) {
	f, err := ParseClientFrame(data)
	if err != nil {
		_ = sess.write(ctx, BuildErrorFrame("bad_frame", err.Error()))
		return
	}
	switch f.Type {
	case "ping":
		_ = sess.write(ctx, []byte(`{"type":"pong"}`))
	case "is_online":
		_ = sess.write(ctx, BuildOnlineStatusFrame(f.Pubkey, h.Hub.IsOnline(f.Pubkey)))
	case "send":
		env, decErr := decodeBase64(f.Envelope)
		if decErr != nil {
			_ = sess.write(ctx, BuildErrorFrame("bad_envelope", decErr.Error()))
			return
		}
		if !h.Hub.DeliverTo(f.To, env, fromPub) {
			_ = sess.write(ctx, BuildErrorFrame("recipient_offline", f.To))
		}
	default:
		_ = sess.write(ctx, BuildErrorFrame("unknown_type", f.Type))
	}
}

func authenticateWSUpgrade(r *http.Request) (ed25519.PublicKey, error) {
	pubHex := r.Header.Get(auth.HeaderPubkey)
	sigHex := r.Header.Get(auth.HeaderSignature)
	tsStr := r.Header.Get(auth.HeaderTimestamp)
	if pubHex == "" || sigHex == "" || tsStr == "" {
		return nil, errMissingAuth
	}
	ts, err := time.Parse(time.RFC3339, tsStr)
	if err != nil {
		return nil, errBadTimestamp
	}
	if d := time.Since(ts); d > 5*time.Minute || d < -5*time.Minute {
		return nil, errStaleTimestamp
	}
	pub, err := keys.DecodePublicHex(pubHex)
	if err != nil {
		return nil, errBadPubkey
	}
	sig, err := hex.DecodeString(sigHex)
	if err != nil {
		return nil, errBadSig
	}
	if !auth.Verify(pub, []byte("WS-CONNECT:"+tsStr), sig) {
		return nil, errBadSig
	}
	return pub, nil
}

// wsSession is the Hub Connection implementation backed by a real WebSocket.
// nhooyr.io/websocket does not allow concurrent writes; writeMu serializes
// all writes through this session (frame replies from the reader goroutine,
// pushes delivered from other sessions' goroutines).
type wsSession struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func (s *wsSession) Push(envelope []byte, from string) error {
	return s.write(context.Background(), BuildDeliverFrame(from, envelope))
}

func (s *wsSession) Close() {
	_ = s.conn.Close(websocket.StatusNormalClosure, "")
}

func (s *wsSession) write(ctx context.Context, b []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.conn.Write(ctx, websocket.MessageText, b)
}

// sentinel errors
var (
	errMissingAuth    = wsErr("missing auth headers")
	errBadTimestamp   = wsErr("bad timestamp")
	errStaleTimestamp = wsErr("stale timestamp")
	errBadPubkey      = wsErr("bad pubkey")
	errBadSig         = wsErr("invalid signature")
)

type wsErr string

func (e wsErr) Error() string { return string(e) }
