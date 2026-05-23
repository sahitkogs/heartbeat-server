package signaling

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"log"
	"net/http"
	"sync"
	"time"

	"nhooyr.io/websocket"

	"github.com/sahitkogs/heartbeat-server/internal/auth"
	"github.com/sahitkogs/heartbeat-server/internal/keys"
	"github.com/sahitkogs/heartbeat-server/internal/phonebook"
	"github.com/sahitkogs/heartbeat-server/internal/wake"
)

func shortPub(p string) string {
	if len(p) < 12 {
		return p
	}
	return p[:6] + ".." + p[len(p)-6:]
}

// Handlers serves the /v1/signal WebSocket endpoint.
//
// Book + Sender are optional dependencies for the server-side wake fallback:
// when DeliverTo fails because the recipient is offline, the handler will
// look up the recipient's FCM token and fire a wake push directly, instead
// of relying on the client to POST /v1/wake afterward. That client round-
// trip historically dropped two categories of sends — bundle pre-keys (not
// queued for wake on the client) and any send after the sender's own WS
// died (no recipient_offline error reaches a dead WS). Wiring Book + Sender
// here closes both gaps without requiring a client update.
//
// Pass nil for either to disable the server-side wake (useful for tests).
type Handlers struct {
	Hub    *Hub
	Book   *phonebook.Store
	Sender *wake.Sender
}

// NewHandlers constructs Handlers around an existing Hub. Book + Sender are
// optional — pass nil to skip the server-side wake fallback.
func NewHandlers(h *Hub, book *phonebook.Store, sender *wake.Sender) *Handlers {
	return &Handlers{Hub: h, Book: book, Sender: sender}
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
	log.Printf("[ws] connect pub=%s", shortPub(pubHex))
	defer func() {
		h.Hub.Remove(pubHex, sess)
		log.Printf("[ws] disconnect pub=%s", shortPub(pubHex))
	}()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Keepalive: ping every pingInterval; if a ping doesn't pong within
	// pingTimeout, close the conn so the reader unblocks and Hub.Remove fires.
	// Without this a recipient who lost wifi (or whose process was killed) can
	// linger in the hub until Linux TCP gives up (hours by default), and
	// senders never get recipient_offline → wake fallback never fires.
	go pingLoop(ctx, conn, pubHex)

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

// Application-level WS keepalive. Tuned aggressively (vs. the prior 15s/5s
// pair) so a force-stopped receiver is detected and removed from the hub
// within ~7s instead of ~20s. Outside that window, DeliverTo's write
// succeeds at the OS level even when the peer is dead (TCP RST hasn't
// fired yet), causing sends to land in a black hole until next ping.
// True elimination of the race requires application-level message acks;
// this is a pragmatic interim fix tracked as Phase 10.4.1 BUG.6.
const (
	pingInterval = 5 * time.Second
	pingTimeout  = 2 * time.Second
)

// pingLoop sends periodic application-level pings and closes the underlying
// connection if a pong doesn't arrive in time. Returns when ctx is cancelled
// (caller-side close) or on the first failed ping (peer-side disconnect).
func pingLoop(ctx context.Context, conn *websocket.Conn, pubHex string) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				log.Printf("[ws] ping_fail pub=%s err=%v — closing", shortPub(pubHex), err)
				_ = conn.Close(websocket.StatusPolicyViolation, "ping timeout")
				return
			}
		}
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
			log.Printf("[ws] deliver_offline from=%s to=%s", shortPub(fromPub), shortPub(f.To))
			_ = sess.write(ctx, BuildErrorFrameForPeer("recipient_offline", f.To))
			h.wakeOfflineRecipient(ctx, fromPub, f.To, env)
		}
	default:
		_ = sess.write(ctx, BuildErrorFrame("unknown_type", f.Type))
	}
}

// wakeOfflineRecipient fires an FCM push to wake the offline recipient. The
// FCM payload mirrors the client-side wake_client.dart format:
//
//	payload = senderPubkeyBytes(32) || envelopeBytes
//
// so the recipient's background isolate strips the first 32 bytes to know
// which libsignal session to decrypt with. Best-effort: any error is logged
// and swallowed — we already sent recipient_offline to the sender, the wake
// is an opportunistic improvement on top.
func (h *Handlers) wakeOfflineRecipient(ctx context.Context, fromPubHex, toPubHex string, env []byte) {
	if h.Book == nil || h.Sender == nil {
		return
	}
	fromPubBytes, err := hex.DecodeString(fromPubHex)
	if err != nil || len(fromPubBytes) != 32 {
		log.Printf("[wake] skip bad_fromPub from=%s err=%v", shortPub(fromPubHex), err)
		return
	}
	entry, err := h.Book.Lookup(ctx, toPubHex)
	if err != nil {
		if errors.Is(err, phonebook.ErrNotFound) {
			log.Printf("[wake] skip no_phonebook to=%s", shortPub(toPubHex))
			return
		}
		log.Printf("[wake] skip lookup_err to=%s err=%v", shortPub(toPubHex), err)
		return
	}
	payload := make([]byte, 0, 32+len(env))
	payload = append(payload, fromPubBytes...)
	payload = append(payload, env...)
	if err := h.Sender.Wake(ctx, entry.FCMToken, payload, false); err != nil {
		log.Printf("[wake] fcm_fail from=%s to=%s err=%v", shortPub(fromPubHex), shortPub(toPubHex), err)
		return
	}
	log.Printf("[wake] fcm_ok from=%s to=%s payloadBytes=%d", shortPub(fromPubHex), shortPub(toPubHex), len(payload))
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
