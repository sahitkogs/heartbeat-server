package auth

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"io"
	"net/http"
	"time"

	"github.com/sahitkogs/heartbeat-server/internal/keys"
)

const (
	HeaderPubkey    = "X-Heartbeat-Pubkey"
	HeaderSignature = "X-Heartbeat-Sig"
	HeaderTimestamp = "X-Heartbeat-Timestamp"

	maxClockSkew = 5 * time.Minute
)

type ctxKey int

const pubkeyCtxKey ctxKey = 1

// ClientPubkeyFromContext returns the Ed25519 public key that signed the
// current request, or nil if the request is not authenticated.
func ClientPubkeyFromContext(ctx context.Context) ed25519.PublicKey {
	v, _ := ctx.Value(pubkeyCtxKey).(ed25519.PublicKey)
	return v
}

// RequireSignature is HTTP middleware that verifies Ed25519 signatures.
// Signed bytes are: timestamp + "\n" + body. Rejects stale timestamps to
// prevent indefinite replay.
func RequireSignature(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pubHex := r.Header.Get(HeaderPubkey)
		sigHex := r.Header.Get(HeaderSignature)
		tsStr := r.Header.Get(HeaderTimestamp)
		if pubHex == "" || sigHex == "" || tsStr == "" {
			http.Error(w, "missing auth headers", http.StatusUnauthorized)
			return
		}

		ts, err := time.Parse(time.RFC3339, tsStr)
		if err != nil {
			http.Error(w, "bad timestamp", http.StatusUnauthorized)
			return
		}
		if d := time.Since(ts); d > maxClockSkew || d < -maxClockSkew {
			http.Error(w, "stale timestamp", http.StatusUnauthorized)
			return
		}

		pub, err := keys.DecodePublicHex(pubHex)
		if err != nil {
			http.Error(w, "bad pubkey", http.StatusUnauthorized)
			return
		}
		sig, err := hex.DecodeString(sigHex)
		if err != nil {
			http.Error(w, "bad signature encoding", http.StatusUnauthorized)
			return
		}

		// Cap the body to a generous-but-bounded size. All v3 endpoints accept
		// small JSON; anything larger is malicious or a buggy client.
		const maxBody = 64 * 1024
		r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))

		toVerify := append([]byte(tsStr+"\n"), body...)
		if !Verify(pub, toVerify, sig) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}

		ctx := context.WithValue(r.Context(), pubkeyCtxKey, pub)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
