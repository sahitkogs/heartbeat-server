package phonebook

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sahitkogs/heartbeat-server/internal/auth"
	"github.com/sahitkogs/heartbeat-server/internal/keys"
)

func newAuthedRequest(t *testing.T, kp *keys.Keypair, method, target string, body []byte) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	ts := time.Now().UTC().Format(time.RFC3339)
	toSign := append([]byte(ts+"\n"), body...)
	sig := ed25519.Sign(kp.Private, toSign)
	req.Header.Set(auth.HeaderPubkey, kp.PublicHex())
	req.Header.Set(auth.HeaderSignature, hex.EncodeToString(sig))
	req.Header.Set(auth.HeaderTimestamp, ts)
	return req
}

func TestRegisterStoresEntry(t *testing.T) {
	s, _ := Open("file::memory:?cache=shared")
	defer s.Close()
	h := NewHandlers(s)

	kp, _ := keys.Generate()
	body, _ := json.Marshal(RegisterRequest{FCMToken: "tok-xyz", Platform: "android"})
	req := newAuthedRequest(t, kp, http.MethodPost, "/v1/phonebook/register", body)
	rec := httptest.NewRecorder()

	auth.RequireSignature(http.HandlerFunc(h.Register)).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", rec.Code, rec.Body.String())
	}
	got, err := s.Lookup(context.Background(), kp.PublicHex())
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if got.FCMToken != "tok-xyz" {
		t.Fatalf("expected stored token tok-xyz, got %q", got.FCMToken)
	}
}

func TestDeleteHandlerRemovesEntry(t *testing.T) {
	s, _ := Open("file::memory:?cache=shared")
	defer s.Close()
	h := NewHandlers(s)

	kp, _ := keys.Generate()
	_ = s.Upsert(context.Background(), kp.PublicHex(), "tok", "android")

	body := []byte(`{}`)
	req := newAuthedRequest(t, kp, http.MethodDelete, "/v1/phonebook/entry", body)
	rec := httptest.NewRecorder()
	auth.RequireSignature(http.HandlerFunc(h.Delete)).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if _, err := s.Lookup(context.Background(), kp.PublicHex()); err != ErrNotFound {
		t.Fatalf("expected entry deleted, got err=%v", err)
	}
}
