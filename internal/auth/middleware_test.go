package auth

import (
	"bytes"
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sahitkogs/heartbeat-server/internal/keys"
)

func signedRequest(t *testing.T, kp *keys.Keypair, method, target string, body []byte, ts time.Time) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	tsStr := ts.UTC().Format(time.RFC3339)
	toSign := append([]byte(tsStr+"\n"), body...)
	sig := Sign(kp.Private, toSign)
	req.Header.Set("X-Heartbeat-Pubkey", kp.PublicHex())
	req.Header.Set("X-Heartbeat-Sig", hex.EncodeToString(sig))
	req.Header.Set("X-Heartbeat-Timestamp", tsStr)
	return req
}

func TestMiddlewareAcceptsValidSignature(t *testing.T) {
	kp, _ := keys.Generate()
	body := []byte(`{"hello":"world"}`)
	req := signedRequest(t, kp, http.MethodPost, "/foo", body, time.Now())

	called := false
	handler := RequireSignature(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		pub := ClientPubkeyFromContext(r.Context())
		if pub == nil {
			t.Fatal("expected client pubkey in context")
		}
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if !called {
		t.Fatal("inner handler not called")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestMiddlewareRejectsBadSignature(t *testing.T) {
	kp, _ := keys.Generate()
	body := []byte(`{"hello":"world"}`)
	req := signedRequest(t, kp, http.MethodPost, "/foo", body, time.Now())
	req.Header.Set("X-Heartbeat-Sig", hex.EncodeToString(make([]byte, 64)))

	handler := RequireSignature(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("inner handler should not be called")
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestMiddlewareRejectsStaleTimestamp(t *testing.T) {
	kp, _ := keys.Generate()
	body := []byte(`{"hello":"world"}`)
	req := signedRequest(t, kp, http.MethodPost, "/foo", body, time.Now().Add(-10*time.Minute))

	handler := RequireSignature(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("inner handler should not be called")
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestClientPubkeyFromContextNilWhenAbsent(t *testing.T) {
	if ClientPubkeyFromContext(context.Background()) != nil {
		t.Fatal("expected nil when no pubkey in context")
	}
}
