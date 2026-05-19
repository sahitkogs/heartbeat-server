package wake

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
	"github.com/sahitkogs/heartbeat-server/internal/phonebook"
)

type recordingFCM struct {
	called bool
}

func (r *recordingFCM) Send(ctx context.Context, token string, payload []byte, dryRun bool) error {
	r.called = true
	return nil
}

func signedRequest(t *testing.T, kp *keys.Keypair, body []byte) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/wake", bytes.NewReader(body))
	ts := time.Now().UTC().Format(time.RFC3339)
	sig := ed25519.Sign(kp.Private, append([]byte(ts+"\n"), body...))
	req.Header.Set(auth.HeaderPubkey, kp.PublicHex())
	req.Header.Set(auth.HeaderSignature, hex.EncodeToString(sig))
	req.Header.Set(auth.HeaderTimestamp, ts)
	return req
}

func TestWakeCallsFCMWhenRecipientRegistered(t *testing.T) {
	pb, _ := phonebook.Open("file::memory:?cache=shared")
	defer pb.Close()
	recip, _ := keys.Generate()
	_ = pb.Upsert(context.Background(), recip.PublicHex(), "fcm-tok", "android")

	fk := &recordingFCM{}
	h := NewHandlers(pb, Sender{FCM: fk})

	sender, _ := keys.Generate()
	body, _ := json.Marshal(WakeRequest{
		RecipientPubkey: recip.PublicHex(),
		OpaquePayload:   "aGVsbG8=", // "hello" base64
		DryRun:          true,
	})
	req := signedRequest(t, sender, body)
	rec := httptest.NewRecorder()
	auth.RequireSignature(http.HandlerFunc(h.Wake)).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !fk.called {
		t.Fatal("expected FCM to be called")
	}
}

func TestWakeReturns404WhenRecipientNotRegistered(t *testing.T) {
	pb, _ := phonebook.Open("file::memory:?cache=shared")
	defer pb.Close()
	fk := &recordingFCM{}
	h := NewHandlers(pb, Sender{FCM: fk})

	sender, _ := keys.Generate()
	recip, _ := keys.Generate()
	body, _ := json.Marshal(WakeRequest{
		RecipientPubkey: recip.PublicHex(),
		OpaquePayload:   "aGVsbG8=",
	})
	req := signedRequest(t, sender, body)
	rec := httptest.NewRecorder()
	auth.RequireSignature(http.HandlerFunc(h.Wake)).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
	if fk.called {
		t.Fatal("expected FCM NOT called")
	}
}
