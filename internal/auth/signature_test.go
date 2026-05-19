package auth

import (
	"testing"

	"github.com/sahitkogs/heartbeat-server/internal/keys"
)

func TestSignVerifyRoundTrip(t *testing.T) {
	kp, _ := keys.Generate()
	body := []byte(`{"hello":"world"}`)
	sig := Sign(kp.Private, body)
	if !Verify(kp.Public, body, sig) {
		t.Fatal("Verify failed on correctly signed body")
	}
}

func TestVerifyRejectsTamperedBody(t *testing.T) {
	kp, _ := keys.Generate()
	body := []byte(`{"hello":"world"}`)
	sig := Sign(kp.Private, body)
	tampered := []byte(`{"hello":"WORLD"}`)
	if Verify(kp.Public, tampered, sig) {
		t.Fatal("Verify accepted tampered body")
	}
}

func TestVerifyRejectsWrongKey(t *testing.T) {
	a, _ := keys.Generate()
	b, _ := keys.Generate()
	body := []byte(`{"hello":"world"}`)
	sig := Sign(a.Private, body)
	if Verify(b.Public, body, sig) {
		t.Fatal("Verify accepted signature from a different key")
	}
}
