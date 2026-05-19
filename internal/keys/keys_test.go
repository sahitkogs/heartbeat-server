package keys

import (
	"bytes"
	"testing"
)

func TestGenerateKeypairProducesDistinctPair(t *testing.T) {
	a, err := Generate()
	if err != nil {
		t.Fatalf("Generate() error: %v", err)
	}
	b, err := Generate()
	if err != nil {
		t.Fatalf("Generate() error: %v", err)
	}
	if bytes.Equal(a.Public, b.Public) {
		t.Fatal("two Generate() calls returned identical public keys")
	}
}

func TestEncodePublicHex(t *testing.T) {
	kp, err := Generate()
	if err != nil {
		t.Fatalf("Generate() error: %v", err)
	}
	encoded := kp.PublicHex()
	if len(encoded) != 64 {
		t.Fatalf("expected 64-char hex (32 bytes), got %d", len(encoded))
	}
}

func TestDecodePublicHexRoundTrip(t *testing.T) {
	kp, _ := Generate()
	encoded := kp.PublicHex()
	decoded, err := DecodePublicHex(encoded)
	if err != nil {
		t.Fatalf("DecodePublicHex error: %v", err)
	}
	if !bytes.Equal(decoded, kp.Public) {
		t.Fatal("round-trip mismatch")
	}
}
