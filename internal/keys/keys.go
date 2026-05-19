// Package keys provides Ed25519 keypair generation and encoding helpers
// used for client identity in Heartbeat v3.
package keys

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// Keypair holds an Ed25519 public/private pair.
type Keypair struct {
	Public  ed25519.PublicKey
	Private ed25519.PrivateKey
}

// Generate creates a fresh Ed25519 keypair using crypto/rand.
func Generate() (*Keypair, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Keypair{Public: pub, Private: priv}, nil
}

// PublicHex returns the public key as a lowercase hex string (64 chars).
func (k *Keypair) PublicHex() string {
	return hex.EncodeToString(k.Public)
}

// DecodePublicHex decodes a hex-encoded Ed25519 public key (64 chars / 32 bytes).
func DecodePublicHex(s string) (ed25519.PublicKey, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("decode hex: %w", err)
	}
	if len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("expected %d bytes, got %d", ed25519.PublicKeySize, len(b))
	}
	return ed25519.PublicKey(b), nil
}
