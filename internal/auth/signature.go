// Package auth provides Ed25519 request authentication for Heartbeat v3.
package auth

import "crypto/ed25519"

// Sign produces an Ed25519 signature over body using private key.
func Sign(priv ed25519.PrivateKey, body []byte) []byte {
	return ed25519.Sign(priv, body)
}

// Verify returns true iff sig is a valid Ed25519 signature of body by pub.
func Verify(pub ed25519.PublicKey, body, sig []byte) bool {
	if len(sig) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(pub, body, sig)
}
