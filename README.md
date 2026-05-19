# heartbeat-server

Minimal stub server for [Heartbeat](https://github.com/sahitkogs/heart-beat) v3.

**What it does**
- Pubkey-keyed FCM push wake-up bridge (`POST /v1/wake`)
- Pubkey-authenticated WebSocket signaling rendezvous (`GET /v1/signal`)
- STUN/TURN via bundled coturn for WebRTC NAT traversal

**What it explicitly does NOT do**
- Store any message content, ciphertext, or attachments
- Hold a queue of pending messages
- Persist any social-graph metadata beyond a (pubkey → FCM token) phonebook
- Issue identities, host directories, or perform identity verification

The server's worst-case data leak (if fully compromised) is the phonebook itself: a mapping from anonymous Ed25519 public keys to FCM tokens. No emails, no phone numbers, no message contents, no contact lists.

**Tech stack:** Go 1.22+, SQLite (pure-Go via modernc.org/sqlite), Ed25519, Firebase Admin SDK, Docker, Caddy, coturn.

## Quickstart (local dev)

```bash
go build ./cmd/heartbeat-server ./cmd/hb-smoketest
./heartbeat-server -addr :8080 -db ./dev.db -fcm-disabled &
./hb-smoketest register --server http://localhost:8080 --key ./alice.key
./hb-smoketest register --server http://localhost:8080 --key ./bob.key
./hb-smoketest wake --server http://localhost:8080 --key ./alice.key --to <BOB_PUBKEY>
```

## Deploy

See [`docs/DEPLOY.md`](docs/DEPLOY.md) for Raspberry Pi 4, Hetzner CX22, and Oracle Always Free walkthroughs.

## Protocol

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md).

## License

MIT. See [LICENSE](LICENSE).
