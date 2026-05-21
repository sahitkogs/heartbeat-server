# Heartbeat v3 — Server Wire Protocol

## Identity
- Each client owns an Ed25519 keypair.
- The 32-byte public key, hex-encoded (64 chars, lowercase), is the client's address.
- All HTTP requests are authenticated by Ed25519 signature over the request body.

## Common headers
- `X-Heartbeat-Pubkey`: hex(32-byte Ed25519 public key)
- `X-Heartbeat-Sig`: hex(Ed25519 signature over the raw request body bytes)
- `X-Heartbeat-Timestamp`: RFC3339 timestamp; rejected if more than 5 minutes from server time

## Endpoints

### `POST /v1/phonebook/register`
Register or update the FCM token for this pubkey.
Body (JSON):
```json
{ "fcm_token": "string", "platform": "android" }
```
Response: `200 OK` with `{ "ok": true }`.

### `DELETE /v1/phonebook/entry`
Delete this pubkey's entry.
Body (JSON): `{}`
Response: `200 OK`.

### `POST /v1/wake`
Ask the server to push-wake a recipient. Body (JSON):
```json
{
  "recipient_pubkey": "hex32",
  "opaque_payload": "base64",
  "dry_run": false
}
```
Server looks up FCM token by recipient pubkey, calls FCM with `opaque_payload`. Returns `200 OK` on FCM success, `404` if recipient not registered.

### `GET /v1/signal`
WebSocket upgrade. Initial HTTP request carries `X-Heartbeat-Pubkey`, `X-Heartbeat-Sig`, `X-Heartbeat-Timestamp` — signature is over the literal string `WS-CONNECT:<timestamp>`.

After upgrade, the connection speaks framed JSON. Frame types:

Client → Server:
```json
{ "type": "ping" }
{ "type": "is_online", "pubkey": "hex32" }
{ "type": "send", "to": "hex32", "envelope": "base64" }
```

Server → Client:
```json
{ "type": "pong" }
{ "type": "online_status", "pubkey": "hex32", "online": true }
{ "type": "deliver", "from": "hex32", "envelope": "base64" }
{ "type": "error", "code": "string", "message": "string", "to": "hex32 (optional)" }
```

The server stores **no envelopes**; if the recipient is offline at the moment of a `send`, the server replies with `error code=recipient_offline`, with the offline peer's pubkey in `to` (and mirrored in `message` for older clients). The client uses `to` to drive a `POST /v1/wake` fallback for the right peer.

### `GET /healthz`
No auth. Returns `200 OK` with `{ "ok": true, "version": "..." }`.

## Rate limits
v3 Phase 10.0: none. Add later if abuse appears.

## TLS
Caddy fronts all HTTP and WebSocket endpoints with automatic Let's Encrypt TLS.
