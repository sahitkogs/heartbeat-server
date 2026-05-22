---
name: heartbeat-server-deploy
description: How to deploy heartbeat-server to its production GCP e2-micro VM — the fast binary-only path, the canonical docker-compose path, and recovery from a wedged build.
metadata:
  tags: heartbeat, deploy, gcp, docker, go
---

## When to use

When you've changed `heartbeat-server` Go code and need to deploy a new version to the live relay at `34.42.231.29:8080`. Or when the live relay is misbehaving and you need to inspect / restart it. Or when you've already wedged the VM with a heavy Go build and need to recover.

## Production target

- **VM:** `heartbeat-relay` in zone `us-central1-a`, project `heartbeat-app-prod`, e2-micro free tier
- **Public IP:** `34.42.231.29` (port 8080 HTTP, no TLS until a domain is registered)
- **Container name:** `heartbeat-relay` (Docker, `--restart unless-stopped`)
- **Image:** locally-built `heartbeat-server:dev` on the VM (no registry push)
- **Mounts:**
  - `/var/lib/heartbeat` → `/var/lib/heartbeat` (phonebook SQLite)
  - `/etc/heartbeat/fcm.json` → `/run/secrets/fcm.json` (read-only, Firebase service account)
- **Health endpoint:** `curl http://34.42.231.29:8080/healthz` → `{"ok":true,"version":"..."}`
- **SSH:** `gcloud compute ssh heartbeat-relay --project=heartbeat-app-prod --zone=us-central1-a`
- **Verify deploy:** the `version` field in `/healthz` mirrors `const version` in `cmd/heartbeat-server/main.go`. Bump it on every deploy so you can tell from a `curl` whether your change is live.

## ⚠️ Don't `docker build` on the VM

The e2-micro has 1 GB RAM and shared vCPU. A `go build` inside `golang:alpine` OOM-thrashes the host and wedges everything — SSH stops responding, HTTP times out, only `gcloud compute instances reset` brings it back. **Always cross-compile locally and ship the binary.** The path below sidesteps the build on the VM entirely.

## Fast path — scp a prebuilt binary into a thin distroless image

Use this for every routine deploy. It takes ~30s after the binary is built. All commands assume the repo at `C:/Users/Lambda/Documents/heartbeat-server` — adjust the `cd` paths if you're working from a different checkout.

```bash
# 1. Edit code. Bump the version string in cmd/heartbeat-server/main.go.
#    Re-run go test ./... locally.

# 2. Cross-compile for Linux amd64 (NOT on the VM — on your dev box):
cd C:/Users/Lambda/Documents/heartbeat-server
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  "C:/Program Files/Go/bin/go.exe" build \
  -o heartbeat-server-linux-amd64 ./cmd/heartbeat-server

# 3. scp the binary to the VM:
gcloud compute scp \
  C:/Users/Lambda/Documents/heartbeat-server/heartbeat-server-linux-amd64 \
  heartbeat-relay:/tmp/heartbeat-server-new \
  --project=heartbeat-app-prod --zone=us-central1-a

# 4. On the VM (one-shot via gcloud ssh --command): copy into the thin-image
#    build dir, rebuild image (no Go compile — just COPY into distroless),
#    retag, swap container.
gcloud compute ssh heartbeat-relay --project=heartbeat-app-prod \
  --zone=us-central1-a --command='set -e
mkdir -p /tmp/hb-thin
cp /tmp/heartbeat-server-new /tmp/hb-thin/heartbeat-server
chmod +x /tmp/hb-thin/heartbeat-server
cat > /tmp/hb-thin/Dockerfile <<EOF
FROM gcr.io/distroless/static:nonroot
COPY heartbeat-server /heartbeat-server
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/heartbeat-server"]
EOF
cd /tmp/hb-thin
sudo docker build -t heartbeat-server:new .
sudo docker tag heartbeat-server:new heartbeat-server:dev
sudo docker stop heartbeat-relay
sudo docker rm heartbeat-relay
sudo docker run -d --name heartbeat-relay --restart unless-stopped \
  -p 8080:8080 \
  -v /var/lib/heartbeat:/var/lib/heartbeat \
  -v /etc/heartbeat/fcm.json:/run/secrets/fcm.json:ro \
  heartbeat-server:dev \
  -addr :8080 -db /var/lib/heartbeat/phonebook.db -fcm-creds /run/secrets/fcm.json
sleep 3
sudo docker ps --format "{{.Names}} {{.Status}}"'

# 5. Verify the live version matches what you bumped:
curl -s http://34.42.231.29:8080/healthz
# expect: {"ok":true,"version":"<your new version>"}
```

The downtime between the `docker stop` and the new container being healthy is ~5–10s. Clients automatically reconnect their WebSockets (the v3 Flutter client retries on disconnect).

## Canonical path — full docker-compose rebuild

Documented in `docs/DEPLOY.md` for Pi / Hetzner / Oracle deploys. On the GCP e2-micro this requires a git clone + Go build on-host, which OOMs the host (see warning above). **Don't use this path on the e2-micro.** Prefer the fast path. Only use compose if you've upgraded the VM tier.

## Verifying the deploy

```bash
# Health endpoint shows the version string:
curl -s http://34.42.231.29:8080/healthz | jq

# Live tail server logs (connect/disconnect/ping_fail/deliver_offline since 0.1.4):
gcloud compute ssh heartbeat-relay --project=heartbeat-app-prod \
  --zone=us-central1-a --command='sudo docker logs -f --tail 30 heartbeat-relay'

# Full smoke test (alice → wake → bob) using the smoketest CLI:
cd C:/Users/Lambda/Documents/heartbeat-server
./hb-smoketest.exe register --server http://34.42.231.29:8080 --key alice.key
./hb-smoketest.exe register --server http://34.42.231.29:8080 --key bob.key
./hb-smoketest.exe wake --server http://34.42.231.29:8080 --key alice.key --to <BOB_PUBKEY>
```

## Recovery — VM wedged (CPU pegged, SSH timing out)

If you accidentally ran a heavy build on the VM and SSH hangs / `curl /healthz` times out:

```bash
# Hard reset. Container has --restart unless-stopped so it auto-comes up
# on the OLD image. You'll lose all live WS connections (clients reconnect
# within seconds).
gcloud compute instances reset heartbeat-relay \
  --project=heartbeat-app-prod --zone=us-central1-a

# Wait for the VM to come back, ~60s. Then verify:
until curl -s --max-time 5 http://34.42.231.29:8080/healthz | grep -q version; do
  sleep 8
done
curl -s http://34.42.231.29:8080/healthz
```

Then deploy your new version via the **fast path** above. Never re-run the on-VM `docker build` from the canonical `deploy/Dockerfile` — that's what wedged it.

## Rolling back

`heartbeat-server:dev` is the live tag. Previous versions are usually retained as `heartbeat-server:new` (the just-built tag) or `heartbeat-server:old` (if you remembered to tag before swap). To check what's available:

```bash
gcloud compute ssh heartbeat-relay --project=heartbeat-app-prod \
  --zone=us-central1-a --command='sudo docker images heartbeat-server'
```

To roll back to a previous image tag:

```bash
gcloud compute ssh heartbeat-relay --project=heartbeat-app-prod \
  --zone=us-central1-a --command='sudo docker tag heartbeat-server:<previous> heartbeat-server:dev
sudo docker stop heartbeat-relay && sudo docker rm heartbeat-relay
sudo docker run -d --name heartbeat-relay --restart unless-stopped \
  -p 8080:8080 \
  -v /var/lib/heartbeat:/var/lib/heartbeat \
  -v /etc/heartbeat/fcm.json:/run/secrets/fcm.json:ro \
  heartbeat-server:dev \
  -addr :8080 -db /var/lib/heartbeat/phonebook.db -fcm-creds /run/secrets/fcm.json'
```

For a permanent rollback, also revert the source commit and rebuild — otherwise the next deploy will re-introduce the broken change.

## Known gotchas

- **gcloud SSH and Claude's Bash tool:** long-running gcloud commands sometimes appear to be auto-backgrounded by the tool and the output file stays empty. Use a single-line `--command='set -e; …; …; …'` with all steps chained, so the command exits when the work is done and output lands at the end.
- **Force-stop ≠ killed (Android quirk worth knowing during testing):** `adb shell am force-stop` puts the app into a "stopped" state where Android suppresses implicit-broadcast delivery (including FCM data messages). Naturally-killed apps (OOM, swipe-from-recents) receive FCM fine. So F7-style "offline wake" tests using `am force-stop` may show server-side wake fires but client-side BG handler never runs — that's the OS, not your code.
- **The e2-micro is shared CPU.** Heavy operations (`docker build`, `docker images --no-trunc`) starve the HTTP server and clients see timeouts. Keep operations on the VM minimal.
- **No TLS yet.** Until a domain is registered, traffic is HTTP/WS plaintext. The Caddy auto-TLS bits in `deploy/docker-compose.yml` are unused on this VM.

## Reference

Source repo: `https://github.com/sahitkogs/heartbeat-server`.

Most recent known-good production version (as of 2026-05-22): `0.1.4-phase10.4.1-bug6` (commit `d3a3731`).
