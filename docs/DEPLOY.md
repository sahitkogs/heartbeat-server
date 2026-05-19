# Heartbeat Server — Deployment

The same docker-compose stack runs on any Linux host with Docker installed. This doc covers three popular options.

## Prerequisites for every option

- A Firebase project with FCM enabled.
- A service-account JSON file from that project. Save it to the host as `deploy/fcm-service-account.json` (not committed).
- A domain name pointing to the host's public IP.
- Docker + Docker Compose v2 installed.

## Option A — Raspberry Pi 4 + Cloudflare Tunnel

1. Install Raspberry Pi OS Lite (64-bit). SSH in.
2. Install Docker: `curl -fsSL https://get.docker.com | sh`. Add user to docker group, log out and back in.
3. Install Cloudflare's `cloudflared` package; run `cloudflared tunnel login` and `cloudflared tunnel create heartbeat`.
4. Map the tunnel: `cloudflared tunnel route dns heartbeat relay.your-domain.com`.
5. Clone this repo, copy `deploy/.env.example` to `deploy/.env`, set `HB_HOST=relay.your-domain.com` and pick a strong TURN password.
6. Place the Firebase service-account JSON at `deploy/fcm-service-account.json`.
7. From the repo root: `cd deploy && docker compose up -d`.
8. Add a Cloudflare Tunnel ingress rule sending `https://relay.your-domain.com` to `http://localhost:8080` (or run `cloudflared tunnel run heartbeat` with a config file referencing the local Caddy).
9. Test: `curl https://relay.your-domain.com/healthz`.

## Option B — Hetzner CX22 (or equivalent $5 VPS)

1. Create a Linux VM (Ubuntu 24.04 LTS or Debian 12).
2. SSH in, install Docker (same one-liner as above).
3. Point an A record (`relay.your-domain.com`) at the VM's public IP.
4. Open ports 80, 443, 3478/udp, 5349/tcp in your provider's firewall.
5. Same steps 5–7 as Option A. Caddy auto-provisions TLS on first request.
6. Test: `curl https://relay.your-domain.com/healthz`.

## Option C — Oracle Always Free (ARM)

Identical to Option B except:
- Pick an Ampere A1 ARM instance.
- The Dockerfile builds for the host architecture automatically (no change required).
- Oracle's firewall is closed by default — open the ports in their networking dashboard AND in the host's iptables.

## Updating

```bash
git pull
cd deploy
docker compose pull
docker compose up -d --build
```

## Backups

The phonebook DB is at the `hb-data` Docker volume's `phonebook.db`. It's small (well under 1 MB even at thousands of users). Snapshot it daily:

```bash
docker run --rm -v deploy_hb-data:/data -v ${PWD}:/backup alpine \
    cp /data/phonebook.db /backup/phonebook-$(date +%F).db
```

The DB is rebuildable: every client re-registers automatically on next launch. Loss = brief delivery delay, no data loss.
