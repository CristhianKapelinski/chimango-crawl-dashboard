# chimango-crawl-dashboard

Real-time monitor for the **tag-backfill stage** of the ChimangoScan Docker Hub
crawl. The backfill worker fleet re-fetches every tag for repositories that
were originally crawled with the "recent-only" policy, diffs against Mongo,
processes the new tags, and stamps `tags_backfilled_at`. This dashboard
surfaces fleet health, queue depth, throughput, and per-worker error tails on
one page.

Live deployment: <https://crawl.anonshield.org>

## What you see

| Panel              | Source                                                                 |
| ------------------ | ---------------------------------------------------------------------- |
| KPIs + progress    | `repositories_data` counts: eligible / backfilled / in-flight / pending |
| Throughput chart   | `tags_backfilled_at` bucketed in 5–60 min windows over the last N h    |
| Worker grid        | Tail of `backfill_metrics.log` per host (rate, ETA, errors, last seen) |
| In-flight list     | Repos with `backfill_claimed=true`, newest first                       |
| Recently completed | Repos with `tags_backfilled_at`, newest first                          |
| Next in queue      | Highest-pull repos still pending — claim order is `pull_count DESC`    |
| Errors rollup      | Aggregated WARN/ERROR lines from each worker's last 300 lines          |

Empty, loading and error states are first-class — the page is readable even
when Mongo is briefly unreachable or worker logs are missing.

## Architecture

```
                  ┌─────────────────────┐
   workers ──HTTPS│ /var/lib/chimango-  │
   POST tail/min  │  crawl/worker_logs  │──rw mount──┐
   (~12 hosts)    └─────────────────────┘            │
                                                     ▼
   ┌────────────────────┐   SSH tunnel      ┌────────────────────┐
   │ coordinator mongo  │◄──────────────────│ tunnel  (autossh)  │
   │ (gpu2, port 27017) │                   │ exposes mongo:27017│
   └────────────────────┘                   └─────────┬──────────┘
                                                      │
                                            ┌─────────▼──────────┐
                                            │ backend (FastAPI)  │
                                            │ 127.0.0.1:8930     │
                                            │ TTL cache + ensure │
                                            │ partial indexes    │
                                            └─────────┬──────────┘
                                                      │
                                            ┌─────────▼──────────┐
                                            │ Caddy              │
                                            │ TLS + reverse proxy│
                                            │ /api/* + static    │
                                            └─────────┬──────────┘
                                                      │ HTTPS
                                                ▼
                                       crawl.anonshield.org
```

Read-only by construction: the backend never writes to Mongo.

## Repo layout

```
backend/             FastAPI app + Dockerfile + requirements
frontend/            static HTML/CSS/JS (no build step, no bundler)
docker-compose.yml   tunnel + backend
Caddyfile.snippet    crawl.anonshield.org reverse-proxy block
Makefile             build / deploy / rotate-key
docs/operations.md   day-2 ops: workers, debugging, key rotation
.env.example         all configuration knobs
```

## Quick start (on the dashboard host)

```bash
git clone https://github.com/CristhianKapelinski/chimango-crawl-dashboard.git
cd chimango-crawl-dashboard

cp .env.example .env
$EDITOR .env                                       # set MONGO_URI, API_KEY, TUNNEL_*

# Drop the SSH key the tunnel uses
sudo install -d -m 0700 /etc/ssh-tunnel
sudo install -m 0600 ~/.ssh/coordinator_relay /etc/ssh-tunnel/id_ed25519

# Append the reverse-proxy block to /etc/caddy/Caddyfile, then:
make install
```

`make install` builds the backend image, starts the tunnel + backend, syncs
the frontend to `/srv/crawl-dashboard`, validates the Caddyfile, and reloads
Caddy.

## Configuration

All knobs live in `.env` and are documented inline in [`.env.example`].
The most relevant ones:

| Variable          | Default                  | Purpose                                                                |
| ----------------- | ------------------------ | ---------------------------------------------------------------------- |
| `MONGO_URI`       | `mongodb://tunnel:27017` | Inside the compose network the autossh sidecar exposes this address    |
| `API_KEY`         | —                        | Shared secret; clients send `X-API-Key: …` (>= 32 bytes recommended)   |
| `PULL_THRESHOLD`  | `72`                     | Stage II floor; matches the value used by the crawler                  |
| `WORKER_HOSTS`    | `l01,…,rtx3060-01`       | Display order + allow-list for `POST /api/v1/metrics/{host}` uploads   |
| `STALL_SECONDS`   | `600`                    | A worker is "stalled" if its log has not been touched for this long    |
| `HISTORY_HOURS`   | `24`                     | Throughput chart window                                                |

`.env` is git-ignored; only `.env.example` is committed.

## Security

* TLS termination by Caddy via Let's Encrypt (HTTP-01 challenge).
* Static security headers (HSTS, X-Content-Type-Options, X-Frame-Options,
  Referrer-Policy, Permissions-Policy).
* Backend binds to `127.0.0.1` only; the only path to it is through Caddy.
* `X-API-Key` required on every `/api/*` request; `/healthz` is anonymous so
  Docker and Caddy can probe it.
* No credentials in the frontend bundle. The API key is held in
  `localStorage` per-user only after they enter it in the auth dialog.
* CORS off by default (frontend served from the same origin).
* `/etc/ssh-tunnel/id_ed25519` is mounted read-only into the tunnel container.

To rotate the API key: `make rotate-key` (prints the new key, rewrites `.env`,
restarts only the backend). Users will see a "401 / auth required" badge and
re-enter the key in the dialog.

## Operations

See [`docs/operations.md`](docs/operations.md) for:

* adding or removing a worker host
* deploying the per-minute push cron to the fleet
* debugging "stalled" vs "down" worker states
* rotating Mongo / API credentials safely
* what to do when Mongo is unreachable

## License

MIT. See [`LICENSE`](LICENSE).
