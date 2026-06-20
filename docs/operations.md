# Operations

Day-two playbook for `chimango-crawl-dashboard`.

## Adding a worker host

The flow is **push from the worker**, not pull from the dashboard. The
dashboard host (a9) cannot resolve the workers' private hostnames, but every
worker can reach the public dashboard URL over HTTPS. Each worker tails its
local `backfill_metrics.log` once a minute via cron and `POST`s the tail to
`/api/v1/metrics/{host}`. The backend authenticates with `X-API-Key`, checks
the host against the `WORKER_HOSTS` allow-list, and atomically writes the
content to `/var/lib/chimango-crawl/worker_logs/{host}/backfill_metrics.log`.

To add `lXX`:

1. **Dashboard host (a9)**: add `lXX` to `WORKER_HOSTS` in `.env`
   (comma-separated, display order is preserved) and restart only the
   backend:

   ```bash
   cd /opt/chimango-crawl-dashboard
   $EDITOR .env                       # edit WORKER_HOSTS
   docker compose -p chimango-crawl-dashboard restart backend
   ```

   Without this step the endpoint returns `403` and the worker upload is
   discarded.

2. **Worker (`lXX`)**: drop `scripts/push-metrics.sh` somewhere on `$PATH`
   (the convention is `/usr/local/bin/push-metrics.sh`) and wire a per-minute
   cron entry. The script reads the metrics file from
   `$HOME/DITector_research/backfill_metrics.log` by default — the path that
   the backfill container bind-mounts to.

   ```bash
   sudo install -m 0755 push-metrics.sh /usr/local/bin/push-metrics.sh
   sudo install -d -m 0700 /etc/chimango-crawl
   printf 'DASHBOARD_URL=https://crawl.anonshield.org\nAPI_KEY=<the key>\n' \
     | sudo tee /etc/chimango-crawl/push.env > /dev/null
   sudo chmod 0600 /etc/chimango-crawl/push.env

   # cron entry — runs as the same user that owns ~/DITector_research
   ( crontab -l 2>/dev/null;
     echo '* * * * * set -a; . /etc/chimango-crawl/push.env; set +a; /usr/local/bin/push-metrics.sh'
   ) | crontab -
   ```

   The fleet-wide one-liner from your workstation is in
   [Deploying the cron to every worker](#deploying-the-cron-to-every-worker)
   below.

3. **Verify** from the dashboard host:

   ```bash
   curl -fsS -H "X-API-Key: $API_KEY" \
     http://127.0.0.1:8930/api/v1/workers | jq '.workers[] | select(.host=="lXX")'
   ```

   The card should flip from `down` to `up` within ~60 s.

A worker that has *never* posted a metric line shows as `down`; a worker
whose log was last touched more than `STALL_SECONDS` seconds ago shows as
`stalled` (amber); anything fresher is `up` (green).

### Deploying the cron to every worker

Workers' hostnames are not resolvable from a9, but they are reachable from a
host inside the campus network (e.g. your workstation). From there, copy the
script and install the cron in one shot per host:

```bash
# Hosts that currently respond. Keep in sync with WORKER_HOSTS on a9.
WORKERS=(l01 l03 l04 l06 l07 l08 l09 l12 l13 rtx5080-01 rtx5080-02 rtx3060-01)
API_KEY='<paste from /opt/chimango-crawl-dashboard/.env on a9>'

for h in "${WORKERS[@]}"; do
  scp -q scripts/push-metrics.sh "$h":/tmp/push-metrics.sh
  ssh -n "$h" \
    "sudo install -m 0755 /tmp/push-metrics.sh /usr/local/bin/push-metrics.sh && \
     sudo install -d -m 0700 /etc/chimango-crawl && \
     printf 'DASHBOARD_URL=https://crawl.anonshield.org\nAPI_KEY=${API_KEY}\n' \
       | sudo tee /etc/chimango-crawl/push.env > /dev/null && \
     sudo chmod 0600 /etc/chimango-crawl/push.env && \
     ( crontab -l 2>/dev/null | grep -v 'push-metrics.sh' ;
       echo '* * * * * set -a; . /etc/chimango-crawl/push.env; set +a; /usr/local/bin/push-metrics.sh' ) \
       | crontab -" \
    && echo "ok $h" || echo "FAIL $h"
done
```

The `grep -v` line keeps the loop idempotent — re-running the deployment
does not duplicate the cron entry. The `API_KEY` lives only in
`/etc/chimango-crawl/push.env` (mode `0600`); it is never visible in
`crontab -l`.

## Debugging stalled vs down

* **down** — no log file on disk for that host. Either the worker cron has
  not fired yet (give it 60 s), or the worker cannot reach
  `https://crawl.anonshield.org` (test with `curl -I` from the worker), or
  the backfill container is not writing `~/DITector_research/backfill_metrics.log`.
* **stalled** — log file exists but the mtime is older than `STALL_SECONDS`.
  Either the worker container is stuck on a slow tag fetch (look at the top
  error in the worker card), or the worker's push cron is failing — check
  `/tmp/push-metrics.log` on the worker, which records every attempt.
* **403 in the backend log** — the worker is pushing under a hostname not
  in `WORKER_HOSTS`. Either fix `WORKER_HOST` on the worker side or add the
  hostname to `.env` on a9.

Useful quick checks on the dashboard host:

```bash
docker compose -p chimango-crawl-dashboard logs --tail=200 backend
ls -lt /var/lib/chimango-crawl/worker_logs/*/backfill_metrics.log
curl -fsS http://127.0.0.1:8930/healthz | jq .
```

If `/healthz` returns `"mongo": false` the autossh tunnel is down. Restart it
with `docker compose restart tunnel` and watch `docker compose logs tunnel`
for the failure reason (host-key change, key permission, network).

## Rotating the API key

```bash
make rotate-key
```

The script generates a 32-byte token, rewrites `API_KEY` in `.env`, and
restarts only the backend. The frontend will get `401`, show the auth dialog,
and accept the new key. The previous key is invalidated immediately — there
is no grace window.

## Rotating Mongo credentials

When the coordinator turns on Mongo auth, regenerate the dashboard user and
update `MONGO_URI` in `.env`:

```
MONGO_URI=mongodb://crawl_ro:<password>@tunnel:27017/dockerhub_data?authSource=admin&readPreference=secondary
```

The user only needs `read` on `dockerhub_data` and `clusterMonitor` on
`admin` (for the startup `ping`). Restart only the backend:

```bash
docker compose -p chimango-crawl-dashboard up -d --no-deps backend
```

## Recovering from a Mongo outage

The TTL cache returns the most recent successful response on transient
errors, so the dashboard stays readable for ~60 s after Mongo goes away.
Beyond that the health badge flips to `degraded`. Nothing needs to be
restarted — the backend reconnects on demand once Mongo is back.

## Reading the throughput chart

X axis is bucket start time (UTC, 5 min for short windows / 15 min for the
default 24 h / 1 h for longer). Y axis is repos with `tags_backfilled_at`
inside that bucket. The "aggregate rate" KPI is the average over the entire
window — it lags real time by up to one bucket.

## Index hygiene

`ensure_indexes()` on startup creates three partial indexes on
`repositories_data`. Without them the `count_documents` queries fall back to
full collection scans on ~12 M docs and the dashboard will time the
coordinator out:

```
dash_backfill_pending        pull_count desc, partial: pending + built
dash_backfill_inflight       backfill_started_at desc, partial: claimed
dash_backfill_done_recent    tags_backfilled_at desc, partial: backfilled
```

If the dashboard role does not have `createIndex` rights, create them
manually on the coordinator with the same definitions and the warning at
startup is harmless.
