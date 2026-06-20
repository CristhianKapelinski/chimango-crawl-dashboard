# Operations

Day-two playbook for `chimango-crawl-dashboard`.

## Adding a worker host

Workers stream a metric line per loop iteration into
`/var/log/chimango-crawl/backfill_metrics.log`. The dashboard sees a worker
only after that file is rsynced to the dashboard host. To add `lXX`:

1. On the worker, make sure the backfill container writes its metrics log to
   the standard path (the worker container is configured to dump it under
   `/var/log/chimango-crawl/`).

2. On the dashboard host (a9), add a pull-mode entry to the rsync cron. The
   cron pulls every minute:

   ```
   * * * * * /usr/bin/rsync -a --delete \
       worker@lXX:/var/log/chimango-crawl/ \
       /var/lib/chimango-crawl/worker_logs/lXX/ \
       >> /var/log/chimango-crawl/sync.log 2>&1
   ```

3. Add `lXX` to `WORKER_HOSTS` in `.env` (display order is preserved), then
   `make up` to restart the backend with the new list.

A worker that has *never* posted a metric line shows as `down`; a worker whose
log was last touched more than `STALL_SECONDS` seconds ago shows as `stalled`
(amber); anything fresher is `up` (green).

## Debugging stalled vs down

* **down** — no log file on disk. Either the rsync cron has not fired yet
  (give it 60 s), or SSH to the worker is broken, or the worker container is
  not writing the metrics file at all.
* **stalled** — log file exists but the mtime is older than `STALL_SECONDS`.
  Either the worker container is stuck on a slow tag fetch (look at the top
  error in the worker card), or rsync is failing to pull (check
  `/var/log/chimango-crawl/sync.log`).

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
