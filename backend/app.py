"""Crawl-backfill dashboard backend.

Read-only API over the ChimangoScan tag-backfill pipeline state. Surface area
is small on purpose: one polling client (the dashboard) talking to ~12 worker
hosts through Mongo. Every handler is cached so a refresh storm cannot
stampede the coordinator.

Auth: every /api/* request must carry X-API-Key matching API_KEY env. /healthz
is unauthenticated for liveness probes.

Mongo connection: MONGO_URI may point at an autossh sidecar that exposes the
coordinator's mongo as `tunnel:27017` inside the docker network.
"""
from __future__ import annotations

import logging
import os
import re
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from bson import ObjectId
from fastapi import Depends, FastAPI, Header, HTTPException, Path as PathParam, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from pymongo import MongoClient
from pymongo.errors import PyMongoError

LOG = logging.getLogger("crawl-backfill")
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

MONGO_URI = os.environ["MONGO_URI"]
MONGO_DB = os.environ.get("MONGO_DB", "dockerhub_data")
API_KEY = os.environ.get("API_KEY", "").strip()
PULL_THRESHOLD = int(os.environ.get("PULL_THRESHOLD", "72"))
CORS_ORIGINS = [o.strip() for o in os.environ.get("CORS_ORIGINS", "").split(",") if o.strip()]
WORKER_HOSTS = [h.strip() for h in os.environ.get("WORKER_HOSTS", "").split(",") if h.strip()]
WORKER_LOGS_DIR = os.environ.get("WORKER_LOGS_DIR", "/data/worker_logs")
STALL_SECONDS = int(os.environ.get("STALL_SECONDS", "600"))
HISTORY_HOURS = int(os.environ.get("HISTORY_HOURS", "24"))

# Upload knobs for POST /api/v1/metrics/{host}. Workers push the tail of their
# local backfill_metrics.log here every minute (cron). The dashboard host does
# not have inbound SSH to workers, so the flow is inverted: workers reach the
# public dashboard URL over HTTPS and push.
METRICS_MAX_BYTES = int(os.environ.get("METRICS_MAX_BYTES", str(2 * 1024 * 1024)))
METRICS_RATE_PER_MIN = int(os.environ.get("METRICS_RATE_PER_MIN", "12"))
WORKER_HOST_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,62}$")

TTL_FAST = 10.0
TTL_SLOW = 60.0
TTL_COUNT = 60.0
TTL_HEAVY = 600.0

# Buckets for the per-repo tag-count distribution. Edges are exclusive on the
# upper end; the final "1000+" bucket catches the long tail that motivates the
# whole backfill stage (recent-only crawl missed thousands of historical tags).
TAG_HIST_EDGES = (1, 10, 100, 1000)
TAG_HIST_LABELS = ("1-9", "10-99", "100-999", "1000+")

# Sampling cap for the tag distribution aggregate. The aggregate touches every
# tag row owned by repos backfilled in the window, so we bound the work to a
# recent slice rather than the whole corpus. 500 covers the noisiest 24h on the
# current fleet without breaking the maxTimeMS budget.
TAG_HIST_SAMPLE = int(os.environ.get("TAG_HIST_SAMPLE", "500"))
TAG_HIST_WINDOW_HOURS = int(os.environ.get("TAG_HIST_WINDOW_HOURS", "24"))

# A worker that has been "up" (log mtime fresh) but reporting rate=0 for longer
# than this is degraded — usually a stuck pull, OAuth refresh, or a dead Mongo
# session. Flagged distinctly from `stalled` (which means the log went silent).
WORKER_STUCK_SECONDS = int(os.environ.get("WORKER_STUCK_SECONDS", "300"))

# Substrings that mark a Docker Hub HTTP 429 / token-bucket rejection in the
# worker error tail. Kept conservative — the warning is a heads-up, not a page.
RATE_LIMIT_MARKERS = ("429", "toomanyrequests", "rate limit", "rate-limit")


# ---------- cache ----------------------------------------------------------

@dataclass
class _Entry:
    ts: float
    val: Any


class TTLCache:
    """Thread-safe TTL cache that returns stale values on transient errors so
    Mongo hiccups do not surface as 500s to the dashboard."""

    def __init__(self) -> None:
        self._store: dict[str, _Entry] = {}
        self._lock = threading.Lock()

    def get_or_set(self, key: str, ttl: float, fn: Callable[[], Any]) -> Any:
        now = time.time()
        with self._lock:
            hit = self._store.get(key)
            if hit and (now - hit.ts) < ttl:
                return hit.val
        try:
            val = fn()
            with self._lock:
                self._store[key] = _Entry(now, val)
            return val
        except Exception as exc:
            with self._lock:
                stale = self._store.get(key)
            if stale is not None:
                LOG.warning("cache %s stale fallback: %s", key, exc)
                return stale.val
            raise


CACHE = TTLCache()


# ---------- mongo ----------------------------------------------------------

_client_lock = threading.Lock()
_client: MongoClient | None = None


def mongo() -> MongoClient:
    global _client
    with _client_lock:
        if _client is None:
            _client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=15000,
                appname="chimango-crawl-dashboard",
            )
        return _client


def repos():
    return mongo()[MONGO_DB].repositories_data


def tags():
    return mongo()[MONGO_DB].tags_data


def ensure_indexes() -> None:
    """Idempotent. Without partial indexes on the claim filter the count
    queries are full collection scans on 12M docs and the dashboard will time
    the coordinator out. We create only what we read."""
    coll = repos()
    try:
        coll.create_index(
            [("pull_count", -1)],
            name="dash_backfill_pending",
            partialFilterExpression={
                "tags_backfilled_at": None,
                "graph_built_at": {"$exists": True, "$ne": None},
            },
            background=True,
        )
        coll.create_index(
            [("backfill_started_at", -1)],
            name="dash_backfill_inflight",
            partialFilterExpression={"backfill_claimed": True},
            background=True,
        )
        coll.create_index(
            [("tags_backfilled_at", -1)],
            name="dash_backfill_done_recent",
            partialFilterExpression={"tags_backfilled_at": {"$exists": True, "$ne": None}},
            background=True,
        )
        LOG.info("indexes ensured")
    except PyMongoError as exc:
        LOG.warning("index ensure failed (continuing): %s", exc)


# ---------- domain queries -------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def progress_snapshot() -> dict:
    coll = repos()
    base = {"pull_count": {"$gte": PULL_THRESHOLD}}
    eligible = coll.count_documents(
        {**base, "graph_built_at": {"$exists": True, "$ne": None}}, maxTimeMS=15000,
    )
    backfilled = coll.count_documents(
        {**base, "tags_backfilled_at": {"$exists": True, "$ne": None}}, maxTimeMS=15000,
    )
    in_flight = coll.count_documents({"backfill_claimed": True}, maxTimeMS=15000)
    pending = max(eligible - backfilled - in_flight, 0)
    return {
        "eligible": eligible,
        "backfilled": backfilled,
        "in_flight": in_flight,
        "pending": pending,
        "pct": (backfilled / eligible * 100) if eligible else 0.0,
        "threshold": PULL_THRESHOLD,
        "ts": _utcnow().isoformat(),
    }


def _bucketize(ts_iter, bucket_seconds: int, window_seconds: int) -> list[dict]:
    """Aggregate ISO timestamp strings into uniform buckets ending at now."""
    now = _utcnow().replace(microsecond=0)
    floor = now - timedelta(seconds=window_seconds)
    counts: dict[int, int] = defaultdict(int)
    for raw in ts_iter:
        dt = _parse_iso(raw) if isinstance(raw, str) else raw
        if not dt:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if dt < floor:
            continue
        epoch = int(dt.timestamp())
        slot = epoch - (epoch % bucket_seconds)
        counts[slot] += 1
    start_epoch = int(floor.timestamp())
    start_epoch -= start_epoch % bucket_seconds
    end_epoch = int(now.timestamp())
    end_epoch -= end_epoch % bucket_seconds
    buckets = []
    cursor = start_epoch
    while cursor <= end_epoch:
        buckets.append({
            "ts": datetime.fromtimestamp(cursor, tz=timezone.utc).isoformat(),
            "count": counts.get(cursor, 0),
        })
        cursor += bucket_seconds
    return buckets


def history(hours: int | None = None) -> dict:
    """Per-bucket backfilled count, last N hours. tags_backfilled_at is a
    string ISO timestamp set by the worker; we project just that field to
    keep this cheap."""
    hours = hours or HISTORY_HOURS
    window = hours * 3600
    bucket = 300 if hours <= 6 else 900 if hours <= 24 else 3600
    cutoff = (_utcnow() - timedelta(hours=hours)).isoformat()
    cursor = repos().find(
        {"tags_backfilled_at": {"$gte": cutoff}},
        {"tags_backfilled_at": 1, "_id": 0},
        max_time_ms=10000,
    )
    buckets = _bucketize((d.get("tags_backfilled_at") for d in cursor), bucket, window)
    total = sum(b["count"] for b in buckets)
    rate_per_min = (total / (window / 60.0)) if window else 0.0
    return {
        "hours": hours,
        "bucket_seconds": bucket,
        "buckets": buckets,
        "rate_per_min": round(rate_per_min, 2),
    }


def in_flight_repos(limit: int = 25) -> list[dict]:
    cursor = repos().find(
        {"backfill_claimed": True},
        {"namespace": 1, "name": 1, "pull_count": 1, "backfill_started_at": 1, "_id": 0},
        sort=[("backfill_started_at", -1)],
        limit=limit,
        max_time_ms=10000,
    )
    out = []
    for r in cursor:
        started = r.get("backfill_started_at")
        if isinstance(started, datetime):
            started_iso = (
                started.replace(tzinfo=timezone.utc).isoformat()
                if started.tzinfo is None else started.isoformat()
            )
        else:
            started_iso = started
        out.append({
            "namespace": r.get("namespace"),
            "name": r.get("name"),
            "pull_count": r.get("pull_count"),
            "started_at": started_iso,
        })
    return out


def recent_backfilled(limit: int = 25) -> list[dict]:
    cursor = repos().find(
        {"tags_backfilled_at": {"$exists": True, "$ne": None}},
        {"namespace": 1, "name": 1, "pull_count": 1, "tags_backfilled_at": 1, "_id": 0},
        sort=[("tags_backfilled_at", -1)],
        limit=limit,
        max_time_ms=10000,
    )
    return [
        {
            "namespace": r.get("namespace"),
            "name": r.get("name"),
            "pull_count": r.get("pull_count"),
            "backfilled_at": r.get("tags_backfilled_at"),
        }
        for r in cursor
    ]


def top_pending(limit: int = 25) -> list[dict]:
    cursor = repos().find(
        {
            "pull_count": {"$gte": PULL_THRESHOLD},
            "graph_built_at": {"$exists": True, "$ne": None},
            "tags_backfilled_at": None,
            "backfill_claimed": {"$ne": True},
        },
        {"namespace": 1, "name": 1, "pull_count": 1, "_id": 0},
        sort=[("pull_count", -1)],
        limit=limit,
        max_time_ms=15000,
    )
    return [
        {"namespace": r.get("namespace"), "name": r.get("name"), "pull_count": r.get("pull_count")}
        for r in cursor
    ]


def tags_collected(hours: int = 24) -> dict:
    """Approximate count of tag documents inserted in the last N hours.

    Uses the implicit timestamp inside the BSON ObjectId rather than scanning a
    date field, so it is O(log n) over the default `_id` index regardless of
    collection size. The count is approximate to within one second (the ObjectId
    timestamp granularity), which is plenty for a dashboard tile."""
    cutoff = _utcnow() - timedelta(hours=hours)
    oid = ObjectId.from_datetime(cutoff)
    coll = mongo()[MONGO_DB].tags_data
    count = coll.count_documents({"_id": {"$gte": oid}}, maxTimeMS=10000)
    return {"hours": hours, "count": count, "ts": _utcnow().isoformat()}


def backfilled_window(hours: int = 24) -> int:
    """Repos marked `tags_backfilled_at` within the window. Cheap thanks to the
    partial index `dash_backfill_done_recent` we already keep."""
    cutoff = (_utcnow() - timedelta(hours=hours)).isoformat()
    return repos().count_documents(
        {"tags_backfilled_at": {"$gte": cutoff}}, maxTimeMS=10000,
    )


def tag_distribution(sample: int = TAG_HIST_SAMPLE, hours: int = TAG_HIST_WINDOW_HOURS) -> dict:
    """Histogram of tag count per repo for the most recently backfilled slice.

    The aggregate is bounded twice: (a) only repos backfilled in the last `hours`
    contribute, (b) we keep at most `sample` of those. That keeps the pipeline
    O(sample * avg_tags_per_repo) instead of touching the full tags_data
    collection. The buckets are computed in Python after $group because Mongo's
    $bucket needs explicit edges and we want a single round trip."""
    cutoff = (_utcnow() - timedelta(hours=hours)).isoformat()
    sample_repos = list(repos().find(
        {"tags_backfilled_at": {"$gte": cutoff}},
        {"namespace": 1, "name": 1, "_id": 0},
        sort=[("tags_backfilled_at", -1)],
        limit=sample,
        max_time_ms=10000,
    ))
    if not sample_repos:
        return {
            "sample_repos": 0,
            "hours": hours,
            "buckets": [{"label": lbl, "count": 0} for lbl in TAG_HIST_LABELS],
            "ts": _utcnow().isoformat(),
        }

    pairs = [{"repositories_namespace": r["namespace"], "repositories_name": r["name"]}
             for r in sample_repos if r.get("namespace") and r.get("name")]
    if not pairs:
        return {"sample_repos": 0, "hours": hours,
                "buckets": [{"label": lbl, "count": 0} for lbl in TAG_HIST_LABELS],
                "ts": _utcnow().isoformat()}

    pipeline = [
        {"$match": {"$or": pairs}},
        {"$group": {
            "_id": {"ns": "$repositories_namespace", "n": "$repositories_name"},
            "tags": {"$sum": 1},
        }},
    ]
    counts = []
    try:
        for doc in mongo()[MONGO_DB].tags_data.aggregate(
            pipeline, allowDiskUse=True, maxTimeMS=20000,
        ):
            counts.append(int(doc.get("tags", 0)))
    except PyMongoError as exc:
        LOG.warning("tag distribution aggregate failed: %s", exc)
        return {"sample_repos": 0, "hours": hours,
                "buckets": [{"label": lbl, "count": 0} for lbl in TAG_HIST_LABELS],
                "error": "aggregate failed", "ts": _utcnow().isoformat()}

    buckets = [0] * len(TAG_HIST_LABELS)
    for c in counts:
        if c >= TAG_HIST_EDGES[3]:
            buckets[3] += 1
        elif c >= TAG_HIST_EDGES[2]:
            buckets[2] += 1
        elif c >= TAG_HIST_EDGES[1]:
            buckets[1] += 1
        elif c >= TAG_HIST_EDGES[0]:
            buckets[0] += 1
    return {
        "sample_repos": len(counts),
        "hours": hours,
        "buckets": [{"label": lbl, "count": n} for lbl, n in zip(TAG_HIST_LABELS, buckets)],
        "ts": _utcnow().isoformat(),
    }


# ---------- worker log parsing --------------------------------------------

# Backfill metrics log line emitted by the worker. Two formats are accepted:
#   old: taxa=2.4 repos/min
#   new: taxa=2.4 repos/min 845 tags/min 1320 imgs/min
# The tags/imgs tail is optional so a worker on an older binary still parses;
# the missing groups fall through as None and the dashboard renders '—'.
# Example new line:
# [BACKFILL METRICS 01:48:23] progresso=12/5600683 (0.0%) | taxa=1.2 repos/min 845 tags/min 1320 imgs/min | ETA=4dias | cache tags=2% imgs=8% | neo4j=18432 | erros=3 | uptime=12m45s
METRIC_RE = re.compile(
    r"\[(?P<label>BACKFILL|BUILD) METRICS (?P<hms>\d\d:\d\d:\d\d)\] "
    r"progresso=(?P<done>\d+)/(?P<total>\d+) \((?P<pct>[\d.]+)%\) \| "
    r"taxa=(?P<rate>[\d.]+) repos/min"
    r"(?:\s+(?P<tags_rate>[\d.]+)\s+tags/min\s+(?P<imgs_rate>[\d.]+)\s+imgs/min)? \| "
    r"ETA=(?P<eta>[^|]+?) \| "
    r"cache tags=(?P<tag_cache>[\d.]+)% imgs=(?P<img_cache>[\d.]+)% \| "
    r"neo4j=(?P<neo4j>\d+) \| erros=(?P<errors>\d+) \| uptime=(?P<uptime>[^\s]+)"
)

ERROR_RE = re.compile(r"\b(WARN|ERROR)\b\s*(?P<msg>.+)")


def _read_tail(path: Path, max_lines: int = 2000) -> list[str]:
    """Read the last max_lines of path without loading the whole file."""
    if not path.exists():
        return []
    try:
        with path.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            block = 64 * 1024
            data = b""
            while size > 0 and data.count(b"\n") <= max_lines:
                step = min(block, size)
                size -= step
                f.seek(size)
                data = f.read(step) + data
            return data.decode("utf-8", errors="replace").splitlines()[-max_lines:]
    except OSError as exc:
        LOG.warning("read tail %s: %s", path, exc)
        return []


def _parse_worker(host: str) -> dict:
    log_path = Path(WORKER_LOGS_DIR) / host / "backfill_metrics.log"
    lines = _read_tail(log_path)
    last_metric = None
    error_buckets: dict[str, int] = defaultdict(int)
    for line in reversed(lines):
        m = METRIC_RE.search(line)
        if m and last_metric is None:
            last_metric = m.groupdict()
            break
    for line in lines[-300:]:
        em = ERROR_RE.search(line)
        if em:
            msg = em.group("msg").strip()
            key = msg[:120]
            error_buckets[key] += 1

    mtime = log_path.stat().st_mtime if log_path.exists() else None
    age = (time.time() - mtime) if mtime else None
    status = "down"
    if last_metric and age is not None:
        status = "up" if age < STALL_SECONDS else "stalled"
    elif log_path.exists():
        status = "stalled"

    return {
        "host": host,
        "status": status,
        "last_seen_seconds": int(age) if age is not None else None,
        "metrics": last_metric and {
            "label": last_metric["label"],
            "processed": int(last_metric["done"]),
            "total": int(last_metric["total"]),
            "pct": float(last_metric["pct"]),
            "rate_per_min": float(last_metric["rate"]),
            "eta": last_metric["eta"].strip(),
            "tag_cache_pct": float(last_metric["tag_cache"]),
            "img_cache_pct": float(last_metric["img_cache"]),
            "neo4j_inserts": int(last_metric["neo4j"]),
            "errors": int(last_metric["errors"]),
            "uptime": last_metric["uptime"],
        },
        "top_errors": sorted(
            [{"msg": k, "count": v} for k, v in error_buckets.items() if v > 0],
            key=lambda x: x["count"], reverse=True,
        )[:5],
    }


def _parse_uptime_seconds(s: str | None) -> int | None:
    """Parse the worker uptime token (e.g. '5m0s', '1h12m', '2d3h') to seconds.
    Returns None if the token is missing or unrecognised — callers fall back to
    a conservative default so a malformed log line never demotes a live worker."""
    if not s:
        return None
    total = 0
    cur = 0
    seen = False
    for ch in s:
        if ch.isdigit():
            cur = cur * 10 + int(ch)
            seen = True
        elif ch == "d" and seen:
            total += cur * 86400; cur = 0; seen = False
        elif ch == "h" and seen:
            total += cur * 3600; cur = 0; seen = False
        elif ch == "m" and seen:
            total += cur * 60; cur = 0; seen = False
        elif ch == "s" and seen:
            total += cur; cur = 0; seen = False
        else:
            cur = 0; seen = False
    return total if total > 0 else None


def _is_stuck(snap: dict) -> bool:
    """A worker is 'stuck' when its log is fresh (status==up) but the rate has
    been flat at zero for longer than WORKER_STUCK_SECONDS. We approximate the
    "how long at zero" duration by min(uptime, WORKER_STUCK_SECONDS+1) — without
    a historical series we cannot tell exactly, but the heuristic only flags
    workers that have been alive long enough for the rate to have moved."""
    if snap.get("status") != "up":
        return False
    m = snap.get("metrics") or {}
    if (m.get("rate_per_min") or 0) > 0:
        return False
    if m.get("processed", 0) > 0:
        return True  # was producing, now at zero — almost certainly stuck
    up_s = _parse_uptime_seconds(m.get("uptime"))
    if up_s is None:
        return False
    return up_s >= WORKER_STUCK_SECONDS


def workers_snapshot() -> dict:
    if WORKER_HOSTS:
        hosts = WORKER_HOSTS
    elif Path(WORKER_LOGS_DIR).exists():
        hosts = sorted({d.name for d in Path(WORKER_LOGS_DIR).iterdir() if d.is_dir()})
    else:
        hosts = []
    snaps = [_parse_worker(h) for h in hosts]
    for s in snaps:
        s["stuck"] = _is_stuck(s)
    total_rate = sum((s["metrics"] or {}).get("rate_per_min", 0) for s in snaps if s["metrics"])
    return {
        "workers": snaps,
        "summary": {
            "configured": len(hosts),
            "up": sum(1 for s in snaps if s["status"] == "up"),
            "stalled": sum(1 for s in snaps if s["status"] == "stalled"),
            "down": sum(1 for s in snaps if s["status"] == "down"),
            "stuck": sum(1 for s in snaps if s.get("stuck")),
            "aggregate_rate_per_min": round(total_rate, 2),
        },
        "ts": _utcnow().isoformat(),
    }


def derived_metrics(workers: dict) -> dict:
    """Pure derivations on top of workers_snapshot — no Mongo round trips."""
    snaps = workers.get("workers") or []
    live = [s for s in snaps if s.get("status") == "up" and s.get("metrics")]

    # Cache hit rate: weight each worker by its observed rate so a quiet worker
    # with a perfect cache does not skew the headline number. Falls back to a
    # simple mean when nothing is moving.
    rate_sum = sum((s["metrics"].get("rate_per_min") or 0) for s in live)
    if rate_sum > 0:
        tag_hit = sum((s["metrics"].get("tag_cache_pct") or 0)
                      * (s["metrics"].get("rate_per_min") or 0) for s in live) / rate_sum
        img_hit = sum((s["metrics"].get("img_cache_pct") or 0)
                      * (s["metrics"].get("rate_per_min") or 0) for s in live) / rate_sum
    elif live:
        tag_hit = sum((s["metrics"].get("tag_cache_pct") or 0) for s in live) / len(live)
        img_hit = sum((s["metrics"].get("img_cache_pct") or 0) for s in live) / len(live)
    else:
        tag_hit = img_hit = 0.0

    # 429 / token-bucket alert: scan top errors only (already capped at 5 per
    # worker upstream). Cheap and avoids re-reading log tails.
    rate_limited_hosts: list[dict] = []
    total_429 = 0
    for s in snaps:
        host = s.get("host")
        for e in s.get("top_errors") or []:
            msg = (e.get("msg") or "").lower()
            if any(mk in msg for mk in RATE_LIMIT_MARKERS):
                total_429 += int(e.get("count", 0))
                rate_limited_hosts.append({
                    "host": host, "count": int(e.get("count", 0)), "msg": e.get("msg"),
                })
                break

    stuck_hosts = [s["host"] for s in snaps if s.get("stuck")]

    return {
        "cache_hit": {
            "tag_pct": round(tag_hit, 1),
            "img_pct": round(img_hit, 1),
            "weighted_by": "rate" if rate_sum > 0 else "uniform",
            "live_workers": len(live),
        },
        "rate_limit_alert": {
            "active": total_429 > 0,
            "total_recent": total_429,
            "hosts": rate_limited_hosts[:5],
        },
        "stuck_workers": {
            "count": len(stuck_hosts),
            "hosts": stuck_hosts,
        },
    }


def eta(progress: dict, workers: dict) -> dict:
    pending = progress.get("pending", 0)
    rate = workers.get("summary", {}).get("aggregate_rate_per_min", 0.0)
    if not rate or rate <= 0 or pending <= 0:
        return {"seconds": None, "human": "—", "rate_per_min": rate}
    seconds = (pending / rate) * 60
    return {"seconds": int(seconds), "human": _humanize(seconds), "rate_per_min": rate}


def _humanize(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    units = [("d", 86400), ("h", 3600), ("m", 60)]
    parts = []
    for label, size in units:
        if seconds >= size:
            n = seconds // size
            seconds -= n * size
            parts.append(f"{n}{label}")
        if len(parts) == 2:
            break
    return " ".join(parts) if parts else "<1m"


# ---------- worker metrics upload -----------------------------------------

class RateLimiter:
    """Per-key sliding window. Cheap, no external deps, plenty for ~12 keys.

    A worker that gets paused or restarted may retry; the limiter exists to
    cap a misbehaving client, not to police well-behaved cron."""

    def __init__(self, max_per_minute: int) -> None:
        self._max = max_per_minute
        self._window = 60.0
        self._hits: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        if self._max <= 0:
            return True
        now = time.monotonic()
        with self._lock:
            q = self._hits[key]
            cutoff = now - self._window
            while q and q[0] < cutoff:
                q.popleft()
            if len(q) >= self._max:
                return False
            q.append(now)
            return True


METRICS_LIMITER = RateLimiter(METRICS_RATE_PER_MIN)
_ALLOWED_UPLOAD_CT = ("text/plain", "application/octet-stream")


def _valid_worker_host(host: str) -> bool:
    if not WORKER_HOST_RE.match(host):
        return False
    return host in WORKER_HOSTS


def _atomic_write_log(host: str, payload: bytes) -> Path:
    """Write payload to worker_logs/<host>/backfill_metrics.log via a tmp file
    rename so a concurrent read never observes a torn file. Bumps mtime even
    when the content is byte-identical to the previous upload, so the
    dashboard's freshness check ("stalled" if mtime is older than
    STALL_SECONDS) sees the worker as live."""
    base = Path(WORKER_LOGS_DIR) / host
    base.mkdir(parents=True, exist_ok=True)
    target = base / "backfill_metrics.log"
    tmp = base / f".backfill_metrics.log.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        with tmp.open("wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, target)
        now = time.time()
        os.utime(target, (now, now))
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
    return target


# ---------- app ------------------------------------------------------------

app = FastAPI(
    title="chimango-crawl-dashboard",
    description="Read-only monitor for the ChimangoScan tag-backfill crawl.",
    version="0.2.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

if CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_credentials=False,
        allow_methods=["GET"],
        allow_headers=["X-API-Key", "Content-Type"],
        max_age=600,
    )


def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if not API_KEY:
        return
    if not x_api_key or x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid api key")


@app.on_event("startup")
def _startup() -> None:
    try:
        mongo().admin.command("ping")
        LOG.info("mongo reachable")
        ensure_indexes()
    except Exception as exc:
        LOG.error("startup mongo unavailable (will retry on demand): %s", exc)


@app.get("/healthz")
def healthz() -> dict:
    """Process liveness. Cheap and unconditional so the reverse proxy never
    marks the backend as down because Mongo is slow — Mongo health is exposed
    separately at /healthz/db."""
    return {"status": "ok", "ts": time.time()}


@app.get("/healthz/db")
def healthz_db() -> dict:
    try:
        mongo().admin.command("ping")
        return {"status": "ok", "mongo": True, "ts": time.time()}
    except Exception as exc:
        return {"status": "degraded", "mongo": False, "error": str(exc)[:200], "ts": time.time()}


@app.get("/api/v1/progress", dependencies=[Depends(require_api_key)])
def api_progress() -> dict:
    return CACHE.get_or_set("progress", TTL_COUNT, progress_snapshot)


@app.get("/api/v1/history", dependencies=[Depends(require_api_key)])
def api_history(hours: int | None = None) -> dict:
    h = hours or HISTORY_HOURS
    return CACHE.get_or_set(f"history:{h}", TTL_SLOW, lambda: history(h))


@app.get("/api/v1/workers", dependencies=[Depends(require_api_key)])
def api_workers() -> dict:
    return CACHE.get_or_set("workers", TTL_FAST, workers_snapshot)


@app.post(
    "/api/v1/metrics/{host}",
    status_code=200,
    dependencies=[Depends(require_api_key)],
)
async def api_metrics_upload(
    request: Request,
    host: str = PathParam(..., min_length=1, max_length=63),
    content_type: str | None = Header(default=None),
) -> Response:
    """Receive the tail of a worker's backfill_metrics.log.

    Flow inversion vs. the original rsync-pull design: dashboard host (a9) can
    reach the workers' DNS but most workers sit behind a private campus DNS,
    so we let each worker push to the public HTTPS endpoint instead. Body is
    the raw log tail (text), capped at METRICS_MAX_BYTES. Idempotent — the
    same content can be re-uploaded harmlessly. Atomic write keeps concurrent
    readers consistent."""
    if not _valid_worker_host(host):
        raise HTTPException(status_code=403, detail="unknown worker host")

    if not METRICS_LIMITER.allow(host):
        raise HTTPException(status_code=429, detail="rate limit exceeded")

    ct = (content_type or "").split(";", 1)[0].strip().lower()
    if ct and ct not in _ALLOWED_UPLOAD_CT:
        raise HTTPException(status_code=415, detail="unsupported content type")

    # Content-Length check first when the client sets it; some clients omit it.
    declared = request.headers.get("content-length")
    if declared:
        try:
            if int(declared) > METRICS_MAX_BYTES:
                raise HTTPException(status_code=413, detail="payload too large")
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid content-length")

    # Stream into a bounded buffer so a lying Content-Length cannot exhaust RAM.
    body = bytearray()
    async for chunk in request.stream():
        if not chunk:
            continue
        body.extend(chunk)
        if len(body) > METRICS_MAX_BYTES:
            raise HTTPException(status_code=413, detail="payload too large")

    try:
        _atomic_write_log(host, bytes(body))
    except OSError as exc:
        LOG.error("metrics upload write failed for %s: %s", host, exc)
        raise HTTPException(status_code=500, detail="write failed") from exc

    return Response(status_code=200)


@app.get("/api/v1/in-flight", dependencies=[Depends(require_api_key)])
def api_in_flight(limit: int = 25) -> dict:
    """In-flight is intentionally cached at TTL_SLOW: the set only changes when
    a worker claims or releases a repo, which the frontend now polls at the same
    cadence. The old 10 s TTL meant repeated sort-by-started_at scans for no
    observable benefit."""
    limit = max(1, min(limit, 100))
    return {"items": CACHE.get_or_set(f"in_flight:{limit}", TTL_SLOW, lambda: in_flight_repos(limit))}


@app.get("/api/v1/recent", dependencies=[Depends(require_api_key)])
def api_recent(limit: int = 25) -> dict:
    """Mongo-heavy: descending sort by tags_backfilled_at. The backing partial
    index covers it but the list only changes when a worker marks a repo, which
    is at most ~tens per minute. A 60 s TTL collapses dozens of polls."""
    limit = max(1, min(limit, 100))
    return {"items": CACHE.get_or_set(f"recent:{limit}", TTL_SLOW, lambda: recent_backfilled(limit))}


@app.get("/api/v1/top-pending", dependencies=[Depends(require_api_key)])
def api_top_pending(limit: int = 25) -> dict:
    """Heaviest read in the dashboard: composite filter + sort by pull_count.
    Same 60 s TTL as /recent — the head of the queue only changes when something
    is claimed."""
    limit = max(1, min(limit, 100))
    return {"items": CACHE.get_or_set(f"top_pending:{limit}", TTL_SLOW, lambda: top_pending(limit))}


@app.get("/api/v1/extras", dependencies=[Depends(require_api_key)])
def api_extras() -> dict:
    """Extra fleet-wide metrics that complement /overview.

    Composition:
      - tags_collected_24h: ObjectId-bounded count on tags_data, the only
        leaf that actually quantifies the gain the backfill exists for.
      - backfilled_24h: matching repo count, so the dashboard can show
        avg_tags_per_repo without a divide-by-zero dance on the client.
      - cache_hit / rate_limit_alert / stuck_workers: pure derivations on
        the already-cached workers snapshot, so they cost zero round trips.
      - tag_distribution: bounded aggregate, heaviest leaf in the file —
        TTL_HEAVY (10 min) so even a refresh storm hits Mongo at most once
        every ten minutes."""
    workers = CACHE.get_or_set("workers", TTL_FAST, workers_snapshot)
    tags_24h = CACHE.get_or_set("tags_collected:24", TTL_SLOW, lambda: tags_collected(24))
    backfilled_24h = CACHE.get_or_set("backfilled_window:24", TTL_SLOW, lambda: backfilled_window(24))
    distribution = CACHE.get_or_set("tag_distribution", TTL_HEAVY, tag_distribution)
    derived = derived_metrics(workers)
    avg_tags = (tags_24h["count"] / backfilled_24h) if backfilled_24h else 0.0
    return {
        "tags_collected_24h": tags_24h,
        "backfilled_24h": backfilled_24h,
        "avg_tags_per_repo_24h": round(avg_tags, 1),
        "cache_hit": derived["cache_hit"],
        "rate_limit_alert": derived["rate_limit_alert"],
        "stuck_workers": derived["stuck_workers"],
        "tag_distribution": distribution,
        "ts": _utcnow().isoformat(),
    }


@app.get("/api/v1/overview", dependencies=[Depends(require_api_key)])
def api_overview() -> dict:
    """One-shot aggregate used by the dashboard's main render to keep request
    count low. Each leaf is independently cached, so this is cheap."""
    progress = CACHE.get_or_set("progress", TTL_COUNT, progress_snapshot)
    workers = CACHE.get_or_set("workers", TTL_FAST, workers_snapshot)
    return {
        "progress": progress,
        "workers": workers,
        "eta": eta(progress, workers),
        "ts": _utcnow().isoformat(),
    }


@app.exception_handler(PyMongoError)
def _mongo_err(req: Request, exc: PyMongoError) -> JSONResponse:
    LOG.error("mongo error on %s: %s", req.url.path, exc)
    return JSONResponse(
        status_code=503,
        content={"error": "database unavailable", "detail": str(exc)[:200]},
    )
