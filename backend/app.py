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
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
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

TTL_FAST = 10.0
TTL_SLOW = 30.0
TTL_COUNT = 60.0


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
                appname="crawl-backfill-dashboard",
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


# ---------- worker log parsing --------------------------------------------

# Backfill metrics log line emitted by the worker:
# [BACKFILL METRICS 18:01:12] progresso=12/3456 (0.3%) | taxa=2.4 repos/min | ETA=21h30m | cache tags=80% imgs=72% | neo4j=400 | erros=2 | uptime=5m0s
METRIC_RE = re.compile(
    r"\[(?P<label>BACKFILL|BUILD) METRICS (?P<hms>\d\d:\d\d:\d\d)\] "
    r"progresso=(?P<done>\d+)/(?P<total>\d+) \((?P<pct>[\d.]+)%\) \| "
    r"taxa=(?P<rate>[\d.]+) repos/min \| "
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


def workers_snapshot() -> dict:
    if WORKER_HOSTS:
        hosts = WORKER_HOSTS
    elif Path(WORKER_LOGS_DIR).exists():
        hosts = sorted({d.name for d in Path(WORKER_LOGS_DIR).iterdir() if d.is_dir()})
    else:
        hosts = []
    snaps = [_parse_worker(h) for h in hosts]
    total_rate = sum((s["metrics"] or {}).get("rate_per_min", 0) for s in snaps if s["metrics"])
    return {
        "workers": snaps,
        "summary": {
            "configured": len(hosts),
            "up": sum(1 for s in snaps if s["status"] == "up"),
            "stalled": sum(1 for s in snaps if s["status"] == "stalled"),
            "down": sum(1 for s in snaps if s["status"] == "down"),
            "aggregate_rate_per_min": round(total_rate, 2),
        },
        "ts": _utcnow().isoformat(),
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


@app.get("/api/v1/in-flight", dependencies=[Depends(require_api_key)])
def api_in_flight(limit: int = 25) -> dict:
    limit = max(1, min(limit, 100))
    return {"items": CACHE.get_or_set(f"in_flight:{limit}", TTL_FAST, lambda: in_flight_repos(limit))}


@app.get("/api/v1/recent", dependencies=[Depends(require_api_key)])
def api_recent(limit: int = 25) -> dict:
    limit = max(1, min(limit, 100))
    return {"items": CACHE.get_or_set(f"recent:{limit}", TTL_FAST, lambda: recent_backfilled(limit))}


@app.get("/api/v1/top-pending", dependencies=[Depends(require_api_key)])
def api_top_pending(limit: int = 25) -> dict:
    limit = max(1, min(limit, 100))
    return {"items": CACHE.get_or_set(f"top_pending:{limit}", TTL_SLOW, lambda: top_pending(limit))}


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
