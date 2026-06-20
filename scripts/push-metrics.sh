#!/usr/bin/env bash
# push-metrics.sh — ship the tail of the local backfill metrics log to the
# crawl dashboard.
#
# Designed to be invoked from cron once a minute on every worker host. The
# dashboard host (a9) does not have inbound SSH to the worker fleet, so the
# flow is inverted: the worker reaches the public HTTPS endpoint and pushes.
#
# Required env:
#   DASHBOARD_URL   base URL of the dashboard, e.g. https://crawl.anonshield.org
#   API_KEY         shared secret (matches the backend's API_KEY)
#
# Optional env:
#   WORKER_HOST     host id to report as; defaults to $(hostname -s)
#   METRICS_FILE    path to the log file to ship; defaults to
#                   $HOME/DITector_research/backfill_metrics.log
#   MAX_BYTES       max bytes to send (must be <= server METRICS_MAX_BYTES);
#                   default 1048576 (1 MiB)
#   LOG_FILE        where to record outcomes; default /tmp/push-metrics.log
#
# Idempotent: re-uploading the same content is a no-op for the dashboard
# (server bumps mtime so freshness still advances). Silent on transient
# network failures — the next cron tick will retry.

set -u
umask 077

WORKER_HOST="${WORKER_HOST:-$(hostname -s)}"
METRICS_FILE="${METRICS_FILE:-$HOME/DITector_research/backfill_metrics.log}"
MAX_BYTES="${MAX_BYTES:-1048576}"
LOG_FILE="${LOG_FILE:-/tmp/push-metrics.log}"

log() {
    printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >>"$LOG_FILE" 2>/dev/null || true
}

die_silent() {
    log "$*"
    exit 0
}

if [[ -z "${DASHBOARD_URL:-}" || -z "${API_KEY:-}" ]]; then
    die_silent "config error: DASHBOARD_URL and API_KEY must be set"
fi

if [[ ! -r "$METRICS_FILE" ]]; then
    die_silent "metrics file missing or unreadable: $METRICS_FILE"
fi

tmpfile="$(mktemp -t push-metrics.XXXXXX)" || die_silent "mktemp failed"
trap 'rm -f "$tmpfile"' EXIT

# Tail by bytes so a large log file does not blow the limit. tail -c is
# POSIX-ish; coreutils ships it.
tail -c "$MAX_BYTES" -- "$METRICS_FILE" >"$tmpfile" 2>/dev/null \
    || die_silent "tail failed on $METRICS_FILE"

bytes=$(wc -c <"$tmpfile" 2>/dev/null || echo 0)
if [[ "$bytes" -eq 0 ]]; then
    die_silent "empty tail; nothing to push"
fi

url="${DASHBOARD_URL%/}/api/v1/metrics/${WORKER_HOST}"

# --fail so 4xx/5xx are non-zero; --silent to keep cron mail quiet; --max-time
# so a stuck connection cannot pile up cron invocations.
http_code=$(curl --silent --show-error --fail \
    --max-time 25 \
    --retry 2 --retry-delay 3 --retry-connrefused \
    --connect-timeout 8 \
    --output /dev/null --write-out '%{http_code}' \
    -H "X-API-Key: ${API_KEY}" \
    -H "Content-Type: text/plain; charset=utf-8" \
    --data-binary "@${tmpfile}" \
    -X POST "$url" 2>>"$LOG_FILE") || {
    log "push failed host=${WORKER_HOST} bytes=${bytes} url=${url}"
    exit 0
}

log "push ok host=${WORKER_HOST} bytes=${bytes} status=${http_code}"
