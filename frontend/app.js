/* crawl-backfill monitor — vanilla JS, Chart.js for the timeline.
   Design goals: graceful degradation (no jank when a leaf fails), explicit
   loading/error states, accessible status announcements, minimum motion. */

(() => {
  "use strict";

  /* Two polling cadences: fast leaves (overview/workers/history) refresh every
     POLL_FAST_MS so live state stays live; slow leaves (in-flight, recently
     completed, next-in-queue, fleet-wide extras) refresh every POLL_SLOW_MS.
     Backend TTLs are aligned so polling more aggressively would only churn the
     cache without reaching Mongo any sooner. */
  const POLL_FAST_MS = 10_000;
  const POLL_SLOW_MS = 60_000;
  const KEY_STORAGE = "crawl-backfill-apikey";
  const BASE = (document.documentElement.dataset.apiBase || "").replace(/\/$/, "");

  /* ---------- tiny utilities ---------- */

  const $ = (sel) => document.querySelector(sel);
  const fmtInt = (n) => (n == null ? "—" : Number(n).toLocaleString("en-US"));
  const fmtPct = (n) => (n == null ? "—" : `${n.toFixed(1)}%`);
  const fmtTime = (iso) => {
    if (!iso) return "—";
    try { return new Date(iso).toLocaleTimeString("en-GB", { hour12: false }); }
    catch { return iso; }
  };
  const sinceShort = (iso) => {
    if (!iso) return "";
    const d = (Date.now() - new Date(iso).getTime()) / 1000;
    if (d < 60) return `${Math.round(d)}s ago`;
    if (d < 3600) return `${Math.round(d / 60)}m ago`;
    if (d < 86400) return `${Math.round(d / 3600)}h ago`;
    return `${Math.round(d / 86400)}d ago`;
  };
  const escapeHTML = (s) => String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])
  );

  /* ---------- API client ---------- */

  class ApiError extends Error {
    constructor(msg, status) { super(msg); this.status = status; }
  }

  const Api = {
    key: localStorage.getItem(KEY_STORAGE) || "",
    setKey(k) { this.key = k; localStorage.setItem(KEY_STORAGE, k); },
    async get(path) {
      const url = `${BASE}${path}`;
      const r = await fetch(url, {
        headers: this.key ? { "X-API-Key": this.key } : {},
        cache: "no-store",
      });
      if (r.status === 401) { throw new ApiError("unauthorized", 401); }
      if (!r.ok) { throw new ApiError(`http ${r.status}`, r.status); }
      return r.json();
    },
  };

  /* ---------- health badge ---------- */

  const Health = {
    set(state, label) {
      const el = $("#health");
      el.dataset.state = state;
      el.querySelector(".health-label").textContent = label || state;
    },
  };

  /* ---------- KPI render ---------- */

  function renderKpis(progress) {
    const kpis = $("#kpis");
    kpis.innerHTML = "";
    const cards = [
      { lbl: "backfilled", val: fmtInt(progress.backfilled), sub: fmtPct(progress.pct), tone: "accent" },
      { lbl: "pending",    val: fmtInt(progress.pending),    sub: `≥ ${progress.threshold} pulls`, tone: "" },
      { lbl: "in flight",  val: fmtInt(progress.in_flight),  sub: "claimed by a worker", tone: "inflight" },
      { lbl: "eligible",   val: fmtInt(progress.eligible),   sub: "stage II complete",   tone: "" },
    ];
    for (const c of cards) {
      const node = document.createElement("div");
      node.className = "kpi";
      if (c.tone) node.dataset.tone = c.tone;
      node.innerHTML = `<div class="lbl">${escapeHTML(c.lbl)}</div>
                        <div class="val">${escapeHTML(c.val)}</div>
                        <div class="sub">${escapeHTML(c.sub)}</div>`;
      kpis.appendChild(node);
    }
  }

  function renderProgressBar(progress) {
    const tot = progress.eligible || 1;
    const donePct = (progress.backfilled / tot) * 100;
    const inflightPct = ((progress.backfilled + progress.in_flight) / tot) * 100;
    $("#bar-fill").style.width = `${donePct}%`;
    $("#bar-inflight").style.width = `${inflightPct}%`;
    $("#progress-label").textContent =
      `${fmtInt(progress.backfilled)} / ${fmtInt(progress.eligible)} (${fmtPct(progress.pct)})`;
  }

  /* ---------- workers ---------- */

  function renderWorkers(snap) {
    const list = $("#workers");
    list.innerHTML = "";
    const summary = snap.summary || {};
    const parts = [
      `${summary.up || 0} up`,
      `${summary.stalled || 0} stalled`,
      `${summary.down || 0} down`,
    ];
    if (summary.stuck) parts.push(`${summary.stuck} stuck`);
    /* tags/min is the canonical throughput signal — repos vary by orders of
       magnitude in tag count, so repos/min is biased. We keep repos/min as a
       secondary readout for continuity with old dashboards/screenshots. */
    if (summary.aggregate_tags_per_min != null) {
      parts.push(`aggregate ${summary.aggregate_tags_per_min.toFixed(1)} tags/min`);
    }
    parts.push(`${(summary.aggregate_rate_per_min || 0).toFixed(1)} repos/min`);
    $("#workers-summary").textContent = parts.join(" · ");

    if (!snap.workers || snap.workers.length === 0) {
      list.innerHTML = `<p class="sec-desc">no worker logs found yet — workers write to WORKER_LOGS_DIR.</p>`;
      return;
    }
    for (const w of snap.workers) {
      const node = document.createElement("article");
      node.className = "worker";
      node.dataset.state = w.status;
      if (w.stuck) node.dataset.stuck = "true";
      node.setAttribute("role", "listitem");

      const m = w.metrics || {};
      const lastSeen = w.last_seen_seconds == null ? "—"
        : (w.last_seen_seconds < 90 ? `${w.last_seen_seconds}s` : `${Math.round(w.last_seen_seconds / 60)}m`);
      const topErr = (w.top_errors && w.top_errors[0]) || null;
      const pillLabel = w.stuck ? `${w.status} · stuck` : w.status;

      /* tags/min sits in the headline slot (where "rate" used to be) because
         it is the unbiased throughput signal — a single repo can carry 10k
         tags, so repos/min hides large variations in real work. repos/min
         and imgs/min remain as secondary rows. A worker on the old binary
         emits no tags/min: we render '—' so the absence is explicit. */
      const tags = m.tags_per_min;
      const imgs = m.imgs_per_min;
      node.innerHTML = `
        <div class="top">
          <span class="host">${escapeHTML(w.host)}</span>
          <span class="pill">${escapeHTML(pillLabel)}</span>
        </div>
        <div class="row primary"><span class="k">tags/min</span><span class="v">${tags != null ? tags.toFixed(0) : "—"}</span></div>
        <div class="row"><span class="k">imgs/min</span><span class="v">${imgs != null ? imgs.toFixed(0) : "—"}</span></div>
        <div class="row"><span class="k">repos/min</span><span class="v">${m.rate_per_min != null ? m.rate_per_min.toFixed(1) : "—"}</span></div>
        <div class="row"><span class="k">processed</span><span class="v">${m.processed != null ? fmtInt(m.processed) : "—"}</span></div>
        <div class="row"><span class="k">errors</span><span class="v">${m.errors != null ? fmtInt(m.errors) : "—"}</span></div>
        <div class="row"><span class="k">eta</span><span class="v">${escapeHTML(m.eta || "—")}</span></div>
        <div class="row"><span class="k">last seen</span><span class="v">${escapeHTML(lastSeen)}</span></div>
        ${topErr ? `<div class="err-line"><b>${topErr.count}×</b> ${escapeHTML(topErr.msg)}</div>` : ""}
      `;
      list.appendChild(node);
    }

    renderErrorsRollup(snap.workers);
  }

  /* ---------- extras (fleet-wide derived + slow leaves) ---------- */

  function renderExtras(ex) {
    if (!ex) return;
    const collected = ex.tags_collected_24h && ex.tags_collected_24h.count;
    $("#extra-tags-24h").textContent = collected != null ? fmtInt(collected) : "—";
    $("#extra-tags-24h-sub").textContent = `${fmtInt(ex.backfilled_24h || 0)} repos · avg ${ex.avg_tags_per_repo_24h ?? 0} tags/repo`;

    const ch = ex.cache_hit || {};
    $("#extra-cache").textContent = ch.tag_pct != null ? `${ch.tag_pct.toFixed(1)}%` : "—";
    $("#extra-cache-sub").textContent =
      `imgs ${ch.img_pct != null ? ch.img_pct.toFixed(1) : "—"}% · ${ch.live_workers || 0} live workers · ${ch.weighted_by || "—"}`;

    const stuck = ex.stuck_workers || {};
    const stuckCard = $("#extra-stuck-card");
    $("#extra-stuck").textContent = String(stuck.count || 0);
    $("#extra-stuck-sub").textContent = (stuck.hosts && stuck.hosts.length)
      ? stuck.hosts.slice(0, 3).join(", ") + (stuck.hosts.length > 3 ? ` +${stuck.hosts.length - 3}` : "")
      : "all live workers producing";
    stuckCard.dataset.tone = stuck.count > 0 ? "warn" : "";

    const alert = ex.rate_limit_alert || {};
    const alertBar = $("#rate-alert");
    if (alert.active) {
      alertBar.hidden = false;
      const hostList = (alert.hosts || []).map((h) => escapeHTML(h.host)).join(", ");
      alertBar.querySelector(".rate-alert-msg").textContent =
        `Docker Hub rate-limit signal detected on ${(alert.hosts || []).length} worker(s): ${hostList}. Total recent hits: ${alert.total_recent}.`;
    } else {
      alertBar.hidden = true;
    }

    const dist = ex.tag_distribution || {};
    const distList = $("#extra-distribution");
    if (!dist.buckets || !dist.buckets.length) {
      distList.innerHTML = `<li class="dist-empty">no distribution data yet</li>`;
    } else {
      const max = Math.max(1, ...dist.buckets.map((b) => b.count));
      distList.innerHTML = dist.buckets.map((b) => {
        const pct = (b.count / max) * 100;
        return `<li>
          <span class="dist-label">${escapeHTML(b.label)} tags</span>
          <span class="dist-bar" aria-hidden="true"><span class="dist-fill" style="width:${pct.toFixed(1)}%"></span></span>
          <span class="dist-count mono">${fmtInt(b.count)}</span>
        </li>`;
      }).join("");
    }
    $("#extra-distribution-sub").textContent = dist.sample_repos
      ? `n=${fmtInt(dist.sample_repos)} repos · last ${dist.hours || 24} h`
      : "no repos in window";
  }

  function renderErrorsRollup(workers) {
    const sect = $("#errors-section");
    const container = $("#errors");
    const rows = [];
    for (const w of workers || []) {
      for (const e of (w.top_errors || []).slice(0, 2)) {
        rows.push({ host: w.host, msg: e.msg, n: e.count });
      }
    }
    if (rows.length === 0) { sect.hidden = true; return; }
    sect.hidden = false;
    rows.sort((a, b) => b.n - a.n);
    container.innerHTML = rows.slice(0, 12).map((r) =>
      `<div class="err-row" role="status">
         <span class="host">${escapeHTML(r.host)}</span>
         <span class="msg" title="${escapeHTML(r.msg)}">${escapeHTML(r.msg)}</span>
         <span class="n">${escapeHTML(String(r.n))}×</span>
       </div>`
    ).join("");
  }

  /* ---------- repo lists ---------- */

  function renderRepoList(selector, items, meta) {
    const ol = $(selector);
    if (!items || items.length === 0) {
      ol.classList.add("empty");
      ol.innerHTML = `<li>${escapeHTML(meta.empty || "nothing here yet")}</li>`;
      return;
    }
    ol.classList.remove("empty");
    ol.innerHTML = items.map((r) => {
      const name = `${r.namespace || ""}/${r.name || ""}`;
      const pulls = r.pull_count != null ? `<span><b>${fmtInt(r.pull_count)}</b> pulls</span>` : "";
      const right = meta.metaFor ? meta.metaFor(r) : "";
      return `<li>
        <span class="name" title="${escapeHTML(name)}">${escapeHTML(name)}</span>
        <span class="meta">${pulls}${right}</span>
      </li>`;
    }).join("");
  }

  /* ---------- chart ---------- */

  let historyChart = null;
  function renderHistory(hist) {
    const ctx = $("#chart-history");
    const labels = hist.buckets.map((b) => fmtTime(b.ts));
    const data = hist.buckets.map((b) => b.count);

    if (!window.Chart) return;

    if (!historyChart) {
      const css = getComputedStyle(document.documentElement);
      const accent = css.getPropertyValue("--accent").trim();
      const gridc = css.getPropertyValue("--rule").trim();
      const text = css.getPropertyValue("--text-2").trim();

      historyChart = new Chart(ctx, {
        type: "bar",
        data: { labels, datasets: [{
          data, backgroundColor: accent, borderRadius: 2, maxBarThickness: 14,
        }] },
        options: {
          responsive: true, maintainAspectRatio: false, animation: { duration: 200 },
          plugins: { legend: { display: false }, tooltip: {
            backgroundColor: "#0b0d11", borderColor: gridc, borderWidth: 1,
            titleColor: "#e8ecf3", bodyColor: "#b1b9c8",
            callbacks: { label: (i) => `${i.parsed.y} repos backfilled` },
          } },
          scales: {
            x: { grid: { color: gridc, display: false }, ticks: { color: text, maxRotation: 0, autoSkipPadding: 24 } },
            y: { grid: { color: gridc }, ticks: { color: text, precision: 0 }, beginAtZero: true },
          },
        },
      });
    } else {
      historyChart.data.labels = labels;
      historyChart.data.datasets[0].data = data;
      historyChart.update("none");
    }
  }

  /* ---------- main poll loop ----------
     Split into fast (live state) and slow (queue + fleet-wide derived). The
     in-flight / recently-completed / next-in-queue panels back Mongo queries
     that only change on claim/mark events — polling them every second was
     paying for an empty diff. Backend TTLs match so the slow cadence does
     not desync. */

  let fastInflight = false;
  let slowInflight = false;
  const lastSlowTs = { v: 0 };

  async function tickFast() {
    if (fastInflight) return;
    fastInflight = true;
    try {
      const [overview, history] = await Promise.all([
        Api.get("/api/v1/overview"),
        Api.get("/api/v1/history"),
      ]);

      renderKpis(overview.progress);
      renderProgressBar(overview.progress);
      renderWorkers(overview.workers);
      renderHistory(history);

      /* Headline = tags/min (unbiased throughput). repos/min stays as the
         secondary readout, partly for continuity and partly because the ETA
         is still derived from it — the universe of tags is unknown until each
         repo is listed, so a tags-based ETA is not well defined. */
      const wsum = (overview.workers && overview.workers.summary) || {};
      const tagsAgg = wsum.aggregate_tags_per_min;
      const imgsAgg = wsum.aggregate_imgs_per_min;
      $("#agg-tags").textContent = tagsAgg != null ? tagsAgg.toFixed(0) : "—";
      $("#agg-imgs").textContent = imgsAgg != null ? imgsAgg.toFixed(0) : "—";
      $("#agg-rate").textContent = (overview.eta.rate_per_min || 0).toFixed(1);
      $("#eta-human").textContent = overview.eta.human || "—";

      $("#last-update").textContent = fmtTime(overview.ts);
      $("#last-update").dateTime = overview.ts;
      Health.set("ok", "live");
    } catch (e) {
      console.error("tickFast", e);
      if (e.status === 401) { await askForKey(); }
      else { Health.set("degraded", e.message || "error"); }
    } finally {
      fastInflight = false;
    }
  }

  async function tickSlow(force) {
    if (slowInflight) return;
    if (!force && Date.now() - lastSlowTs.v < POLL_SLOW_MS - 1_000) return;
    slowInflight = true;
    try {
      const [inflightItems, recent, topPending, extras] = await Promise.all([
        Api.get("/api/v1/in-flight?limit=15"),
        Api.get("/api/v1/recent?limit=15"),
        Api.get("/api/v1/top-pending?limit=20"),
        Api.get("/api/v1/extras"),
      ]);

      renderRepoList("#inflight-list", inflightItems.items, {
        empty: "no workers are claiming repos right now",
        metaFor: (r) => `<span>${sinceShort(r.started_at)}</span>`,
      });
      renderRepoList("#recent-list", recent.items, {
        empty: "no repos backfilled yet",
        metaFor: (r) => `<span>${sinceShort(r.backfilled_at)}</span>`,
      });
      renderRepoList("#top-list", topPending.items, {
        empty: "queue is drained",
      });
      renderExtras(extras);

      lastSlowTs.v = Date.now();
    } catch (e) {
      console.error("tickSlow", e);
      if (e.status === 401) { await askForKey(); }
      // do not demote health for slow-cycle failures; the fast cycle owns the badge
    } finally {
      slowInflight = false;
    }
  }

  function tickAll(force) {
    tickFast();
    tickSlow(force);
  }

  /* ---------- auth dialog ---------- */

  async function askForKey() {
    Health.set("down", "auth required");
    const dlg = $("#auth-dialog");
    const input = $("#auth-input");
    input.value = Api.key || "";
    if (typeof dlg.showModal === "function") {
      dlg.showModal();
      $("#auth-save").addEventListener("click", () => { Api.setKey(input.value.trim()); tickAll(true); }, { once: true });
    } else {
      const k = prompt("API key:");
      if (k) { Api.setKey(k); tickAll(true); }
    }
  }

  /* ---------- boot ---------- */

  function boot() {
    $("#poll-fast").textContent = String(POLL_FAST_MS / 1000);
    $("#poll-slow").textContent = String(POLL_SLOW_MS / 1000);
    /* Manual refresh forces both cycles to fire immediately — useful when an
       operator just claimed/marked a repo and wants to see the queue update
       without waiting for the slow cadence. */
    $("#refresh").addEventListener("click", () => tickAll(true));
    if (!Api.key) { askForKey(); return; }
    tickAll(true);
    setInterval(tickFast, POLL_FAST_MS);
    setInterval(() => tickSlow(false), POLL_SLOW_MS);
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) tickAll(true);
    });
  }

  document.addEventListener("DOMContentLoaded", boot);
})();
