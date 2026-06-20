/* crawl-backfill monitor — vanilla JS, Chart.js for the timeline.
   Design goals: graceful degradation (no jank when a leaf fails), explicit
   loading/error states, accessible status announcements, minimum motion. */

(() => {
  "use strict";

  const POLL_MS = 10_000;
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
    $("#workers-summary").textContent =
      `${summary.up || 0} up · ${summary.stalled || 0} stalled · ${summary.down || 0} down · aggregate ${(summary.aggregate_rate_per_min || 0).toFixed(1)} repos/min`;

    if (!snap.workers || snap.workers.length === 0) {
      list.innerHTML = `<p class="sec-desc">no worker logs found yet — workers write to WORKER_LOGS_DIR.</p>`;
      return;
    }
    for (const w of snap.workers) {
      const node = document.createElement("article");
      node.className = "worker";
      node.dataset.state = w.status;
      node.setAttribute("role", "listitem");

      const m = w.metrics || {};
      const lastSeen = w.last_seen_seconds == null ? "—"
        : (w.last_seen_seconds < 90 ? `${w.last_seen_seconds}s` : `${Math.round(w.last_seen_seconds / 60)}m`);
      const topErr = (w.top_errors && w.top_errors[0]) || null;

      node.innerHTML = `
        <div class="top">
          <span class="host">${escapeHTML(w.host)}</span>
          <span class="pill">${escapeHTML(w.status)}</span>
        </div>
        <div class="row"><span class="k">rate</span><span class="v">${m.rate_per_min != null ? m.rate_per_min.toFixed(1) : "—"} /min</span></div>
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

  /* ---------- main poll loop ---------- */

  let inflight = false;

  async function tick() {
    if (inflight) return;
    inflight = true;
    try {
      const [overview, history, inflightItems, recent, topPending] = await Promise.all([
        Api.get("/api/v1/overview"),
        Api.get("/api/v1/history"),
        Api.get("/api/v1/in-flight?limit=15"),
        Api.get("/api/v1/recent?limit=15"),
        Api.get("/api/v1/top-pending?limit=20"),
      ]);

      renderKpis(overview.progress);
      renderProgressBar(overview.progress);
      renderWorkers(overview.workers);
      renderHistory(history);

      $("#agg-rate").textContent = (overview.eta.rate_per_min || 0).toFixed(1);
      $("#eta-human").textContent = overview.eta.human || "—";

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

      $("#last-update").textContent = fmtTime(overview.ts);
      $("#last-update").dateTime = overview.ts;
      Health.set("ok", "live");
    } catch (e) {
      console.error("tick", e);
      if (e.status === 401) { await askForKey(); }
      else { Health.set("degraded", e.message || "error"); }
    } finally {
      inflight = false;
    }
  }

  /* ---------- auth dialog ---------- */

  async function askForKey() {
    Health.set("down", "auth required");
    const dlg = $("#auth-dialog");
    const input = $("#auth-input");
    input.value = Api.key || "";
    if (typeof dlg.showModal === "function") {
      dlg.showModal();
      $("#auth-save").addEventListener("click", () => { Api.setKey(input.value.trim()); tick(); }, { once: true });
    } else {
      const k = prompt("API key:");
      if (k) { Api.setKey(k); tick(); }
    }
  }

  /* ---------- boot ---------- */

  function boot() {
    $("#poll-interval").textContent = String(POLL_MS / 1000);
    $("#refresh").addEventListener("click", () => tick());
    if (!Api.key) { askForKey(); return; }
    tick();
    setInterval(tick, POLL_MS);
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) tick();
    });
  }

  document.addEventListener("DOMContentLoaded", boot);
})();
