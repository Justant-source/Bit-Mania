"use strict";

const CARD_BG = "#161b22";
const BG      = "#0d1117";
const GRID    = "#21262d";
const TEXT    = "#c9d1d9";
const PROFIT  = "#3fb950";
const LOSS    = "#f85149";

function darkLayout(extra) {
  return Object.assign({
    paper_bgcolor: CARD_BG, plot_bgcolor: BG,
    font: { color: TEXT, size: 11 },
    xaxis: { gridcolor: GRID, zerolinecolor: GRID, color: TEXT },
    yaxis: { gridcolor: GRID, zerolinecolor: GRID, color: TEXT },
    margin: { l: 55, r: 20, t: 20, b: 40 },
    autosize: true,
    hovermode: "x unified",
  }, extra);
}

async function apiFetch(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

function fmtKST(isoStr) {
  if (!isoStr) return "—";
  return new Date(isoStr).toLocaleString("ko-KR", {
    timeZone: "Asia/Seoul", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit",
  });
}

function fmtUSD(val) {
  if (val === null || val === undefined) return "—";
  const n = Number(val);
  return (n >= 0 ? "+" : "") + `$${n.toFixed(2)}`;
}

function setEl(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

function setElHtml(id, html) {
  const el = document.getElementById(id);
  if (el) el.innerHTML = html;
}

// ── Portfolio ─────────────────────────────────────────────────────────────

async function updatePortfolio() {
  try {
    const data = await apiFetch("/api/internal/monitor/portfolio?days=30");
    const lat = data.latest || {};

    setEl("kpi-equity", lat.equity ? `$${Number(lat.equity).toFixed(2)}` : "—");
    setEl("kpi-equity-sub", lat.snapshot_at ? fmtKST(lat.snapshot_at) : "—");

    const dailyEl = document.getElementById("kpi-daily-pnl");
    if (dailyEl) {
      const v = lat.daily_pnl;
      dailyEl.textContent = fmtUSD(v);
      dailyEl.className = `kpi-value ${v >= 0 ? "pos" : "neg"}`;
    }

    const urEl = document.getElementById("kpi-unrealized");
    if (urEl) {
      const v = lat.unrealized_pnl;
      urEl.textContent = fmtUSD(v);
      urEl.className = `kpi-value ${!v || v >= 0 ? "pos" : "neg"}`;
    }

    const mtdEl = document.getElementById("kpi-mtd");
    if (mtdEl) {
      const v = lat.mtd_pnl;
      mtdEl.textContent = v !== null ? fmtUSD(v) : "—";
      mtdEl.className = `kpi-value ${!v || v >= 0 ? "pos" : "neg"}`;
    }

    const mddEl = document.getElementById("kpi-mdd");
    if (mddEl && data.max_drawdown !== null) {
      const pct = (data.max_drawdown * 100).toFixed(1);
      mddEl.textContent = `${pct}%`;
      mddEl.className = `kpi-value ${data.max_drawdown < -0.05 ? "neg" : "warn"}`;
    }

    // Equity chart
    if (data.curve && data.curve.length > 0) {
      const div = document.getElementById("chart-equity");
      Plotly.react(div, [{
        type: "scatter", mode: "lines",
        x: data.curve.map((d) => new Date(d.ts)),
        y: data.curve.map((d) => d.equity),
        name: "총 자산", line: { color: "#58a6ff", width: 2 },
        hovertemplate: "%{y:$,.2f}<extra></extra>",
      }], darkLayout({
        height: 210,
        yaxis: { title: "USD", tickformat: "$,.0f" },
        xaxis: { type: "date" },
      }), { responsive: true, displayModeBar: false });
      div.querySelector(".empty-state")?.remove();
    }
  } catch (err) {
    console.warn("[monitor] portfolio error:", err);
  }
}

// ── Kill Switch ───────────────────────────────────────────────────────────

async function updateKillSwitch() {
  try {
    const data = await apiFetch("/api/internal/monitor/killswitch");

    const ksEl = document.getElementById("kpi-ks");
    const ksSubEl = document.getElementById("kpi-ks-sub");
    if (data.is_active) {
      if (ksEl) { ksEl.textContent = "🔴 발동"; ksEl.className = "kpi-value neg"; }
      if (ksSubEl) { ksSubEl.textContent = data.current_event?.reason || "활성"; }
    } else {
      if (ksEl) { ksEl.textContent = "🟢 정상"; ksEl.className = "kpi-value pos"; }
      if (ksSubEl) { ksSubEl.textContent = "이상 없음"; }
    }

    const tbody = document.getElementById("ks-tbody");
    if (tbody) {
      if (!data.history || data.history.length === 0) {
        tbody.innerHTML = `<tr><td colspan="5" style="color:#484f58;text-align:center">이력 없음</td></tr>`;
      } else {
        const lvlLabel = { 1: "L1-전략", 2: "L2-포트폴리오", 3: "L3-시스템", 4: "L4-수동" };
        tbody.innerHTML = data.history.map((e) =>
          `<tr>
            <td>${fmtKST(e.triggered_at)}</td>
            <td><span class="badge badge-red">${lvlLabel[e.level] || `L${e.level}`}</span></td>
            <td>${e.reason}</td>
            <td>${e.resolved_at ? fmtKST(e.resolved_at) : '<span class="neg">미해제</span>'}</td>
            <td class="${e.pnl_at_trigger >= 0 ? "pos" : "neg"}">${e.pnl_at_trigger ? fmtUSD(e.pnl_at_trigger) : "—"}</td>
          </tr>`
        ).join("");
      }
    }
  } catch (err) {
    console.warn("[monitor] killswitch error:", err);
  }
}

// ── Regime ────────────────────────────────────────────────────────────────

const REGIME_COLORS = { ranging: "#2196F3", trending_up: "#4CAF50", trending_down: "#F44336", volatile: "#FF9800", uncertain: "#9E9E9E" };
const REGIME_LABELS = { ranging: "횡보", trending_up: "상승추세", trending_down: "하락추세", volatile: "변동성", uncertain: "불확실" };

async function updateRegime() {
  try {
    const data = await apiFetch("/api/internal/monitor/regime?days=7");
    const panel = document.getElementById("regime-panel");
    if (!panel) return;

    const cur = data.current;
    const w = data.weights || {};
    const regimeName = cur ? (REGIME_LABELS[cur.regime] || cur.regime) : "—";
    const regimeColor = cur ? (REGIME_COLORS[cur.regime] || "#9E9E9E") : "#9E9E9E";

    const weightRows = Object.entries(w).map(([k, v]) =>
      `<div style="display:flex;justify-content:space-between;font-size:12px;padding:2px 0">
        <span style="color:#8b949e">${k}</span>
        <span style="color:#c9d1d9">${(v * 100).toFixed(0)}%</span>
      </div>`
    ).join("");

    panel.innerHTML = `
      <div style="margin-bottom:10px">
        <span style="font-size:18px;font-weight:700;color:${regimeColor}">${regimeName}</span>
        ${cur && cur.transition_type ? `<span style="font-size:11px;color:#8b949e;margin-left:8px">${cur.transition_type}</span>` : ""}
      </div>
      <div style="margin-bottom:4px;font-size:11px;color:#484f58;text-transform:uppercase;letter-spacing:.05em">오케스트레이터 가중치</div>
      ${weightRows}
      ${cur && cur.confirmed_at ? `<div style="font-size:11px;color:#8b949e;margin-top:8px">전환: ${fmtKST(cur.confirmed_at)}</div>` : ""}
    `;
  } catch (err) {
    console.warn("[monitor] regime error:", err);
  }
}

// ── Positions ─────────────────────────────────────────────────────────────

async function updatePositions() {
  try {
    const data = await apiFetch("/api/internal/monitor/positions");
    const tbody = document.getElementById("positions-tbody");
    if (!tbody) return;

    if (!data.positions || data.positions.length === 0) {
      tbody.innerHTML = `<tr><td colspan="6" style="color:#484f58;text-align:center">포지션 없음</td></tr>`;
    } else {
      tbody.innerHTML = data.positions.map((p) => {
        const liqDist = p.liq_distance_pct;
        const liqClass = liqDist === null ? "" : liqDist < 5 ? "neg" : liqDist < 10 ? "warn" : "pos";
        const upnl = p.unrealized_pnl || 0;
        return `<tr>
          <td>${p.symbol}</td>
          <td><span class="badge badge-blue">${(p.side || "").toUpperCase()}</span></td>
          <td>${p.size || "—"}</td>
          <td>$${p.entry_price ? Number(p.entry_price).toLocaleString() : "—"}</td>
          <td class="${upnl >= 0 ? "pos" : "neg"}">${fmtUSD(upnl)}</td>
          <td class="${liqClass}">${liqDist !== null ? `${liqDist.toFixed(1)}%` : "—"}</td>
        </tr>`;
      }).join("");
    }
  } catch (err) {
    console.warn("[monitor] positions error:", err);
  }
}

// ── Service Health ────────────────────────────────────────────────────────

async function updateServiceHealth() {
  try {
    const data = await apiFetch("/api/internal/monitor/service");
    const grid = document.getElementById("health-grid");
    if (!grid) return;

    const staleLabel = (sec) => {
      if (sec === null || sec === undefined || !isFinite(sec)) return "응답 없음";
      if (sec < 60) return `${sec}초 전`;
      if (sec < 3600) return `${Math.round(sec / 60)}분 전`;
      return `${Math.round(sec / 3600)}시간 전`;
    };

    grid.innerHTML = (data.services || []).map((s) =>
      `<div class="health-card">
        <div class="health-dot ${s.status}"></div>
        <div>
          <div class="health-name">${s.service}</div>
          <div class="health-stale">${staleLabel(s.stale_sec)}${s.errors_6h > 0 ? ` · 오류 ${s.errors_6h}건` : ""}</div>
        </div>
      </div>`
    ).join("");
  } catch (err) {
    console.warn("[monitor] service health error:", err);
  }
}

// ── Infra ─────────────────────────────────────────────────────────────────

async function updateInfra() {
  try {
    const data = await apiFetch("/api/internal/monitor/infra");

    const items = [
      { label: "CPU",     val: data.cpu_pct,       status: data.cpu_status,       unit: "%" },
      { label: "메모리",  val: data.mem_pct,       status: data.mem_status,       unit: "%" },
      { label: "디스크",  val: data.disk_pct,      status: data.disk_status,      unit: "%" },
      { label: "Redis",   val: data.redis_mem_pct, status: data.redis_mem_status, unit: "%" },
    ];

    const grid = document.getElementById("infra-grid");
    if (!grid) return;

    grid.innerHTML = items.map(({ label, val, status, unit }) =>
      `<div class="infra-card">
        <div class="infra-label">${label}</div>
        <div class="infra-val ${status === "red" ? "neg" : status === "yellow" ? "warn" : "pos"}">
          ${val !== null ? `${val}${unit}` : "—"}
        </div>
        ${val !== null ? `<div class="infra-bar"><div class="infra-fill ${status}" style="width:${Math.min(val, 100)}%"></div></div>` : ""}
      </div>`
    ).join("");
  } catch (err) {
    console.warn("[monitor] infra error:", err);
  }
}

// ── Init & polling ────────────────────────────────────────────────────────

async function refreshAll() {
  await Promise.allSettled([
    updatePortfolio(),
    updateKillSwitch(),
    updateRegime(),
    updatePositions(),
    updateServiceHealth(),
    updateInfra(),
  ]);
  document.getElementById("last-updated").textContent =
    `갱신: ${new Date().toLocaleTimeString("ko-KR")}`;
}

refreshAll();
setInterval(refreshAll, 10_000);
