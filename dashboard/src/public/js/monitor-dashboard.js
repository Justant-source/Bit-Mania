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

// ── Infra gauges (circular) ───────────────────────────────────────────────

const GAUGE_STEPS = [
  { max: 60, color: "#3fb950" },   // 녹색
  { max: 75, color: "#e3b341" },   // 노랑
  { max: 90, color: "#e67e22" },   // 주황
  { max: 101, color: "#f85149" },  // 빨강
];

function gaugeColor(val) {
  if (val === null) return "#484f58";
  for (const s of GAUGE_STEPS) {
    if (val < s.max) return s.color;
  }
  return "#f85149";
}

function svgGauge(val, label, color) {
  const r = 36, cx = 50, cy = 52;
  const circ = 2 * Math.PI * r;
  const fill = val !== null ? Math.min(circ * (val / 100), circ) : 0;
  const gap  = circ - fill;
  const pct  = val !== null ? val.toFixed(1) : "—";
  const c    = val !== null ? color : "#30363d";

  return `<svg viewBox="0 0 100 104" width="108" height="108">
    <circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="#21262d" stroke-width="8"/>
    <circle cx="${cx}" cy="${cy}" r="${r}" fill="none"
      stroke="${c}" stroke-width="8"
      stroke-dasharray="${fill.toFixed(2)} ${gap.toFixed(2)}"
      stroke-linecap="round"
      transform="rotate(-90 ${cx} ${cy})"/>
    <text x="${cx}" y="${cy - 3}" text-anchor="middle" fill="#e6edf3"
      font-size="15" font-weight="700" font-family="'Segoe UI',system-ui">${pct}${val !== null ? "%" : ""}</text>
    <text x="${cx}" y="${cy + 13}" text-anchor="middle" fill="#8b949e"
      font-size="9" font-family="'Segoe UI',system-ui">${label}</text>
  </svg>`;
}

const GAUGE_META = [
  { key: "cpu",   label: "CPU",    apiKey: "cpu_pct" },
  { key: "mem",   label: "메모리", apiKey: "mem_pct" },
  { key: "disk",  label: "디스크", apiKey: "disk_pct" },
  { key: "redis", label: "Redis",  apiKey: "redis_mem_pct" },
];

async function updateInfra() {
  try {
    const data = await apiFetch("/api/internal/monitor/infra");
    const grid = document.getElementById("infra-grid");
    if (!grid) return;

    grid.innerHTML = GAUGE_META.map(({ key, label, apiKey }) => {
      const val   = data[apiKey] !== undefined ? data[apiKey] : null;
      const color = gaugeColor(val);
      const tier  = val === null ? "—" : val < 60 ? "정상" : val < 75 ? "주의" : val < 90 ? "경고" : "위험";
      return `<div class="gauge-card" data-key="${key}" data-label="${label}" title="클릭 시 24시간 추이">
        ${svgGauge(val, label, color)}
        <div class="gauge-tier" style="color:${color}">${tier}</div>
        <div class="gauge-hint">클릭 → 24h 추이</div>
      </div>`;
    }).join("");

    grid.querySelectorAll(".gauge-card").forEach((card) => {
      card.addEventListener("click", () => openInfraModal(card.dataset.key, card.dataset.label));
    });
  } catch (err) {
    console.warn("[monitor] infra error:", err);
  }
}

// ── Infra history modal ───────────────────────────────────────────────────

async function openInfraModal(key, label) {
  const overlay = document.getElementById("infra-modal");
  const chartDiv = document.getElementById("modal-chart");
  document.getElementById("modal-title").textContent = `${label} — 24시간 추이`;
  overlay.style.display = "flex";
  chartDiv.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:300px;color:#484f58">로딩 중…</div>`;
  document.getElementById("modal-last-update").textContent = "";

  try {
    const data = await apiFetch(`/api/internal/monitor/infra/history?key=${key}`);
    const pts  = data.history || [];

    if (pts.length === 0) {
      chartDiv.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:300px;color:#484f58">스냅샷 누적 중 (5분 주기 기록)</div>`;
      return;
    }

    const ts   = pts.map((p) => new Date(p.ts));
    const vals = pts.map((p) => p.val);
    const color = gaugeColor(vals[vals.length - 1]);

    Plotly.react(chartDiv, [{
      type: "scatter", mode: "lines+markers",
      x: ts, y: vals,
      name: label,
      line: { color, width: 2 },
      marker: { color, size: 4 },
      fill: "tozeroy",
      fillcolor: color + "22",
      hovertemplate: `%{x|%m/%d %H:%M}<br>${label}: %{y:.1f}%<extra></extra>`,
    }], {
      paper_bgcolor: "#161b22", plot_bgcolor: "#0d1117",
      font: { color: "#c9d1d9", size: 11 },
      xaxis: { gridcolor: "#21262d", color: "#c9d1d9", type: "date" },
      yaxis: { gridcolor: "#21262d", color: "#c9d1d9", range: [0, 100], ticksuffix: "%" },
      margin: { l: 45, r: 15, t: 15, b: 40 },
      autosize: true,
      shapes: [
        { type: "line", x0: ts[0], x1: ts[ts.length - 1], y0: 60, y1: 60, line: { color: "#e3b341", width: 1, dash: "dot" } },
        { type: "line", x0: ts[0], x1: ts[ts.length - 1], y0: 75, y1: 75, line: { color: "#e67e22", width: 1, dash: "dot" } },
        { type: "line", x0: ts[0], x1: ts[ts.length - 1], y0: 90, y1: 90, line: { color: "#f85149", width: 1, dash: "dot" } },
      ],
    }, { responsive: true, displayModeBar: false });

    const last = pts[pts.length - 1]?.ts;
    if (last) {
      document.getElementById("modal-last-update").textContent =
        `마지막 기록: ${new Date(last).toLocaleString("ko-KR", { timeZone: "Asia/Seoul" })}`;
    }
  } catch (err) {
    chartDiv.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:300px;color:#484f58">로딩 실패</div>`;
  }
}

document.getElementById("modal-close")?.addEventListener("click", () => {
  document.getElementById("infra-modal").style.display = "none";
});
document.getElementById("infra-modal")?.addEventListener("click", (e) => {
  if (e.target === e.currentTarget) e.currentTarget.style.display = "none";
});

// ── Init & polling ────────────────────────────────────────────────────────

async function refreshAll() {
  await Promise.allSettled([
    updatePortfolio(),
    updateKillSwitch(),
    updatePositions(),
    updateServiceHealth(),
    updateInfra(),
  ]);
  document.getElementById("last-updated").textContent =
    `갱신: ${new Date().toLocaleTimeString("ko-KR")}`;
}

refreshAll();
setInterval(refreshAll, 10_000);
