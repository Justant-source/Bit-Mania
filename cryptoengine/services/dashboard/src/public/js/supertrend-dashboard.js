"use strict";

// ── Theme constants (from 7-strategies dashboard) ─────────────────────────
const BG      = "#0d1117";
const CARD_BG = "#161b22";
const GRID    = "#21262d";
const TEXT    = "#c9d1d9";
const PROFIT  = "#3fb950";
const LOSS    = "#f85149";
const PRIMARY = "#1f6feb";
const BTC_CLR = "#f7931a";
const EXP_CLR = "#58a6ff";  // expected: blue
const ACT_CLR = "#3fb950";  // actual:   green

function darkLayout(extra) {
  return Object.assign({
    paper_bgcolor: CARD_BG,
    plot_bgcolor:  BG,
    font:   { color: TEXT, size: 11 },
    xaxis:  { gridcolor: GRID, zerolinecolor: GRID, color: TEXT },
    yaxis:  { gridcolor: GRID, zerolinecolor: GRID, color: TEXT },
    legend: { bgcolor: "rgba(0,0,0,0)", bordercolor: GRID, borderwidth: 1, orientation: "h", y: 1.08 },
    margin: { l: 55, r: 20, t: 30, b: 40 },
    autosize: true,
    hovermode: "x unified",
  }, extra);
}

// ── State ─────────────────────────────────────────────────────────────────
let currentDays = 30;
let compareData = [];

// ── Helpers ───────────────────────────────────────────────────────────────

async function apiFetch(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`HTTP ${r.status}: ${path}`);
  return r.json();
}

function fmtKST(isoStr) {
  if (!isoStr) return "—";
  return new Date(isoStr).toLocaleString("ko-KR", {
    timeZone: "Asia/Seoul",
    month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit",
  });
}

function fmtMs(ms) {
  if (ms === null || ms === undefined) return "—";
  if (ms < 60_000) return `${Math.round(ms / 1000)}초`;
  return `${Math.round(ms / 60_000)}분`;
}

function fmtPct(val, digits = 2) {
  if (val === null || val === undefined) return "—";
  return `${Number(val).toFixed(digits)}%`;
}

function fmtUSD(val) {
  if (val === null || val === undefined) return "—";
  return `$${Number(val).toFixed(2)}`;
}

function fmtBTC(val) {
  if (val === null || val === undefined) return "—";
  return `${Number(val).toFixed(5)} BTC`;
}

function setKpi(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

// ── Day-range selector ────────────────────────────────────────────────────

document.querySelectorAll(".day-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".day-btn").forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    currentDays = parseInt(btn.dataset.days, 10);
    loadCharts();
  });
});

// ── Status (10s poll) ─────────────────────────────────────────────────────

async function updateStatus() {
  try {
    const data = await apiFetch("/api/internal/supertrend/status");

    // Position
    const pos = data.live_position;
    if (pos) {
      const side = pos.side || "LONG";
      const upnl = pos.unrealized_pnl;
      setKpi("kpi-position", `${side} ${fmtBTC(pos.size)}`);
      const upnlEl = document.getElementById("kpi-position-sub");
      if (upnlEl) {
        upnlEl.textContent = `미실현 ${fmtUSD(upnl)}`;
        upnlEl.className = `kpi-sub ${upnl >= 0 ? "pos" : "neg"}`;
      }
    } else {
      setKpi("kpi-position", "없음");
      setKpi("kpi-position-sub", "포지션 없음");
    }

    // Signal
    const sig = data.latest_signal;
    if (sig) {
      const signalText = sig.expected_action === "enter" ? "🟢 진입 신호" :
                         sig.expected_action === "exit"  ? "🔴 청산 신호" : "⚪ 대기";
      setKpi("kpi-signal", signalText);
      setKpi("kpi-signal-sub", `ST${sig.st_dir > 0 ? "↑" : "↓"} 가격 ${Number(sig.price).toLocaleString()}`);

      // Capital
      setKpi("kpi-capital", fmtUSD(sig.allocated_capital));

      // Indicators panel
      renderIndicators(sig);
    }

    // Next bar
    if (data.next_bar_eta_ms !== null) {
      const mins = Math.ceil(data.next_bar_eta_ms / 60_000);
      setKpi("kpi-nextbar", `${mins}분 후`);
      setKpi("kpi-nextbar-sub", fmtKST(data.next_bar_ts));
    }

    document.getElementById("last-updated").textContent =
      `갱신: ${new Date().toLocaleTimeString("ko-KR")}`;
  } catch (err) {
    console.warn("[supertrend] status error:", err);
  }
}

function renderIndicators(sig) {
  const panel = document.getElementById("indicator-panel");
  if (!panel) return;
  const items = [
    { label: "ST 방향",    val: sig.st_dir > 0 ? "🟢 상승" : "🔴 하락",  raw: null },
    { label: "EMA(7)",     val: Number(sig.fast_ema).toLocaleString(),   raw: null },
    { label: "EMA(27)",    val: Number(sig.slow_ema).toLocaleString(),   raw: null },
    { label: "EMA(230)",   val: Number(sig.dir_ema).toLocaleString(),    raw: null },
    { label: "BTC 가격",   val: `$${Number(sig.price).toLocaleString()}`, raw: null },
    { label: "ATR(14)",    val: Number(sig.atr_14).toLocaleString(),     raw: null },
    { label: "예상 진입가", val: sig.expected_stop_loss ? `SL $${Number(sig.expected_stop_loss).toLocaleString()}` : "—", raw: null },
  ];
  panel.innerHTML = items.map(({ label, val }) =>
    `<div class="kpi-card"><div class="kpi-label">${label}</div><div class="kpi-value" style="font-size:16px">${val}</div></div>`
  ).join("");
}

// ── Compare (10s poll) ────────────────────────────────────────────────────

async function updateCompare() {
  try {
    const data = await apiFetch(`/api/internal/supertrend/compare?from=${currentDays} days`);
    compareData = data.comparison || [];

    const stats = data.stats || {};
    const matchPct = stats.total > 0 ? Math.round((stats.matched / stats.total) * 100) : 0;
    setKpi("kpi-match", `${matchPct}%`);
    setKpi("kpi-match-sub", `${stats.matched}/${stats.total} 매칭`);
    setKpi("kpi-slippage", stats.avg_slippage_pct !== undefined ? fmtPct(stats.avg_slippage_pct, 3) : "—");
    document.getElementById("compare-title").textContent =
      `총 ${stats.total}건 · 매칭 ${stats.matched} · 놓침 ${stats.missed} · 초과 ${stats.extra}`;

    renderCompareTable(compareData);
  } catch (err) {
    console.warn("[supertrend] compare error:", err);
  }
}

function renderCompareTable(rows) {
  const tbody = document.getElementById("compare-tbody");
  if (!tbody) return;
  if (!rows || rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="9" style="color:#484f58;text-align:center">비교 데이터 없음 (신호 기록 누적 중)</td></tr>`;
    return;
  }

  tbody.innerHTML = rows.map((r) => {
    const statusClass = r.status === "matched" ? "cmp-matched" :
                        r.status === "missed"  ? "cmp-miss" : "cmp-extra";
    const statusLabel = r.status === "matched" ? "✅ 매칭" :
                        r.status === "missed"  ? "❌ 놓침" : "⚠️ 초과";
    const actionLabel = r.expected_action === "enter" ? "🟢 진입" : "🔴 청산";
    const slipColor = r.slippage_pct !== null && Math.abs(r.slippage_pct) > 0.1 ? LOSS : TEXT;

    return `<tr class="${r.status === "matched" ? "" : ""}">
      <td>${fmtKST(r.bar_ts)}</td>
      <td>${actionLabel}</td>
      <td class="cmp-expected">${r.expected_price ? fmtUSD(r.expected_price) : "—"}</td>
      <td class="cmp-expected">${r.expected_qty ? fmtBTC(r.expected_qty) : "—"}</td>
      <td class="cmp-actual">${r.actual_price ? fmtUSD(r.actual_price) : "—"}</td>
      <td class="cmp-actual">${r.actual_qty ? fmtBTC(r.actual_qty) : "—"}</td>
      <td class="cmp-diff">${fmtMs(r.timing_lag_ms)}</td>
      <td style="color:${slipColor}">${r.slippage_pct !== null ? fmtPct(r.slippage_pct, 3) : "—"}</td>
      <td class="${statusClass}">${statusLabel}</td>
    </tr>`;
  }).join("");
}

// ── Charts (5 min refresh) ────────────────────────────────────────────────

async function loadCharts() {
  await Promise.allSettled([renderPriceChart(), renderEquityChart()]);
}

async function renderPriceChart() {
  const div = document.getElementById("chart-price");
  if (!div) return;

  try {
    const [candlesData, expData, actData] = await Promise.all([
      apiFetch(`/api/internal/supertrend/candles?days=${currentDays}`),
      apiFetch(`/api/internal/supertrend/expected?from=${currentDays} days`),
      apiFetch(`/api/internal/supertrend/actual?from=${currentDays} days`),
    ]);

    const candles = candlesData.candles || [];
    const expected = (expData.signals || []).filter((s) => s.expected_action !== "hold");
    const actual = actData.trades || [];

    if (candles.length === 0) {
      div.innerHTML = `<div class="empty-state">OHLCV 데이터 없음</div>`;
      return;
    }

    const ts = candles.map((c) => new Date(c.ts));

    const traces = [
      // Candlestick
      {
        type: "candlestick",
        x: ts,
        open: candles.map((c) => c.open),
        high: candles.map((c) => c.high),
        low: candles.map((c) => c.low),
        close: candles.map((c) => c.close),
        name: "BTC 4h",
        increasing: { line: { color: PROFIT } },
        decreasing: { line: { color: LOSS } },
        showlegend: false,
        hoverinfo: "x+y",
      },
    ];

    // Expected entries (blue ▲)
    const expEntries = expected.filter((s) => s.expected_action === "enter");
    if (expEntries.length) {
      traces.push({
        type: "scatter", mode: "markers",
        x: expEntries.map((s) => new Date(s.bar_ts)),
        y: expEntries.map((s) => parseFloat(s.price) * 0.997),
        name: "예상 진입",
        marker: { color: EXP_CLR, symbol: "triangle-up", size: 10 },
        hovertemplate: "예상 진입 %{x|%m/%d %H:%M}<br>가격 %{y:$,.0f}<extra></extra>",
      });
    }

    // Expected exits (blue ▼)
    const expExits = expected.filter((s) => s.expected_action === "exit");
    if (expExits.length) {
      traces.push({
        type: "scatter", mode: "markers",
        x: expExits.map((s) => new Date(s.bar_ts)),
        y: expExits.map((s) => parseFloat(s.price) * 1.003),
        name: "예상 청산",
        marker: { color: EXP_CLR, symbol: "triangle-down", size: 10 },
        hovertemplate: "예상 청산 %{x|%m/%d %H:%M}<br>가격 %{y:$,.0f}<extra></extra>",
      });
    }

    // Actual entries (green ▲)
    const actEntries = actual.filter((t) => t.entry_price);
    if (actEntries.length) {
      traces.push({
        type: "scatter", mode: "markers",
        x: actEntries.map((t) => new Date(t.entry_fill_time || t.entry_time)),
        y: actEntries.map((t) => t.entry_price * 0.994),
        name: "실제 체결(진입)",
        marker: { color: ACT_CLR, symbol: "triangle-up-open", size: 12, line: { width: 2, color: ACT_CLR } },
        hovertemplate: "실제 진입 %{x|%m/%d %H:%M}<br>체결가 %{y:$,.0f}<extra></extra>",
      });
    }

    // Actual exits (orange ▼)
    const actExits = actual.filter((t) => t.exit_price);
    if (actExits.length) {
      traces.push({
        type: "scatter", mode: "markers",
        x: actExits.map((t) => new Date(t.exit_fill_time || t.exit_time)),
        y: actExits.map((t) => t.exit_price * 1.006),
        name: "실제 체결(청산)",
        marker: { color: BTC_CLR, symbol: "triangle-down-open", size: 12, line: { width: 2, color: BTC_CLR } },
        hovertemplate: "실제 청산 %{x|%m/%d %H:%M}<br>체결가 %{y:$,.0f}<extra></extra>",
      });
    }

    const layout = darkLayout({
      height: 340,
      xaxis: { type: "date", rangeslider: { visible: false } },
      yaxis: { title: "BTC (USDT)", tickformat: "$,.0f" },
    });

    Plotly.react(div, traces, layout, { responsive: true, displayModeBar: false });
    div.querySelector(".empty-state")?.remove();
  } catch (err) {
    console.error("[supertrend] price chart error:", err);
    div.innerHTML = `<div class="empty-state">차트 로딩 실패</div>`;
  }
}

async function renderEquityChart() {
  const div = document.getElementById("chart-equity");
  if (!div) return;

  try {
    const data = await apiFetch(`/api/internal/supertrend/equity?days=${currentDays}`);
    const exp = data.expected || [];
    const act = data.actual || [];

    if (exp.length === 0 && act.length === 0) {
      div.innerHTML = `<div class="empty-state">자산 데이터 없음</div>`;
      return;
    }

    const traces = [];

    if (exp.length > 0) {
      traces.push({
        type: "scatter", mode: "lines",
        x: exp.map((d) => new Date(d.ts)),
        y: exp.map((d) => d.equity),
        name: "예상 자산",
        line: { color: EXP_CLR, width: 2, dash: "dot" },
        hovertemplate: "예상 %{y:$,.2f}<extra></extra>",
      });
    }

    if (act.length > 0) {
      traces.push({
        type: "scatter", mode: "lines",
        x: act.map((d) => new Date(d.ts)),
        y: act.map((d) => d.equity),
        name: "실제 자산",
        line: { color: ACT_CLR, width: 2 },
        hovertemplate: "실제 %{y:$,.2f}<extra></extra>",
      });
    }

    const layout = darkLayout({
      height: 230,
      yaxis: { title: "자산 (USD)", tickformat: "$,.0f" },
      xaxis: { type: "date" },
    });

    Plotly.react(div, traces, layout, { responsive: true, displayModeBar: false });
    div.querySelector(".empty-state")?.remove();
  } catch (err) {
    console.error("[supertrend] equity chart error:", err);
    div.innerHTML = `<div class="empty-state">차트 로딩 실패</div>`;
  }
}

// ── Init & polling ────────────────────────────────────────────────────────

async function init() {
  await Promise.allSettled([updateStatus(), updateCompare(), loadCharts()]);
}

init();

// 10s: status + compare table
setInterval(() => { updateStatus(); updateCompare(); }, 10_000);

// 5min: charts
setInterval(loadCharts, 5 * 60_000);
