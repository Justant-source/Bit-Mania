"use strict";

// ── Design token helpers ──────────────────────────────────────────────────

const T = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

const palette = () => ({
  success: T('--c-success'),
  danger:  T('--c-danger'),
  warning: T('--c-warning'),
  info:    T('--c-info'),
  text:    T('--text'),
  muted:   T('--text-muted'),
  border:  T('--border'),
  surface: T('--surface'),
  bg:      T('--bg'),
});

const plotlyLayout = (extra) => {
  const p = palette();
  return Object.assign({
    paper_bgcolor: 'transparent',
    plot_bgcolor:  'transparent',
    font: { family: "'Inter', system-ui, sans-serif", color: p.text, size: 11 },
    xaxis: { gridcolor: p.border, linecolor: p.border, tickcolor: p.border, zerolinecolor: p.border, color: p.muted },
    yaxis: { gridcolor: p.border, linecolor: p.border, tickcolor: p.border, zerolinecolor: p.border, color: p.muted },
    legend: { bgcolor: 'transparent', font: { color: p.muted, size: 11 }, orientation: 'h', y: 1.08 },
    margin: { l: 55, r: 20, t: 30, b: 40 },
    autosize: true,
    hovermode: 'x unified',
  }, extra);
};

// ── State ─────────────────────────────────────────────────────────────────

let currentFrom = '2026-05-15';
let compareData = [];

function fromToDays(from) {
  if (/^\d{4}/.test(from)) {
    const diffMs = Date.now() - new Date(from).getTime();
    return Math.max(1, Math.ceil(diffMs / 86400000) + 1);
  }
  const m = from.match(/^(\d+)/);
  return m ? parseInt(m[1]) : 30;
}

// ── Utilities ─────────────────────────────────────────────────────────────

async function apiFetch(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`HTTP ${r.status}: ${path}`);
  return r.json();
}

function fmtKST(isoStr) {
  if (!isoStr) return '—';
  return new Date(isoStr).toLocaleString('ko-KR', {
    timeZone: 'Asia/Seoul',
    month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit',
  });
}

function fmtMs(ms) {
  if (ms === null || ms === undefined) return '—';
  if (ms < 60_000) return `${Math.round(ms / 1000)}초`;
  return `${Math.round(ms / 60_000)}분`;
}

function fmtPct(val, digits = 2) {
  if (val === null || val === undefined) return '—';
  return `${Number(val).toFixed(digits)}%`;
}

function fmtUSD(val) {
  if (val === null || val === undefined) return '—';
  return `$${Number(val).toFixed(2)}`;
}

function fmtBTC(val) {
  if (val === null || val === undefined) return '—';
  return `${Number(val).toFixed(5)} BTC`;
}

function setKpi(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

// ── Range selector ────────────────────────────────────────────────────────

document.querySelectorAll('[data-from]').forEach((btn) => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('[data-from]').forEach((b) => b.classList.remove('active'));
    btn.classList.add('active');
    currentFrom = btn.dataset.from;
    loadCharts();
    updateCompare();
  });
});

// ── Status (10s poll) ─────────────────────────────────────────────────────

async function updateStatus() {
  try {
    const data = await apiFetch('/api/internal/supertrend/status');

    const pos = data.live_position;
    if (pos) {
      const side = pos.side || 'LONG';
      const upnl = pos.unrealized_pnl;
      const posEl = document.getElementById('kpi-position');
      if (posEl) {
        posEl.textContent = `${side} ${fmtBTC(pos.size)}`;
        posEl.className = 'kpi-value ' + (side === 'LONG' ? 'pos' : 'neg');
      }
      const upnlEl = document.getElementById('kpi-position-sub');
      if (upnlEl) {
        upnlEl.textContent = `미실현 ${fmtUSD(upnl)}`;
        upnlEl.className = `kpi-meta ${upnl >= 0 ? 'pos' : 'neg'}`;
      }
    } else {
      setKpi('kpi-position', '없음');
      setKpi('kpi-position-sub', '포지션 없음');
    }

    const sig = data.latest_signal;
    if (sig) {
      const signalEl = document.getElementById('kpi-signal');
      if (signalEl) {
        if (sig.expected_action === 'enter') {
          signalEl.textContent = '진입';
          signalEl.className = 'kpi-value pos';
        } else if (sig.expected_action === 'exit') {
          signalEl.textContent = '종료';
          signalEl.className = 'kpi-value neg';
        } else {
          signalEl.textContent = '대기';
          signalEl.className = 'kpi-value';
        }
      }
      setKpi('kpi-capital', fmtUSD(sig.allocated_capital));
      renderIndicators(sig);
    }

    if (data.next_bar_eta_ms !== null) {
      const mins = Math.ceil(data.next_bar_eta_ms / 60_000);
      setKpi('kpi-nextbar', `${mins}분 후`);
      setKpi('kpi-nextbar-sub', fmtKST(data.next_bar_ts));
    }

    // Sidebar
    const sbPos = document.getElementById('sb-position');
    if (sbPos) sbPos.textContent = pos ? `${pos.side || 'LONG'} ${fmtBTC(pos.size)}` : '없음';
    const sbSig = document.getElementById('sb-signal');
    if (sbSig && sig) sbSig.textContent = sig.expected_action === 'enter' ? '진입' : sig.expected_action === 'exit' ? '종료' : '대기';
    const sbNext = document.getElementById('sb-nextbar');
    if (sbNext && data.next_bar_eta_ms !== null) sbNext.textContent = `${Math.ceil(data.next_bar_eta_ms / 60_000)}분 후`;

    setKpi('last-updated', `갱신: ${new Date().toLocaleTimeString('ko-KR')}`);
  } catch (err) {
    console.warn('[supertrend] status error:', err);
  }
}

function renderIndicators(sig) {
  const panel = document.getElementById('indicator-panel');
  if (!panel) return;
  const items = [
    { label: 'ST 방향',    val: sig.st_dir > 0 ? '상승' : '하락',       cls: sig.st_dir > 0 ? 'pos' : 'neg' },
    { label: 'EMA(7)',     val: Number(sig.fast_ema).toLocaleString(),   cls: '' },
    { label: 'EMA(27)',    val: Number(sig.slow_ema).toLocaleString(),   cls: '' },
    { label: 'EMA(230)',   val: Number(sig.dir_ema).toLocaleString(),    cls: '' },
    { label: 'BTC 가격',   val: `$${Number(sig.price).toLocaleString()}`, cls: '' },
    { label: 'ATR(14)',    val: Number(sig.atr_14).toLocaleString(),     cls: '' },
    { label: 'Stop Loss',  val: sig.expected_stop_loss ? `$${Number(sig.expected_stop_loss).toLocaleString()}` : '—', cls: 'warn' },
  ];
  panel.innerHTML = items.map(({ label, val, cls }) =>
    `<div class="kpi">
      <div class="kpi-label">${label}</div>
      <div class="kpi-value ${cls}" style="font-size:16px">${val}</div>
    </div>`
  ).join('');
}

// ── Compare (10s poll) ────────────────────────────────────────────────────

async function updateCompare() {
  try {
    const data = await apiFetch(`/api/internal/supertrend/compare?from=${currentFrom}`);
    compareData = data.comparison || [];

    const stats = data.stats || {};
    const matchPct = stats.total > 0 ? Math.round((stats.matched / stats.total) * 100) : 0;
    setKpi('kpi-match', `${matchPct}%`);
    setKpi('kpi-match-sub', `${stats.matched}/${stats.total} 매칭`);
    setKpi('kpi-slippage', stats.avg_slippage_pct !== undefined ? fmtPct(stats.avg_slippage_pct, 3) : '—');

    const titleEl = document.getElementById('compare-title');
    if (titleEl) titleEl.textContent = `총 ${stats.total}건 · 매칭 ${stats.matched} · 놓침 ${stats.missed} · 초과 ${stats.extra}`;

    const matchedBadge = document.getElementById('badge-matched');
    const extraBadge   = document.getElementById('badge-extra');
    const missedBadge  = document.getElementById('badge-missed');
    if (matchedBadge) matchedBadge.textContent = `매칭 ${stats.matched || 0}`;
    if (extraBadge)   extraBadge.textContent   = `초과 ${stats.extra || 0}`;
    if (missedBadge)  missedBadge.textContent  = `놓침 ${stats.missed || 0}`;

    renderCompareTable(compareData);
  } catch (err) {
    console.warn('[supertrend] compare error:', err);
  }
}

function renderCompareTable(rows) {
  const tbody = document.getElementById('compare-tbody');
  if (!tbody) return;
  if (!rows || rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="9" class="muted" style="text-align:center">비교 데이터 없음 (신호 기록 누적 중)</td></tr>`;
    return;
  }

  tbody.innerHTML = rows.map((r) => {
    const statusBadge = r.status === 'matched'
      ? '<span class="badge success">매칭</span>'
      : r.status === 'missed'
        ? '<span class="badge danger">놓침</span>'
        : '<span class="badge warning">초과</span>';
    const actionBadge = r.expected_action === 'enter'
      ? '<span class="badge success">진입</span>'
      : '<span class="badge danger">종료</span>';
    const slipAbs = r.slippage_pct !== null ? Math.abs(r.slippage_pct) : null;
    const slipClass = slipAbs !== null && slipAbs > 0.1 ? 'neg' : '';

    return `<tr>
      <td class="mono" style="font-size:12px">${fmtKST(r.bar_ts)}</td>
      <td>${actionBadge}</td>
      <td class="num cmp-expected">${r.expected_price ? fmtUSD(r.expected_price) : '—'}</td>
      <td class="num cmp-expected">${r.expected_qty ? fmtBTC(r.expected_qty) : '—'}</td>
      <td class="num cmp-actual">${r.actual_price ? fmtUSD(r.actual_price) : '—'}</td>
      <td class="num cmp-actual">${r.actual_qty ? fmtBTC(r.actual_qty) : '—'}</td>
      <td class="num cmp-diff">${fmtMs(r.timing_lag_ms)}</td>
      <td class="num ${slipClass}">${r.slippage_pct !== null ? fmtPct(r.slippage_pct, 3) : '—'}</td>
      <td>${statusBadge}</td>
    </tr>`;
  }).join('');
}

// ── Charts (5 min refresh) ────────────────────────────────────────────────

async function loadCharts() {
  await Promise.allSettled([renderPriceChart(), renderEquityChart()]);
}

async function renderPriceChart() {
  const div = document.getElementById('chart-price');
  if (!div) return;

  try {
    const p = palette();
    const [candlesData, expData, actData] = await Promise.all([
      apiFetch(`/api/internal/supertrend/candles?days=${fromToDays(currentFrom)}`),
      apiFetch(`/api/internal/supertrend/expected?from=${currentFrom}`),
      apiFetch(`/api/internal/supertrend/actual?from=${currentFrom}`),
    ]);

    const candles  = candlesData.candles || [];
    const expected = (expData.signals || []).filter((s) => s.expected_action !== 'hold');
    const actual   = actData.trades || [];

    if (candles.length === 0) {
      div.innerHTML = `<div class="empty-state">OHLCV 데이터 없음</div>`;
      return;
    }

    const ts = candles.map((c) => new Date(c.ts));
    const traces = [
      {
        type: 'candlestick',
        x: ts,
        open:  candles.map((c) => c.open),
        high:  candles.map((c) => c.high),
        low:   candles.map((c) => c.low),
        close: candles.map((c) => c.close),
        name: 'BTC 4h',
        increasing: { line: { color: p.success } },
        decreasing: { line: { color: p.danger } },
        showlegend: false,
        hoverinfo: 'x+y',
      },
    ];

    const expEntries = expected.filter((s) => s.expected_action === 'enter');
    if (expEntries.length) {
      traces.push({
        type: 'scatter', mode: 'markers',
        x: expEntries.map((s) => new Date(s.bar_ts)),
        y: expEntries.map((s) => parseFloat(s.price) * 0.997),
        name: '예상 진입',
        marker: { color: p.info, symbol: 'triangle-up', size: 10 },
        hovertemplate: '예상 진입 %{x|%m/%d %H:%M}<br>가격 %{y:$,.0f}<extra></extra>',
      });
    }

    const expExits = expected.filter((s) => s.expected_action === 'exit');
    if (expExits.length) {
      traces.push({
        type: 'scatter', mode: 'markers',
        x: expExits.map((s) => new Date(s.bar_ts)),
        y: expExits.map((s) => parseFloat(s.price) * 1.003),
        name: '예상 종료',
        marker: { color: p.info, symbol: 'triangle-down', size: 10 },
        hovertemplate: '예상 종료 %{x|%m/%d %H:%M}<br>가격 %{y:$,.0f}<extra></extra>',
      });
    }

    const actEntries = actual.filter((t) => t.entry_price);
    if (actEntries.length) {
      traces.push({
        type: 'scatter', mode: 'markers',
        x: actEntries.map((t) => new Date(t.entry_fill_time || t.entry_time)),
        y: actEntries.map((t) => t.entry_price * 0.994),
        name: '실제 체결(진입)',
        marker: { color: p.success, symbol: 'triangle-up-open', size: 12, line: { width: 2, color: p.success } },
        hovertemplate: '실제 진입 %{x|%m/%d %H:%M}<br>체결가 %{y:$,.0f}<extra></extra>',
      });
    }

    const actExits = actual.filter((t) => t.exit_price);
    if (actExits.length) {
      traces.push({
        type: 'scatter', mode: 'markers',
        x: actExits.map((t) => new Date(t.exit_fill_time || t.exit_time)),
        y: actExits.map((t) => t.exit_price * 1.006),
        name: '실제 체결(종료)',
        marker: { color: p.warning, symbol: 'triangle-down-open', size: 12, line: { width: 2, color: p.warning } },
        hovertemplate: '실제 종료 %{x|%m/%d %H:%M}<br>체결가 %{y:$,.0f}<extra></extra>',
      });
    }

    Plotly.react(div, traces, plotlyLayout({
      height: 340,
      xaxis: { type: 'date', rangeslider: { visible: false } },
      yaxis: { title: 'BTC (USDT)', tickformat: '$,.0f' },
    }), { responsive: true, displayModeBar: false });
    div.querySelector('.empty-state')?.remove();
  } catch (err) {
    console.error('[supertrend] price chart error:', err);
    div.innerHTML = `<div class="empty-state">차트 로딩 실패</div>`;
  }
}

async function renderEquityChart() {
  const div = document.getElementById('chart-equity');
  if (!div) return;

  try {
    const data = await apiFetch(`/api/internal/supertrend/equity?days=${fromToDays(currentFrom)}`);
    const exp = data.expected || [];
    const act = data.actual   || [];
    const p   = palette();

    if (exp.length === 0 && act.length === 0) {
      div.innerHTML = `<div class="empty-state">자산 데이터 없음</div>`;
      return;
    }

    const traces = [];
    if (exp.length > 0) {
      traces.push({
        type: 'scatter', mode: 'lines',
        x: exp.map((d) => new Date(d.ts)),
        y: exp.map((d) => d.equity),
        name: '예상 자산',
        line: { color: p.info, width: 2, dash: 'dot' },
        hovertemplate: '예상 %{y:$,.2f}<extra></extra>',
      });
    }
    if (act.length > 0) {
      traces.push({
        type: 'scatter', mode: 'lines',
        x: act.map((d) => new Date(d.ts)),
        y: act.map((d) => d.equity),
        name: '실제 자산',
        line: { color: p.success, width: 2 },
        fill: 'tozeroy',
        fillcolor: p.success + '18',
        hovertemplate: '실제 %{y:$,.2f}<extra></extra>',
      });
    }

    Plotly.react(div, traces, plotlyLayout({
      height: 230,
      yaxis: { title: '자산 (USD)', tickformat: '$,.0f' },
      xaxis: { type: 'date' },
    }), { responsive: true, displayModeBar: false });
    div.querySelector('.empty-state')?.remove();
  } catch (err) {
    console.error('[supertrend] equity chart error:', err);
    div.innerHTML = `<div class="empty-state">차트 로딩 실패</div>`;
  }
}

// ── Init & polling ────────────────────────────────────────────────────────

async function init() {
  await Promise.allSettled([updateStatus(), updateCompare(), loadCharts()]);
}

init();
setInterval(() => { updateStatus(); updateCompare(); }, 10_000);
setInterval(loadCharts, 5 * 60_000);

// Re-render charts when theme changes
window.addEventListener('bm:themechange', loadCharts);
