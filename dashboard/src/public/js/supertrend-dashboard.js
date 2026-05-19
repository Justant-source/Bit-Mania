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
    legend: { bgcolor: 'transparent', font: { color: p.muted, size: 10 }, orientation: 'h', y: 1.06, x: 0 },
    margin: { l: 55, r: 20, t: 30, b: 40 },
    autosize: true,
    hovermode: 'x unified',
  }, extra);
};

// ── Strategy params (SSOT: cryptoengine/config/strategies/supertrend.yaml) ──

// st_factor=2.4 st_period=8 | EMA 7/27/230 | atr_mult=3.2 (ATR(14) exit distance)
const ST = { factor: 2.4, period: 8, fastEma: 7, slowEma: 27, dirEma: 230, atrMult: 3.2 };

// ── Indicator helpers (port of cryptoengine/services/strategies/supertrend/indicators.py) ──

function computeEma(values, len) {
  const k = 2 / (len + 1);
  const result = new Array(values.length).fill(null);
  let sum = 0, count = 0, seedIdx = -1;
  for (let i = 0; i < values.length; i++) {
    const v = parseFloat(values[i]);
    if (isNaN(v)) continue;
    sum += v; count++;
    if (count === len) { result[i] = sum / len; seedIdx = i; break; }
  }
  if (seedIdx === -1) return result;
  for (let i = seedIdx + 1; i < values.length; i++) {
    const v = parseFloat(values[i]);
    result[i] = isNaN(v) ? result[i - 1] : v * k + result[i - 1] * (1 - k);
  }
  return result;
}

function atrWilder(high, low, close, period) {
  const n = close.length;
  const tr = new Array(n).fill(null);
  for (let i = 1; i < n; i++) {
    const h = parseFloat(high[i]), l = parseFloat(low[i]), pc = parseFloat(close[i - 1]);
    tr[i] = Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
  }
  const atr = new Array(n).fill(null);
  let sum = 0, count = 0, start = -1;
  for (let i = 1; i < n; i++) {
    if (tr[i] === null) continue;
    sum += tr[i]; count++;
    if (count === period) { atr[i] = sum / period; start = i; break; }
  }
  if (start === -1) return atr;
  for (let i = start + 1; i < n; i++) {
    if (tr[i] !== null) atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period;
  }
  return atr;
}

// Sticky supertrend (exact logic from indicators.py compute_supertrend).
// Returns [{ts, trend(+1/-1), band}] where band = lower band when uptrend, upper band when downtrend.
function computeSupertrend(candles, factor, period) {
  const n     = candles.length;
  const high  = candles.map(c => parseFloat(c.high));
  const low   = candles.map(c => parseFloat(c.low));
  const close = candles.map(c => parseFloat(c.close));
  const atr   = atrWilder(high, low, close, period);

  const final_ub = new Array(n).fill(null);
  const final_lb = new Array(n).fill(null);
  const trend    = new Array(n).fill(1);

  const seedIdx = atr.findIndex(v => v !== null);
  if (seedIdx === -1) return candles.map(c => ({ ts: c.ts, trend: 1, band: null }));

  for (let i = seedIdx; i < n; i++) {
    const hl2 = (high[i] + low[i]) / 2;
    const bub = hl2 + factor * atr[i];
    const blb = hl2 - factor * atr[i];

    // Upper band: tighten (lower) OR reset when previous close was above it (Pine Script rule)
    if (i === seedIdx || final_ub[i - 1] === null) {
      final_ub[i] = bub;
    } else if (bub < final_ub[i - 1] || close[i - 1] > final_ub[i - 1]) {
      final_ub[i] = bub;
    } else {
      final_ub[i] = final_ub[i - 1];
    }

    // Lower band: tighten (raise) OR reset when previous close was below it (Pine Script rule)
    if (i === seedIdx || final_lb[i - 1] === null) {
      final_lb[i] = blb;
    } else if (blb > final_lb[i - 1] || close[i - 1] < final_lb[i - 1]) {
      final_lb[i] = blb;
    } else {
      final_lb[i] = final_lb[i - 1];
    }

    // Trend: sticky — only flips when close clearly breaks the opposite band
    if (i === seedIdx) {
      trend[i] = 1;
    } else if (close[i] <= final_lb[i]) {
      trend[i] = -1;
    } else if (close[i] >= final_ub[i]) {
      trend[i] = 1;
    } else {
      trend[i] = trend[i - 1];
    }
  }

  return candles.map((c, i) => ({
    ts:    c.ts,
    trend: trend[i],
    band:  final_ub[i] === null ? null : (trend[i] === 1 ? final_lb[i] : final_ub[i]),
  }));
}

// ── State ─────────────────────────────────────────────────────────────────

let currentFrom = '2026-05-15';
let compareData = [];
let _signalMap  = new Map();
let _compareMap = new Map();

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
      setKpi('kpi-capital', fmtUSD(data.total_equity ?? sig.allocated_capital));
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

// ── Price chart (Supertrend band + EMA 3선 + 마커 + 청산선) ─────────────

async function renderPriceChart() {
  const div = document.getElementById('chart-price');
  if (!div) return;

  try {
    const p = palette();
    const CANDLES_FROM = '2026-01-01';
    const SIGNALS_FROM = '2026-05-18';

    const [candlesData, expData, cmpData] = await Promise.all([
      apiFetch(`/api/internal/supertrend/candles?from=${CANDLES_FROM}`),
      apiFetch(`/api/internal/supertrend/expected?from=${SIGNALS_FROM}`),
      apiFetch(`/api/internal/supertrend/compare?from=${SIGNALS_FROM}`),
    ]);

    const candles = candlesData.candles  || [];
    const signals = expData.signals      || [];
    const cmpRows = cmpData.comparison   || [];

    if (candles.length === 0) {
      div.innerHTML = `<div class="empty-state">OHLCV 데이터 없음</div>`;
      return;
    }

    // Store for click handler
    _signalMap  = new Map(signals.map(s => [new Date(s.bar_ts).getTime(), s]));
    _compareMap = new Map(cmpRows.map(r => [new Date(r.bar_ts).getTime(), r]));

    // ── Compute indicators ─────────────────────────────────────────
    const closes = candles.map(c => parseFloat(c.close));
    const highs  = candles.map(c => parseFloat(c.high));
    const lows   = candles.map(c => parseFloat(c.low));
    const ts     = candles.map(c => new Date(c.ts));

    const ema7   = computeEma(closes, ST.fastEma);
    const ema27  = computeEma(closes, ST.slowEma);
    const ema230 = computeEma(closes, ST.dirEma);
    const stData = computeSupertrend(candles, ST.factor, ST.period);

    // Supertrend band: two series (null where trend is the other type → connectgaps:false)
    const stUpBand = stData.map(s => s.trend ===  1 ? s.band : null);
    const stDnBand = stData.map(s => s.trend === -1 ? s.band : null);

    // ── ATR exit condition lines (while had_position=true) ─────────
    const atrUpTs = [], atrUpY = [];
    const atrDnTs = [], atrDnY = [];
    let entryPx = null, wasPos = false;

    for (const s of signals) {
      const t = new Date(s.bar_ts);
      if (s.expected_action === 'enter') entryPx = parseFloat(s.price);
      if (s.had_position && entryPx !== null) {
        const dist = parseFloat(s.atr_14) * ST.atrMult;
        atrUpTs.push(t); atrUpY.push(+(entryPx + dist).toFixed(2));
        atrDnTs.push(t); atrDnY.push(+(entryPx - dist).toFixed(2));
        wasPos = true;
      } else if (wasPos && !s.had_position) {
        atrUpTs.push(null); atrUpY.push(null);
        atrDnTs.push(null); atrDnY.push(null);
        wasPos = false; entryPx = null;
      }
    }

    // ── Build signal markers ───────────────────────────────────────
    const mxs = [], mys = [], msym = [], mcol = [], mdata = [], msz = [];
    for (const s of signals) {
      if (s.expected_action !== 'enter' && s.expected_action !== 'exit') continue;
      const key    = new Date(s.bar_ts).getTime();
      const cmp    = _compareMap.get(key);
      const status = cmp ? cmp.status : 'missed';
      const enter  = s.expected_action === 'enter';
      const filled = status === 'matched';
      const base   = enter ? 'triangle-up' : 'triangle-down';
      mxs.push(new Date(s.bar_ts));
      mys.push(parseFloat(s.price) * (enter ? 0.997 : 1.003));
      msym.push(filled ? base : `${base}-open`);
      mcol.push(enter ? p.success : p.danger);
      mdata.push(s.bar_ts);
      msz.push(filled ? 14 : 12);
    }

    // Extra actual fills (no matching expected signal)
    const extras = cmpRows.filter(r => r.status === 'extra');

    // ── Assemble traces ────────────────────────────────────────────
    const traces = [];

    // 1. Candlestick
    traces.push({
      type: 'candlestick', x: ts,
      open:  candles.map(c => parseFloat(c.open)),
      high:  highs, low: lows, close: closes,
      name: 'BTC 4h',
      increasing: { line: { color: p.success } },
      decreasing: { line: { color: p.danger } },
      showlegend: false, hoverinfo: 'x+y',
    });

    // 2. EMA 230 (장기 방향, orange-ish via gauge token)
    traces.push({
      type: 'scatter', mode: 'lines', x: ts, y: ema230, name: 'EMA 230',
      line: { color: T('--c-gauge-orange'), width: 1 },
      hovertemplate: 'EMA230 %{y:$,.0f}<extra></extra>',
    });

    // 3. EMA 27 (중기)
    traces.push({
      type: 'scatter', mode: 'lines', x: ts, y: ema27, name: 'EMA 27',
      line: { color: p.info, width: 1 },
      hovertemplate: 'EMA27 %{y:$,.0f}<extra></extra>',
    });

    // 4. EMA 7 (단기)
    traces.push({
      type: 'scatter', mode: 'lines', x: ts, y: ema7, name: 'EMA 7',
      line: { color: p.muted, width: 1 },
      hovertemplate: 'EMA7 %{y:$,.0f}<extra></extra>',
    });

    // 5. Supertrend 상승 (green, below price)
    traces.push({
      type: 'scatter', mode: 'lines', x: ts, y: stUpBand, name: 'ST 상승',
      line: { color: p.success, width: 2 },
      connectgaps: false,
      hovertemplate: 'ST 상승 %{y:$,.0f}<extra></extra>',
    });

    // 6. Supertrend 하락 (red, above price)
    traces.push({
      type: 'scatter', mode: 'lines', x: ts, y: stDnBand, name: 'ST 하락',
      line: { color: p.danger, width: 2 },
      connectgaps: false,
      hovertemplate: 'ST 하락 %{y:$,.0f}<extra></extra>',
    });

    // 7. ATR exit upper (익절, while in position)
    if (atrUpTs.length > 0) {
      traces.push({
        type: 'scatter', mode: 'lines', x: atrUpTs, y: atrUpY, name: 'ATR 익절',
        line: { color: T('--c-purple'), width: 1.5 },
        connectgaps: false,
        hovertemplate: 'ATR 익절↑ %{y:$,.0f}<extra></extra>',
      });
    }

    // 8. ATR exit lower (손절, no legend duplication)
    if (atrDnTs.length > 0) {
      traces.push({
        type: 'scatter', mode: 'lines', x: atrDnTs, y: atrDnY,
        line: { color: T('--c-purple'), width: 1.5 },
        connectgaps: false, showlegend: false,
        hovertemplate: 'ATR 손절↓ %{y:$,.0f}<extra></extra>',
      });
    }

    // 10. Signal markers
    if (mxs.length > 0) {
      traces.push({
        type: 'scatter', mode: 'markers',
        x: mxs, y: mys, name: '신호 (클릭)',
        marker: {
          symbol: msym, color: mcol, size: msz,
          line: { width: 2, color: mcol },
        },
        customdata: mdata,
        hovertemplate: '신호 %{x|%m/%d %H:%M} %{y:$,.0f}<extra></extra>',
      });
    }

    // 11. Extra fills (actual with no expected signal)
    if (extras.length > 0) {
      traces.push({
        type: 'scatter', mode: 'markers',
        x: extras.map(r => new Date(r.actual_fill_time || r.bar_ts)),
        y: extras.map(r => parseFloat(r.actual_price || 0)),
        name: '초과 체결',
        marker: { symbol: 'diamond-open', color: p.warning, size: 10, line: { width: 2, color: p.warning } },
        customdata: extras.map(r => r.bar_ts),
        hovertemplate: '초과 체결 %{x|%m/%d %H:%M} %{y:$,.0f}<extra></extra>',
      });
    }

    // Initial visible window: last ~30 days (180 4h bars), pan left for history
    const WINDOW = 180;
    const initX0 = ts.length > WINDOW ? ts[ts.length - WINDOW] : ts[0];
    const initX1 = ts[ts.length - 1];

    // y-axis auto-follow for visible x window (TradingView-style pan)
    function fitYToVisible() {
      if (div._yFitting) return;
      const layout = div._fullLayout;
      if (!layout) return;
      const xrange = layout.xaxis.range;
      if (!xrange || xrange.length < 2) return;
      const x0 = new Date(xrange[0]).getTime();
      const x1 = new Date(xrange[1]).getTime();
      let lo = Infinity, hi = -Infinity;
      for (let i = 0; i < ts.length; i++) {
        const t = ts[i].getTime();
        if (t < x0 || t > x1) continue;
        if (lows[i]  < lo) lo = lows[i];
        if (highs[i] > hi) hi = highs[i];
        if (ema230[i] != null) { if (ema230[i] < lo) lo = ema230[i]; if (ema230[i] > hi) hi = ema230[i]; }
      }
      // Include ATR 익절/손절 lines so they're never clipped off-screen
      for (let i = 0; i < atrUpTs.length; i++) {
        if (!atrUpTs[i]) continue;
        const t = atrUpTs[i].getTime();
        if (t < x0 || t > x1) continue;
        if (atrUpY[i] != null && atrUpY[i] > hi) hi = atrUpY[i];
        if (atrDnY[i] != null && atrDnY[i] < lo) lo = atrDnY[i];
      }
      if (!isFinite(lo) || !isFinite(hi)) return;
      const pad = (hi - lo) * 0.04;
      div._yFitting = true;
      Plotly.relayout(div, { 'yaxis.range': [lo - pad, hi + pad] });
      div._yFitting = false;
    }

    Plotly.react(div, traces, plotlyLayout({
      height: 480,
      dragmode: 'pan',
      xaxis: { type: 'date', rangeslider: { visible: false }, fixedrange: false, range: [initX0, initX1] },
      yaxis: { title: 'BTC (USDT)', tickformat: '$,.0f', fixedrange: true, autorange: false },
    }), { responsive: true, displayModeBar: false, scrollZoom: true, doubleClick: 'reset' });
    div.querySelector('.empty-state')?.remove();

    // Attach handlers (remove previous to avoid doubles)
    div.removeAllListeners?.('plotly_click');
    div.removeAllListeners?.('plotly_relayout');
    div.on('plotly_click', onMarkerClick);
    div.on('plotly_relayout', (e) => {
      if (e['xaxis.range[0]'] !== undefined || e['xaxis.range'] !== undefined || e['xaxis.autorange'] !== undefined) {
        fitYToVisible();
      }
    });
    fitYToVisible();

  } catch (err) {
    console.error('[supertrend] price chart error:', err);
    div.innerHTML = `<div class="empty-state">차트 로딩 실패</div>`;
  }
}

// ── Equity chart ──────────────────────────────────────────────────────────

async function renderEquityChart() {
  const div = document.getElementById('chart-equity');
  if (!div) return;

  try {
    // Always from strategy live date ($200 baseline), not affected by range buttons
    const data = await apiFetch(`/api/internal/supertrend/equity?from=2026-05-18`);
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
        line: { color: p.info, width: 2 },
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

// ── Signal click → condition explanation modal ────────────────────────────

function onMarkerClick(ev) {
  if (!ev.points || ev.points.length === 0) return;
  const pt    = ev.points[0];
  const barTs = pt.customdata;
  if (!barTs) return;
  const key = new Date(barTs).getTime();
  const sig = _signalMap.get(key);
  const cmp = _compareMap.get(key);
  if (!sig) return;
  openSignalModal(sig, cmp);
}

function openSignalModal(sig, cmp) {
  const overlay = document.getElementById('signal-modal');
  const titleEl = document.getElementById('signal-modal-title');
  const bodyEl  = document.getElementById('signal-modal-body');
  if (!overlay || !bodyEl) return;

  const isEnter = sig.expected_action === 'enter';
  titleEl.textContent = `${isEnter ? '진입' : '종료'} 신호 상세 — ${fmtKST(sig.bar_ts)}`;

  const ema7v  = parseFloat(sig.fast_ema);
  const ema27v = parseFloat(sig.slow_ema);
  const ema230v = parseFloat(sig.dir_ema);
  const price  = parseFloat(sig.price);
  const atr14  = parseFloat(sig.atr_14);

  function chk(ok) {
    return `<span class="${ok ? 'pos' : 'neg'}">${ok ? '✓' : '✗'}</span>`;
  }

  let html = '';

  if (isEnter) {
    const c1 = parseInt(sig.st_dir) === 1;
    const c2 = ema7v > ema27v;
    const c3 = price > ema230v;
    html += `
      <table class="tbl" style="width:100%;margin-bottom:12px">
        <thead><tr><th>진입 조건</th><th>값</th><th></th></tr></thead>
        <tbody>
          <tr>
            <td>ST 방향 = +1 (상승)</td>
            <td class="mono">${sig.st_dir}</td>
            <td>${chk(c1)}</td>
          </tr>
          <tr>
            <td>EMA(7) &gt; EMA(27)</td>
            <td class="mono">${ema7v.toLocaleString()} / ${ema27v.toLocaleString()}</td>
            <td>${chk(c2)}</td>
          </tr>
          <tr>
            <td>종가 &gt; EMA(230)</td>
            <td class="mono">$${price.toLocaleString()} / $${ema230v.toLocaleString()}</td>
            <td>${chk(c3)}</td>
          </tr>
        </tbody>
      </table>`;
    html += `
      <div style="font:var(--t-small);display:flex;flex-direction:column;gap:6px">
        <div style="display:flex;justify-content:space-between"><span class="muted">예상 진입가</span><span class="mono">${fmtUSD(sig.price)}</span></div>
        <div style="display:flex;justify-content:space-between"><span class="muted">예상 수량</span><span class="mono">${sig.expected_qty ? fmtBTC(sig.expected_qty) : '—'}</span></div>
        <div style="display:flex;justify-content:space-between"><span class="muted">스톱로스 (카타스트로픽)</span><span class="mono">${sig.expected_stop_loss ? fmtUSD(sig.expected_stop_loss) : '—'}</span></div>
        <div style="display:flex;justify-content:space-between"><span class="muted">ATR(14)</span><span class="mono">${atr14.toFixed(1)}</span></div>
      </div>`;
  } else {
    const reason  = sig.exit_reason;
    const atrDist = atr14 * ST.atrMult;
    const isEma   = reason === 'ema_cross';
    const isAtr   = reason === 'atr_distance';
    html += `
      <table class="tbl" style="width:100%;margin-bottom:12px">
        <thead><tr><th>종료 조건</th><th>값</th><th></th></tr></thead>
        <tbody>
          <tr>
            <td>EMA(7) &lt; EMA(27) (EMA 크로스)</td>
            <td class="mono">${ema7v.toLocaleString()} / ${ema27v.toLocaleString()}</td>
            <td>${chk(isEma)}</td>
          </tr>
          <tr>
            <td>|종가−진입가| ≥ ATR(14)×${ST.atrMult}</td>
            <td class="mono">임계 $${atrDist.toFixed(0)}</td>
            <td>${chk(isAtr)}</td>
          </tr>
        </tbody>
      </table>`;
    html += `
      <div style="font:var(--t-small);display:flex;flex-direction:column;gap:6px">
        <div style="display:flex;justify-content:space-between"><span class="muted">종료 사유</span>
          <span class="badge ${isEma ? 'warning' : 'danger'}">${isEma ? 'EMA 크로스' : isAtr ? 'ATR 거리' : reason || '—'}</span></div>
        <div style="display:flex;justify-content:space-between"><span class="muted">예상 종료가</span><span class="mono">${fmtUSD(sig.price)}</span></div>
        <div style="display:flex;justify-content:space-between"><span class="muted">ATR(14)</span><span class="mono">${atr14.toFixed(1)}</span></div>
      </div>`;
  }

  // Actual fill comparison
  if (cmp) {
    const statusBadge = cmp.status === 'matched'
      ? '<span class="badge success">매칭</span>'
      : cmp.status === 'missed'
        ? '<span class="badge danger">미체결</span>'
        : '<span class="badge warning">초과</span>';
    html += `
      <div style="margin-top:16px;padding-top:12px;border-top:1px solid var(--border)">
        <div style="font:var(--t-label);color:var(--text-muted);margin-bottom:8px">실제 체결</div>
        <div style="font:var(--t-small);display:flex;flex-direction:column;gap:6px">
          <div style="display:flex;justify-content:space-between"><span class="muted">체결 여부</span>${statusBadge}</div>
          ${cmp.actual_price ? `<div style="display:flex;justify-content:space-between"><span class="muted">체결가</span><span class="mono">${fmtUSD(cmp.actual_price)}</span></div>` : ''}
          ${cmp.actual_qty   ? `<div style="display:flex;justify-content:space-between"><span class="muted">체결 수량</span><span class="mono">${fmtBTC(cmp.actual_qty)}</span></div>` : ''}
          ${cmp.slippage_pct != null ? `<div style="display:flex;justify-content:space-between"><span class="muted">슬리피지</span><span class="mono ${Math.abs(cmp.slippage_pct) > 0.1 ? 'neg' : ''}">${fmtPct(cmp.slippage_pct, 3)}</span></div>` : ''}
          ${cmp.timing_lag_ms != null ? `<div style="display:flex;justify-content:space-between"><span class="muted">타이밍 지연</span><span class="mono">${fmtMs(cmp.timing_lag_ms)}</span></div>` : ''}
          ${cmp.actual_fill_time ? `<div style="display:flex;justify-content:space-between"><span class="muted">체결 시각</span><span class="mono">${fmtKST(cmp.actual_fill_time)}</span></div>` : ''}
        </div>
      </div>`;
  } else {
    html += `<div style="margin-top:16px;padding-top:12px;border-top:1px solid var(--border);font:var(--t-small);color:var(--text-muted)">실제 체결 데이터 없음</div>`;
  }

  bodyEl.innerHTML = html;
  overlay.style.display = 'flex';
}

// ── Init & polling ────────────────────────────────────────────────────────

async function loadCharts() {
  await Promise.allSettled([renderPriceChart(), renderEquityChart()]);
}

async function init() {
  // Modal close handlers
  document.getElementById('signal-modal-close')?.addEventListener('click', () => {
    document.getElementById('signal-modal').style.display = 'none';
  });
  document.getElementById('signal-modal')?.addEventListener('click', (e) => {
    if (e.target === e.currentTarget) e.currentTarget.style.display = 'none';
  });

  await Promise.allSettled([updateStatus(), updateCompare(), loadCharts()]);
}

init();
setInterval(() => { updateStatus(); updateCompare(); }, 10_000);
setInterval(loadCharts, 5 * 60_000);

// Re-render charts when theme changes
window.addEventListener('bm:themechange', loadCharts);
