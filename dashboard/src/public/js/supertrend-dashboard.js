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

// ── Strategy params (SSOT: cryptoengine/config/strategies/supertrend.yaml) ──

// st_factor=2.6 st_period=9 | EMA 7/29/240 | atr_mult=3.3 (ATR(14) exit distance)
const ST = { factor: 2.6, period: 9, fastEma: 7, slowEma: 29, dirEma: 240, atrMult: 3.3 };

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
let _compareMap = new Map();
let _priceChart  = null;
let _equityChart = null;
let _priceSeries  = null;   // in-progress candle update
let _lastBucketTs = null;   // detect 4h bucket change
let _tooltip      = null;   // floating crosshair card (TradingView-style)
let _emaChartData = null;   // {ema7,ema29,ema240,tsArr,tsToIdx,candleMap,stSigMap} set by renderPriceChart
let _liveCandle        = null;  // current in-progress 4h candle {time,open,high,low,close}
let _tooltipPinnedTime = null;  // bar time the user pinned via hover; null = follow live edge
let _renderTooltip     = null;  // ref to renderPriceChart's renderTooltip closure (for live refresh)

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

// bar_ts is bar OPEN time; signal fires at bar CLOSE (+4h)
const BAR_4H_MS = 14_400_000;
function barCloseMs(barTsStr)  { return new Date(barTsStr).getTime() + BAR_4H_MS; }
function barCloseIso(barTsStr) { return new Date(barCloseMs(barTsStr)).toISOString(); }

// One-step EMA update: extend a confirmed EMA to the in-progress bar's close.
// ema_live = close*k + ema_prev*(1-k), k = 2/(len+1). Returns null if no seed.
function _emaStep(prev, close, len) {
  if (prev == null || !isFinite(close)) return null;
  const k = 2 / (len + 1);
  return close * k + prev * (1 - k);
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

  // Detect split fills: same bar_ts + same expected_action = multiple fills for one signal
  const seen = new Map();
  tbody.innerHTML = rows.map((r) => {
    const key = `${r.bar_ts}|${r.expected_action}`;
    const isFirst = !seen.has(key);
    seen.set(key, true);
    const isSplit = r.status === 'matched' && !isFirst;

    const statusBadge = r.status === 'matched'
      ? '<span class="badge success">매칭</span>'
      : '<span class="badge danger">놓침</span>';
    const actionBadge = r.expected_action === 'enter'
      ? '<span class="badge success">진입</span>'
      : '<span class="badge danger">종료</span>';
    const slipAbs = r.slippage_pct !== null ? Math.abs(r.slippage_pct) : null;
    const slipClass = slipAbs !== null && slipAbs > 0.1 ? 'neg' : '';

    return `<tr${isSplit ? ' style="background:color-mix(in srgb,var(--surface) 94%,var(--c-primary,#1f6feb))"' : ''}>
      <td class="mono" style="font-size:12px">${fmtKST(barCloseIso(r.bar_ts))}</td>
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

// ── LWC theme helper ──────────────────────────────────────────────────────

function lwcThemeOpts() {
  const p          = palette();
  const bg         = T('--surface') || '#ffffff';
  const gridColor  = T('--border')  || '#e6e7e9';
  const textColor  = T('--text-muted') || '#667085';
  return {
    layout: {
      background: { type: 'solid', color: bg },
      textColor,
      fontFamily: "'Inter', system-ui, sans-serif",
      fontSize: 11,
    },
    grid: {
      vertLines: { color: gridColor },
      horzLines: { color: gridColor },
    },
    rightPriceScale: { borderColor: gridColor },
    timeScale:       { borderColor: gridColor, timeVisible: true, secondsVisible: false,
      tickMarkFormatter: (t) => new Date(t * 1000).toLocaleString('ko-KR', {
        timeZone: 'Asia/Seoul', month: '2-digit', day: '2-digit',
      }),
    },
    localization: {
      timeFormatter: (t) => new Date(t * 1000).toLocaleString('ko-KR', {
        timeZone: 'Asia/Seoul', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit',
      }),
    },
    handleScale: true,
    handleScroll: true,
  };
}


// ── Price chart (TradingView Lightweight Charts) ──────────────────────────

async function renderPriceChart() {
  const div = document.getElementById('chart-price');
  if (!div) return;

  if (_priceChart) { _priceChart.remove(); _priceChart = null; _priceSeries = null; _tooltip = null; }
  // Fresh render → follow the live edge again until the user pins a bar by hovering.
  _tooltipPinnedTime = null;

  try {
    const p = palette();
    const CANDLES_FROM = '2026-01-01';
    const SIGNALS_FROM = CANDLES_FROM;  // signals DB starts 2026-04-20; fetch everything

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

    _compareMap = new Map(cmpRows.map(r => [barCloseMs(r.bar_ts), r]));

    // ── Compute indicators ─────────────────────────────────────────
    const closes = candles.map(c => parseFloat(c.close));
    const unixTs = candles.map(c => Math.floor(new Date(c.ts).getTime() / 1000));

    const ema7   = computeEma(closes, ST.fastEma);   // EMA7
    const ema27  = computeEma(closes, ST.slowEma);   // EMA29
    const ema230 = computeEma(closes, ST.dirEma);    // EMA240
    // ST line built from DB signals (canonical: matches live strategy + backtest Jesse)
    const _stSigMap = new Map(
      signals
        .filter(s => s.st_line != null)
        .map(s => [Math.floor(new Date(s.bar_ts).getTime() / 1000),
                   { trend: parseInt(s.st_dir), line: parseFloat(s.st_line) }])
    );

    // Store indicator data for legend functions (module-level access)
    const tsToIdx  = new Map(unixTs.map((t, i) => [t, i]));
    const candleMap = new Map(candles.map((c, i) => [unixTs[i], {
      open: parseFloat(c.open), close: parseFloat(c.close),
    }]));
    const _lastIdx = unixTs.length - 1;
    _emaChartData = {
      ema7, ema29: ema27, ema240: ema230,
      tsArr: unixTs, tsToIdx, candleMap, stSigMap: _stSigMap,
      // live in-progress bar extension (populated by updateInProgressCandle)
      emaSeed: _lastIdx >= 0
        ? { e7: ema7[_lastIdx], e29: ema27[_lastIdx], e240: ema230[_lastIdx] }
        : { e7: null, e29: null, e240: null },
      lastConfirmedTs: _lastIdx >= 0 ? unixTs[_lastIdx] : null,
      liveTime: null,
      liveEma:  null,
    };

    // ── Build signal markers ───────────────────────────────────────
    const markers = [];
    for (const s of signals) {
      if (s.expected_action !== 'enter' && s.expected_action !== 'exit') continue;
      const key     = barCloseMs(s.bar_ts);   // signal fires at bar CLOSE, not bar open
      const cmp     = _compareMap.get(key);
      const matched = cmp ? cmp.status === 'matched' : false;
      const enter   = s.expected_action === 'enter';
      markers.push({
        time:     Math.floor(key / 1000),      // bar close unix timestamp
        position: enter ? 'belowBar' : 'aboveBar',
        color:    matched ? (enter ? p.success : p.danger) : p.muted,
        shape:    enter ? 'arrowUp' : 'arrowDown',
        text:     matched ? '체결' : '예상',
      });
    }
    markers.sort((a, b) => a.time - b.time);

    // ── Create chart ───────────────────────────────────────────────
    div.innerHTML = '';
    div.style.minHeight = '460px';
    _priceChart = LightweightCharts.createChart(div, {
      ...lwcThemeOpts(),
      autoSize: true,
      crosshair: {
        mode: LightweightCharts.CrosshairMode.Normal,
        // hide the cursor's own price-axis label (would look like a duplicate box)
        horzLine: { labelVisible: false },
      },
    });

    // Candlestick
    _priceSeries = _priceChart.addCandlestickSeries({
      upColor:        p.success, downColor:     p.danger,
      borderVisible:  false,
      wickUpColor:    p.success, wickDownColor: p.danger,
      lastValueVisible: true,   // live current-price ticker on right axis
    });
    _priceSeries.setData(candles.map((c, i) => ({
      time:  unixTs[i],
      open:  parseFloat(c.open),
      high:  parseFloat(c.high),
      low:   parseFloat(c.low),
      close: parseFloat(c.close),
    })));

    // EMA lines (width:1) — sparse: only include bars where value is non-null
    function addEmaLine(values, color, title) {
      const s = _priceChart.addLineSeries({
        color, lineWidth: 1, title,
        priceLineVisible: false, lastValueVisible: true,
      });
      s.setData(unixTs.reduce((acc, t, i) => {
        if (values[i] != null) acc.push({ time: t, value: values[i] });
        return acc;
      }, []));
    }
    addEmaLine(ema230, T('--c-gauge-orange') || '#E87722', 'EMA240');
    addEmaLine(ema27,  p.info,  'EMA29');
    addEmaLine(ema7,   p.muted, 'EMA7');

    const stSeries = _priceChart.addLineSeries({
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: true,
      title: 'ST',
    });
    stSeries.setData(
      unixTs.reduce((acc, t) => {
        const sig = _stSigMap.get(t);
        if (sig) acc.push({ time: t, value: sig.line, color: sig.trend === 1 ? p.success : p.danger });
        return acc;
      }, [])
    );

    // Markers on price series
    _priceSeries.setMarkers(markers);

    // Initial window: last ~180 4h bars (~30 days), pan left for history
    // Use timestamp-based range (more reliable than logical index across sparse series)
    const len = unixTs.length;
    if (len > 1) {
      const startIdx = Math.max(0, len - 180);
      _priceChart.timeScale().setVisibleRange({
        from: unixTs[startIdx],
        to:   unixTs[len - 1],
      });
    }

    // ── Floating crosshair tooltip (TradingView-style) ─────────────────────────
    // Appears at the crosshair position, freezes there on release/leave.
    div.style.position = 'relative';
    _tooltip = document.createElement('div');
    _tooltip.style.cssText = [
      'position:absolute;pointer-events:none;z-index:6;display:none',
      'background:rgba(13,18,28,.94);color:#fff',
      'border:1px solid rgba(255,255,255,.12);border-radius:6px',
      'box-shadow:var(--shadow-3);padding:8px 10px',
      "font:11px/1.5 'JetBrains Mono',monospace;white-space:nowrap",
    ].join(';');
    div.appendChild(_tooltip);

    function renderTooltip(time, point) {
      if (!_emaChartData || !_tooltip) return;
      const { ema7, ema29, ema240, tsToIdx, candleMap, stSigMap, liveTime, liveEma } = _emaChartData;
      const p2     = palette();
      const isLive = (liveTime != null && time === liveTime);
      const idx    = tsToIdx.get(time);
      const stSig  = stSigMap.get(time);
      // Confirmed bars resolve via candleMap; the in-progress bar falls back to _liveCandle.
      const cand   = candleMap.get(time)
        || (isLive && _liveCandle ? { open: _liveCandle.open, close: _liveCandle.close } : null);
      if (idx == null && !stSig && !cand) return;
      const rows = [];
      if (stSig) rows.push({ label: stSig.trend===1?'ST(롱)':'ST(숏)', color: stSig.trend===1?p2.success:p2.danger, price: stSig.line });
      // EMA values: live bar uses the 1-step extended EMA; confirmed bars use the array.
      const e240v = isLive ? (liveEma ? liveEma.e240 : null) : (idx != null ? ema240[idx] : null);
      const e29v  = isLive ? (liveEma ? liveEma.e29  : null) : (idx != null ? ema29[idx]  : null);
      const e7v   = isLive ? (liveEma ? liveEma.e7   : null) : (idx != null ? ema7[idx]   : null);
      if (e240v != null) rows.push({ label:'EMA240', color: T('--c-gauge-orange')||'#E87722', price: e240v });
      if (e29v  != null) rows.push({ label:'EMA29',  color: p2.info,  price: e29v });
      if (e7v   != null) rows.push({ label:'EMA7',   color: p2.muted, price: e7v });
      if (cand) rows.push({ label:'Close', color: cand.close>=cand.open?p2.success:p2.danger, price: cand.close });
      rows.sort((a, b) => b.price - a.price);
      const barLabel = new Date(time * 1000).toLocaleString('ko-KR',
        { timeZone:'Asia/Seoul', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit' })
        + (isLive ? ' (진행중)' : '');
      _tooltip.innerHTML =
        `<div style="color:rgba(255,255,255,.5);font-size:10px;margin-bottom:5px">${barLabel}</div>` +
        rows.map(r =>
          `<div style="display:flex;align-items:center;gap:8px;justify-content:space-between">` +
          `<span style="display:flex;align-items:center;gap:4px">` +
          `<span style="width:8px;height:8px;border-radius:50%;background:${r.color};flex-shrink:0"></span>` +
          `<span style="color:rgba(255,255,255,.7)">${r.label}</span></span>` +
          `<span style="color:#fff">${fmtUSD(r.price)}</span></div>`
        ).join('');
      if (point) {
        const tw = _tooltip.offsetWidth || 140, th = _tooltip.offsetHeight || 80;
        const cw = div.clientWidth,           ch = div.clientHeight;
        const m  = 8;
        let left = point.x + 16; if (left + tw > cw - m) left = point.x - tw - 16;
        let top  = point.y + 16; if (top  + th > ch - m) top  = point.y - th - 16;
        _tooltip.style.left = Math.max(m, left) + 'px';
        _tooltip.style.top  = Math.max(m, top)  + 'px';
      } else {
        _tooltip.style.left = '10px';
        _tooltip.style.top  = '10px';
      }
      _tooltip.style.display = 'block';
    }

    // Expose for live refresh from updateInProgressCandle (keeps this closure's div/_tooltip).
    _renderTooltip = renderTooltip;

    // Default: show last bar's card at top-left on load (non-empty initial state).
    // updateInProgressCandle (called right after render) refreshes this to the live bar.
    requestAnimationFrame(() => { if (len > 0) renderTooltip(unixTs[len - 1], null); });

    // Crosshair → update floating card at crosshair position; freeze on leave/release
    // Use seriesData to get the exact bar-open timestamp (param.time is raw x-axis seconds,
    // not necessarily snapped to bar timestamps in CrosshairMode.Normal).
    _priceChart.subscribeCrosshairMove((param) => {
      if (param.time && param.point) {
        const candleBar = param.seriesData?.get(_priceSeries);
        const barTime = candleBar?.time ?? param.time;
        // Pin the card to the hovered bar. Hovering the live edge keeps following live;
        // hovering a historical bar freezes the card there (matches freeze-on-leave UX).
        _tooltipPinnedTime = barTime;
        // NOTE: never call series.applyOptions() here — applyOptions triggers a chart
        // redraw that synchronously re-fires crosshairMove → infinite recursion (stack
        // overflow) that breaks the whole tooltip. ST marker uses its series default color.
        renderTooltip(barTime, param.point);
      }
      // on leave / touch-end: keep tooltip frozen at last position (don't hide)
    });

  } catch (err) {
    console.error('[supertrend] price chart error:', err);
    div.innerHTML = `<div class="empty-state">차트 로딩 실패</div>`;
  }
}

// ── Equity chart (TradingView Lightweight Charts) ─────────────────────────

async function renderEquityChart() {
  const div = document.getElementById('chart-equity');
  if (!div) return;

  if (_equityChart) { _equityChart.remove(); _equityChart = null; }

  try {
    const data = await apiFetch(`/api/internal/supertrend/equity?from=2026-05-18`);
    const exp = data.expected || [];
    const act = data.actual   || [];
    const p   = palette();

    if (exp.length === 0 && act.length === 0) {
      div.innerHTML = `<div class="empty-state">자산 데이터 없음</div>`;
      return;
    }

    div.innerHTML = '';
    div.style.minHeight = '220px';
    _equityChart = LightweightCharts.createChart(div, {
      ...lwcThemeOpts(),
      autoSize: true,
    });

    // Actual equity (area fill)
    if (act.length > 0) {
      const actSeries = _equityChart.addAreaSeries({
        lineColor:   p.success,
        topColor:    p.success + '30',
        bottomColor: 'transparent',
        lineWidth: 2,
        title: '실제 자산',
        priceLineVisible: false,
      });
      actSeries.setData(act.map(d => ({
        time:  Math.floor(new Date(d.ts).getTime() / 1000),
        value: d.equity,
      })));
    }

    // Expected equity (line)
    if (exp.length > 0) {
      const expSeries = _equityChart.addLineSeries({
        color: p.info, lineWidth: 2,
        title: '예상 자산',
        priceLineVisible: false,
      });
      expSeries.setData(exp.map(d => ({
        time:  Math.floor(new Date(d.ts).getTime() / 1000),
        value: d.equity,
      })));
    }

    _equityChart.timeScale().fitContent();

  } catch (err) {
    console.error('[supertrend] equity chart error:', err);
    div.innerHTML = `<div class="empty-state">차트 로딩 실패</div>`;
  }
}

// ── In-progress 4h candle (1-min polling) ────────────────────────────────

async function updateInProgressCandle() {
  if (!_priceSeries) return;
  try {
    const r = await apiFetch('/api/internal/supertrend/candles/in-progress');
    if (!r || !r.candle) return;
    const c = r.candle;
    const liveTime = Math.floor(c.ts / 1000);
    const o  = parseFloat(c.open), h = parseFloat(c.high),
          l  = parseFloat(c.low),  cl = parseFloat(c.close);
    _priceSeries.update({ time: liveTime, open: o, high: h, low: l, close: cl });

    // Integrate the live bar into the tooltip data so the floating card resolves
    // and updates for the in-progress candle — not only confirmed bars.
    _liveCandle = { time: liveTime, open: o, high: h, low: l, close: cl };
    if (_emaChartData) {
      _emaChartData.liveTime = liveTime;
      _emaChartData.candleMap.set(liveTime, { open: o, close: cl });
      const seed = _emaChartData.emaSeed || {};
      _emaChartData.liveEma = {
        e240: _emaStep(seed.e240, cl, ST.dirEma),
        e29:  _emaStep(seed.e29,  cl, ST.slowEma),
        e7:   _emaStep(seed.e7,   cl, ST.fastEma),
      };
    }
    // Auto-refresh the floating card when it's following the live edge
    // (not pinned by the user onto a historical bar).
    if (_renderTooltip && (_tooltipPinnedTime == null || _tooltipPinnedTime === liveTime)) {
      _renderTooltip(liveTime, null);
    }

    if (_lastBucketTs !== null && _lastBucketTs !== c.ts) {
      await loadCharts();
    }
    _lastBucketTs = c.ts;
  } catch (e) {
    console.error('[supertrend] in-progress update error:', e);
  }
}

// ── Init & polling ────────────────────────────────────────────────────────

async function loadCharts() {
  await Promise.allSettled([renderPriceChart(), renderEquityChart()]);
  updateInProgressCandle();
}

async function init() {
  await Promise.allSettled([updateStatus(), updateCompare(), loadCharts()]);
}

init();
setInterval(() => { updateStatus(); updateCompare(); }, 10_000);
setInterval(loadCharts, 5 * 60_000);
setInterval(updateInProgressCandle, 60_000);

// Re-render charts when theme changes (chart instances are destroyed and recreated)
window.addEventListener('bm:themechange', loadCharts);
