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
    margin: { l: 52, r: 16, t: 16, b: 36 },
    autosize: true,
    hovermode: 'x unified',
    showlegend: true,
    legend: { bgcolor: 'transparent', font: { color: p.muted, size: 11 } },
  }, extra);
};

// ── Utilities ─────────────────────────────────────────────────────────────

async function apiFetch(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

function fmtKST(isoStr) {
  if (!isoStr) return '—';
  return new Date(isoStr).toLocaleString('ko-KR', {
    timeZone: 'Asia/Seoul', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit',
  });
}

function fmtUSD(val) {
  if (val === null || val === undefined) return '—';
  const n = Number(val);
  return (n >= 0 ? '+' : '') + `$${n.toFixed(2)}`;
}

function setEl(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

// ── Portfolio ─────────────────────────────────────────────────────────────

async function updatePortfolio() {
  try {
    const data = await apiFetch('/api/internal/monitor/portfolio?days=30');
    const lat = data.latest || {};

    setEl('kpi-equity', lat.equity ? `$${Number(lat.equity).toFixed(2)}` : '—');
    setEl('kpi-equity-sub', lat.snapshot_at ? fmtKST(lat.snapshot_at) : '—');

    const dailyEl = document.getElementById('kpi-daily-pnl');
    if (dailyEl) {
      const v = lat.daily_pnl;
      dailyEl.textContent = fmtUSD(v);
      dailyEl.className = `kpi-value ${v >= 0 ? 'pos' : 'neg'}`;
    }

    const urEl = document.getElementById('kpi-unrealized');
    if (urEl) {
      const v = lat.unrealized_pnl;
      urEl.textContent = fmtUSD(v);
      urEl.className = `kpi-value ${!v || v >= 0 ? 'pos' : 'neg'}`;
    }

    const mtdEl = document.getElementById('kpi-mtd');
    if (mtdEl) {
      const v = lat.mtd_pnl;
      mtdEl.textContent = v !== null ? fmtUSD(v) : '—';
      mtdEl.className = `kpi-value ${!v || v >= 0 ? 'pos' : 'neg'}`;
    }

    const mddEl = document.getElementById('kpi-mdd');
    if (mddEl && data.max_drawdown !== null) {
      const pct = (data.max_drawdown * 100).toFixed(1);
      mddEl.textContent = `${pct}%`;
      mddEl.className = `kpi-value ${data.max_drawdown < -0.05 ? 'neg' : 'warn'}`;
    }

    if (data.curve && data.curve.length > 0) {
      const div = document.getElementById('chart-equity');
      const p = palette();
      Plotly.react(div, [{
        type: 'scatter', mode: 'lines',
        x: data.curve.map((d) => new Date(d.ts)),
        y: data.curve.map((d) => d.equity),
        name: '총 자산',
        line: { color: p.info, width: 2 },
        fill: 'tozeroy',
        fillcolor: p.info + '18',
        hovertemplate: '%{y:$,.2f}<extra></extra>',
      }], plotlyLayout({
        height: 230,
        yaxis: { tickformat: '$,.0f' },
        xaxis: { type: 'date' },
      }), { responsive: true, displayModeBar: false });
      div.querySelector('.empty-state')?.remove();
    }
  } catch (err) {
    console.warn('[monitor] portfolio error:', err);
  }
}

// ── Kill Switch ───────────────────────────────────────────────────────────

async function updateKillSwitch() {
  try {
    const data = await apiFetch('/api/internal/monitor/killswitch');

    const ksEl = document.getElementById('kpi-ks');
    const ksSubEl = document.getElementById('kpi-ks-sub');
    if (ksEl) {
      ksEl.innerHTML = `<span class="status-dot ${data.is_active ? 'red' : 'green live'}" style="width:10px;height:10px;flex-shrink:0"></span>${data.is_active ? '발동' : '정상'}`;
      ksEl.style.cssText = 'display:flex;align-items:center;gap:8px;font-size:22px';
      ksEl.className = `kpi-value ${data.is_active ? 'neg' : ''}`;
    }
    if (ksSubEl) {
      ksSubEl.textContent = data.is_active ? (data.current_event?.reason || '활성') : '이상 없음';
    }

    const tbody = document.getElementById('ks-tbody');
    if (tbody) {
      if (!data.history || data.history.length === 0) {
        tbody.innerHTML = `<tr><td colspan="5" class="muted" style="text-align:center">이력 없음</td></tr>`;
      } else {
        const lvlLabel = { 1: 'L1-전략', 2: 'L2-포트폴리오', 3: 'L3-시스템', 4: 'L4-수동' };
        tbody.innerHTML = data.history.map((e) =>
          `<tr>
            <td class="mono" style="font-size:12px">${fmtKST(e.triggered_at)}</td>
            <td><span class="badge danger">${lvlLabel[e.level] || `L${e.level}`}</span></td>
            <td>${e.reason}</td>
            <td>${e.resolved_at ? fmtKST(e.resolved_at) : '<span class="neg">미해제</span>'}</td>
            <td class="num ${e.pnl_at_trigger >= 0 ? 'pos' : 'neg'}">${e.pnl_at_trigger ? fmtUSD(e.pnl_at_trigger) : '—'}</td>
          </tr>`
        ).join('');
      }
    }
  } catch (err) {
    console.warn('[monitor] killswitch error:', err);
  }
}

// ── Positions ─────────────────────────────────────────────────────────────

async function updatePositions() {
  try {
    const data = await apiFetch('/api/internal/monitor/positions');
    const tbody = document.getElementById('positions-tbody');
    const countEl = document.getElementById('positions-count');

    if (!tbody) return;

    const count = data.positions?.length || 0;
    if (countEl) {
      countEl.textContent = count > 0 ? `${count} open` : '없음';
      countEl.className = count > 0 ? 'badge success' : 'badge neutral';
    }

    if (!data.positions || data.positions.length === 0) {
      tbody.innerHTML = `<tr><td colspan="6" class="muted" style="text-align:center">포지션 없음</td></tr>`;
    } else {
      tbody.innerHTML = data.positions.map((p) => {
        const liqDist = p.liq_distance_pct;
        const liqClass = liqDist === null ? '' : liqDist < 5 ? 'neg' : liqDist < 10 ? 'warn' : 'pos';
        const upnl = p.unrealized_pnl || 0;
        const sideClass = (p.side || '').toLowerCase() === 'long' ? 'success' : 'danger';
        return `<tr>
          <td><strong>${p.symbol}</strong></td>
          <td><span class="badge ${sideClass}">${(p.side || '').toUpperCase()}</span></td>
          <td class="num">${p.size || '—'}</td>
          <td class="num">$${p.entry_price ? Number(p.entry_price).toLocaleString() : '—'}</td>
          <td class="num ${upnl >= 0 ? 'pos' : 'neg'}">${fmtUSD(upnl)}</td>
          <td class="num ${liqClass}">${liqDist !== null ? `${liqDist.toFixed(1)}%` : '—'}</td>
        </tr>`;
      }).join('');
    }
  } catch (err) {
    console.warn('[monitor] positions error:', err);
  }
}

// ── Service Health ────────────────────────────────────────────────────────

async function updateServiceHealth() {
  try {
    const data = await apiFetch('/api/internal/monitor/service');
    const grid = document.getElementById('health-grid');
    if (!grid) return;

    const staleLabel = (sec) => {
      if (sec === null || sec === undefined || !isFinite(sec)) return '응답 없음';
      if (sec < 60)   return `${sec}초 전`;
      if (sec < 3600) return `${Math.round(sec / 60)}분 전`;
      return `${Math.round(sec / 3600)}시간 전`;
    };

    const services = data.services || [];
    const healthyCount = services.filter((s) => s.status === 'green').length;
    const metaEl = document.getElementById('health-meta');
    if (metaEl) metaEl.textContent = `${services.length}개 컨테이너 · ${healthyCount}개 정상`;

    const liveStatuses = new Set(['green']);
    grid.innerHTML = services.map((s) =>
      `<div class="health-card">
        <span class="status-dot ${s.status}${liveStatuses.has(s.status) ? ' live' : ''}"></span>
        <div>
          <div class="health-name">${s.service}</div>
          <div class="health-meta">${staleLabel(s.stale_sec)}${s.errors_6h > 0 ? ` · 오류 ${s.errors_6h}건` : ''}</div>
        </div>
      </div>`
    ).join('');
  } catch (err) {
    console.warn('[monitor] service health error:', err);
  }
}

// ── Infra gauges (circular) ───────────────────────────────────────────────

const GAUGE_STEPS = [
  { max: 60,  token: '--c-success' },
  { max: 75,  token: '--c-warning' },
  { max: 90,  token: '--c-gauge-orange' },
  { max: 101, token: '--c-danger' },
];

function gaugeColor(val) {
  if (val === null) return T('--text-soft');
  for (const s of GAUGE_STEPS) {
    if (val < s.max) return T(s.token);
  }
  return T('--c-danger');
}

function svgGauge(val, color) {
  const r = 38, circ = 2 * Math.PI * r;
  const offset = val !== null ? circ * (1 - Math.min(val, 100) / 100) : circ;
  const c = val !== null ? color : T('--text-soft');
  const pct = val !== null ? `${val.toFixed(1)}%` : '—';
  return `<svg viewBox="0 0 96 96">
    <circle class="track" cx="48" cy="48" r="38"/>
    <circle class="bar" cx="48" cy="48" r="38" stroke="${c}"
      stroke-dasharray="${circ.toFixed(2)}"
      stroke-dashoffset="${offset.toFixed(2)}"/>
  </svg>
  <div class="label">${pct}</div>`;
}

const GAUGE_META = [
  { key: 'cpu',   label: 'CPU',    apiKey: 'cpu_pct' },
  { key: 'mem',   label: '메모리', apiKey: 'mem_pct' },
  { key: 'disk',  label: '디스크', apiKey: 'disk_pct' },
  { key: 'redis', label: 'Redis',  apiKey: 'redis_mem_pct' },
];

async function updateInfra() {
  try {
    const data = await apiFetch('/api/internal/monitor/infra');
    const grid = document.getElementById('infra-grid');
    if (!grid) return;

    grid.innerHTML = GAUGE_META.map(({ key, label, apiKey }) => {
      const val   = data[apiKey] !== undefined ? data[apiKey] : null;
      const color = gaugeColor(val);
      const tier  = val === null ? '—' : val < 60 ? '정상' : val < 75 ? '주의' : val < 90 ? '경고' : '위험';
      return `<div class="gauge-item" data-key="${key}" data-label="${label}" title="클릭 시 24시간 추이">
        <div class="gauge">${svgGauge(val, color)}</div>
        <div class="gauge-name">${label}</div>
        <div class="gauge-status" style="color:${color}">${tier}</div>
      </div>`;
    }).join('');

    grid.querySelectorAll('.gauge-item').forEach((item) => {
      item.addEventListener('click', () => openInfraModal(item.dataset.key, item.dataset.label));
    });
  } catch (err) {
    console.warn('[monitor] infra error:', err);
  }
}

// ── Infra history modal ───────────────────────────────────────────────────

async function openInfraModal(key, label) {
  const overlay  = document.getElementById('infra-modal');
  const chartDiv = document.getElementById('modal-chart');
  document.getElementById('modal-title').textContent = `${label} — 24시간 추이`;
  overlay.style.display = 'flex';
  chartDiv.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:300px;color:var(--text-soft)">로딩 중…</div>`;
  document.getElementById('modal-last-update').textContent = '';

  try {
    const data = await apiFetch(`/api/internal/monitor/infra/history?key=${key}`);
    const pts  = data.history || [];

    if (pts.length === 0) {
      chartDiv.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:300px;color:var(--text-soft)">스냅샷 누적 중 (5분 주기 기록)</div>`;
      return;
    }

    const ts    = pts.map((p) => new Date(p.ts));
    const vals  = pts.map((p) => p.val);
    const color = gaugeColor(vals[vals.length - 1]);
    const p     = palette();

    Plotly.react(chartDiv, [{
      type: 'scatter', mode: 'lines+markers',
      x: ts, y: vals,
      name: label,
      line: { color, width: 2 },
      marker: { color, size: 4 },
      fill: 'tozeroy',
      fillcolor: color + '22',
      hovertemplate: `%{x|%m/%d %H:%M}<br>${label}: %{y:.1f}%<extra></extra>`,
    }], {
      ...plotlyLayout(),
      yaxis: { ...plotlyLayout().yaxis, range: [0, 100], ticksuffix: '%' },
      xaxis: { ...plotlyLayout().xaxis, type: 'date' },
      shapes: [
        { type: 'line', x0: ts[0], x1: ts[ts.length - 1], y0: 60, y1: 60, line: { color: T('--c-warning'),      width: 1, dash: 'dot' } },
        { type: 'line', x0: ts[0], x1: ts[ts.length - 1], y0: 75, y1: 75, line: { color: T('--c-gauge-orange'), width: 1, dash: 'dot' } },
        { type: 'line', x0: ts[0], x1: ts[ts.length - 1], y0: 90, y1: 90, line: { color: T('--c-danger'),       width: 1, dash: 'dot' } },
      ],
    }, { responsive: true, displayModeBar: false });

    const last = pts[pts.length - 1]?.ts;
    if (last) {
      document.getElementById('modal-last-update').textContent =
        `마지막 기록: ${new Date(last).toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' })}`;
    }
  } catch (err) {
    chartDiv.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:300px;color:var(--text-soft)">로딩 실패</div>`;
  }
}

document.getElementById('modal-close')?.addEventListener('click', () => {
  document.getElementById('infra-modal').style.display = 'none';
});
document.getElementById('infra-modal')?.addEventListener('click', (e) => {
  if (e.target === e.currentTarget) e.currentTarget.style.display = 'none';
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
  setEl('last-updated', `갱신: ${new Date().toLocaleTimeString('ko-KR')}`);
}

refreshAll();
setInterval(refreshAll, 10_000);

// Re-render charts when theme changes
window.addEventListener('bm:themechange', () => {
  updatePortfolio();
  updateInfra();
});
