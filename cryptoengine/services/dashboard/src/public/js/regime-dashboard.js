/**
 * regime-dashboard.js
 * CryptoEngine 레짐 대시보드 메인 스크립트
 */

"use strict";

// ── 상수 ──────────────────────────────────────────────────────

const REGIME_COLORS = {
  ranging: "#2196F3",
  trending_up: "#4CAF50",
  trending_down: "#F44336",
  volatile: "#FF9800",
  uncertain: "#9E9E9E",
};

const REGIME_LABELS = {
  ranging: "횡보",
  trending_up: "상승 추세",
  trending_down: "하락 추세",
  volatile: "변동성",
  uncertain: "불확실",
};

// 레짐명 → CSS 클래스명
function regimeToCssClass(regime) {
  if (!regime) return "";
  return regime.replace(/_/g, "-");
}

// 레짐 한글 라벨
function regimeLabel(regime) {
  return REGIME_LABELS[regime] || regime || "—";
}

// 레짐 색상
function regimeColor(regime) {
  return REGIME_COLORS[regime] || "#9E9E9E";
}

// 날짜 포맷 (KST)
function formatDatetime(isoStr) {
  if (!isoStr) return "—";
  try {
    const d = new Date(isoStr);
    return d.toLocaleString("ko-KR", {
      timeZone: "Asia/Seoul",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return isoStr;
  }
}

// 숫자 포맷
function fmt(val, digits = 1) {
  if (val === null || val === undefined) return "—";
  return Number(val).toFixed(digits);
}

// ── Chart.js 인스턴스 관리 ────────────────────────────────────

let confirmedChart = null;
let allocationChart = null;

// ── 섹션 1: 현재 상태 카드 갱신 ─────────────────────────────

async function updateCurrentStatus() {
  try {
    const data = await fetch("/api/internal/regime/current").then((r) => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return r.json();
    });

    // 원시 레짐 카드
    const rawRegimeEl = document.getElementById("raw-regime-value");
    const cardRaw = document.getElementById("card-raw-regime");
    rawRegimeEl.textContent = regimeLabel(data.raw_regime);
    cardRaw.className = "card " + regimeToCssClass(data.raw_regime);
    document.getElementById("raw-regime-at").textContent = data.raw_regime_at
      ? formatDatetime(data.raw_regime_at)
      : "—";

    // 확정 레짐 카드
    const confirmedEl = document.getElementById("confirmed-regime-value");
    const cardConf = document.getElementById("card-confirmed-regime");
    confirmedEl.textContent = regimeLabel(data.confirmed_regime);
    cardConf.className = "card " + regimeToCssClass(data.confirmed_regime);
    document.getElementById("confirmed-regime-count").textContent =
      data.consecutive_count ? `연속 ${data.consecutive_count}회 확인` : "—";

    // 원시 ≠ 확정이면 pulse 클래스 추가
    if (
      data.raw_regime &&
      data.confirmed_regime &&
      data.raw_regime !== data.confirmed_regime
    ) {
      cardConf.classList.add("pulse");
    } else {
      cardConf.classList.remove("pulse");
    }

    // 전략 적용 레짐 카드
    const activeEl = document.getElementById("active-regime-value");
    const cardActive = document.getElementById("card-active-regime");
    activeEl.textContent = regimeLabel(data.active_regime);
    cardActive.className = "card " + regimeToCssClass(data.active_regime);
    // 지표 표시
    const indParts = [];
    if (data.indicators?.adx != null) indParts.push(`ADX ${fmt(data.indicators.adx)}`);
    if (data.indicators?.bb_width != null) indParts.push(`BB폭 ${fmt(data.indicators.bb_width, 4)}`);
    document.getElementById("active-regime-sub").textContent =
      indParts.length ? indParts.join(" | ") : "—";

    // 가중치 전환 카드
    const trans = data.weight_transition;
    const transEl = document.getElementById("transition-value");
    const transProg = document.getElementById("transition-progress");
    if (trans && trans.in_progress) {
      const pct =
        trans.total_steps > 0
          ? Math.round((trans.current_step / trans.total_steps) * 100)
          : 0;
      transEl.textContent = `전환 중 (${trans.current_step}/${trans.total_steps})`;
      transProg.style.width = `${pct}%`;
      document.getElementById("card-transition").classList.add("active");
    } else {
      transEl.textContent = "안정";
      transProg.style.width = "100%";
      document.getElementById("card-transition").classList.remove("active");
    }

    // FA 비중 카드
    const fa = data.fa_allocation;
    document.getElementById("fa-pct-value").textContent =
      fa ? `${fmt(fa.pct)}%` : "—";
    document.getElementById("fa-usd-value").textContent =
      fa && fa.usd > 0 ? `$${fa.usd.toLocaleString()}` : "—";

    // 가중치 전환 섹션(4) 갱신
    updateTransitionSection(trans, data.active_weights);

    // 자본 배분 차트(5) 갱신
    updateAllocationChart(data);

    // 갱신 시각
    document.getElementById("last-updated").textContent =
      "최종 갱신: " +
      new Date().toLocaleTimeString("ko-KR", { timeZone: "Asia/Seoul" });
  } catch (err) {
    console.error("[regime] updateCurrentStatus error:", err);
  }
}

// ── 섹션 2: 원시 레짐 타임라인 ──────────────────────────────

async function renderRawTimeline() {
  try {
    const data = await fetch("/api/internal/regime/raw-timeline?hours=24").then(
      (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      }
    );

    const container = document.getElementById("raw-timeline-bar");
    container.innerHTML = "";

    const timeline = data.timeline || [];

    if (timeline.length === 0) {
      container.innerHTML = '<div class="no-data-msg">데이터 없음</div>';
    } else {
      // 전체 시간 범위로 각 셀 너비 계산
      const first = new Date(timeline[0].detected_at).getTime();
      const last = new Date(timeline[timeline.length - 1].detected_at).getTime();
      const totalMs = last - first || 1;

      for (let i = 0; i < timeline.length; i++) {
        const item = timeline[i];
        const color = regimeColor(item.regime);

        // 다음 항목까지의 시간 비율로 너비 결정
        let widthPct;
        if (i < timeline.length - 1) {
          const nextTime = new Date(timeline[i + 1].detected_at).getTime();
          const currTime = new Date(item.detected_at).getTime();
          widthPct = ((nextTime - currTime) / totalMs) * 100;
        } else {
          widthPct = 1;
        }
        widthPct = Math.max(widthPct, 0.1);

        const cell = document.createElement("div");
        cell.className = "timeline-cell";
        cell.style.width = widthPct + "%";
        cell.style.backgroundColor = color;
        cell.style.opacity = item.is_fake ? "0.35" : "1";
        cell.title = `${regimeLabel(item.regime)}\n${formatDatetime(item.detected_at)}\n${item.is_fake ? "[가짜 전환]" : ""}`;
        container.appendChild(cell);
      }
    }

    // 요약
    const sumEl = document.getElementById("raw-timeline-summary");
    if (data.summary) {
      const s = data.summary;
      const ratioText = Object.entries(s.regime_ratios || {})
        .map(([k, v]) => `<span class="regime-badge ${regimeToCssClass(k)}">${regimeLabel(k)} ${v}%</span>`)
        .join(" ");
      sumEl.innerHTML = `
        <div class="summary-row">
          ${ratioText}
        </div>
        <div class="summary-stats">
          총 전환 <strong>${s.total_transitions}</strong>회 &nbsp;|&nbsp;
          가짜 전환 <strong>${s.fake_transitions}</strong>회 &nbsp;|&nbsp;
          데이터 포인트 <strong>${s.total_points}</strong>개
        </div>
      `;
    } else {
      sumEl.innerHTML = "";
    }
  } catch (err) {
    console.error("[regime] renderRawTimeline error:", err);
    document.getElementById("raw-timeline-bar").innerHTML =
      '<div class="no-data-msg">로드 실패</div>';
  }
}

// ── 섹션 3: 확정 레짐 + BTC 가격 차트 ──────────────────────

async function renderConfirmedChart() {
  try {
    const data = await fetch(
      "/api/internal/regime/confirmed-timeline?days=7"
    ).then((r) => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return r.json();
    });

    const ohlcv = data.ohlcv || [];
    const periods = data.periods || [];

    if (confirmedChart) {
      confirmedChart.destroy();
      confirmedChart = null;
    }

    const ctx = document.getElementById("confirmed-chart").getContext("2d");

    if (ohlcv.length === 0) {
      // 빈 차트
      confirmedChart = new Chart(ctx, {
        type: "line",
        data: { labels: [], datasets: [] },
        options: {
          responsive: true,
          plugins: { legend: { display: false }, title: { display: true, text: "데이터 없음", color: "#9E9E9E" } },
        },
      });
      return;
    }

    const labels = ohlcv.map((r) => formatDatetime(r.timestamp));
    const prices = ohlcv.map((r) => parseFloat(r.close));

    // 레짐별 배경색 데이터셋 (annotation 대신 pointBackgroundColor 활용)
    // 각 캔들의 레짐 색상 계산
    const pointColors = ohlcv.map((candle) => {
      const t = new Date(candle.timestamp).getTime();
      let color = "#9E9E9E44";
      for (const p of periods) {
        const start = new Date(p.detected_at).getTime();
        const end = p.confirmed_at ? new Date(p.confirmed_at).getTime() : Date.now();
        if (t >= start && t <= end) {
          const c = regimeColor(p.new_regime);
          color = c + "88";
          break;
        }
      }
      return color;
    });

    confirmedChart = new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "BTC 가격 (USDT)",
            data: prices,
            borderColor: "#E0E0E0",
            borderWidth: 1.5,
            pointRadius: 0,
            pointHoverRadius: 4,
            pointBackgroundColor: pointColors,
            fill: false,
            tension: 0.1,
          },
        ],
      },
      options: {
        responsive: true,
        interaction: { intersect: false, mode: "index" },
        plugins: {
          legend: {
            display: true,
            labels: { color: "#E0E0E0" },
          },
          tooltip: {
            backgroundColor: "#16213e",
            titleColor: "#E0E0E0",
            bodyColor: "#B0B0B0",
            callbacks: {
              label: (ctx) => ` $${ctx.raw.toLocaleString()}`,
            },
          },
        },
        scales: {
          x: {
            ticks: {
              color: "#9E9E9E",
              maxTicksLimit: 8,
              maxRotation: 0,
            },
            grid: { color: "#2a2a4a" },
          },
          y: {
            ticks: {
              color: "#9E9E9E",
              callback: (v) => "$" + v.toLocaleString(),
            },
            grid: { color: "#2a2a4a" },
          },
        },
      },
    });

    // 레짐 범례 추가
    const legendContainer = document.querySelector(".confirmed-chart-legend") ||
      (() => {
        const el = document.createElement("div");
        el.className = "confirmed-chart-legend";
        document.getElementById("confirmed-chart").parentNode.appendChild(el);
        return el;
      })();
    legendContainer.innerHTML = Object.entries(REGIME_LABELS)
      .map(
        ([k, v]) =>
          `<span class="legend-item"><span class="legend-dot" style="background:${regimeColor(k)}"></span>${v}</span>`
      )
      .join("");
  } catch (err) {
    console.error("[regime] renderConfirmedChart error:", err);
  }
}

// ── 섹션 4: 가중치 전환 상태 ─────────────────────────────────

function updateTransitionSection(trans, activeWeights) {
  const label = document.getElementById("transition-status-label");
  const prog = document.getElementById("weight-transition-progress");
  const tbody = document.getElementById("weight-table-body");

  if (trans && trans.in_progress) {
    const pct =
      trans.total_steps > 0
        ? Math.round((trans.current_step / trans.total_steps) * 100)
        : 0;
    label.textContent = `전환 진행 중 — ${trans.current_step}/${trans.total_steps} 단계 (${pct}%)`;
    label.className = "transition-status-label in-progress";
    prog.style.width = pct + "%";
  } else {
    label.textContent = "전환 없음 (안정)";
    label.className = "transition-status-label stable";
    prog.style.width = "100%";
  }

  // 가중치 테이블 갱신
  const allStrategies = new Set([
    ...Object.keys(trans?.previous_weights || {}),
    ...Object.keys(trans?.current_weights || {}),
    ...Object.keys(trans?.target_weights || {}),
    ...Object.keys(activeWeights || {}),
  ]);

  if (allStrategies.size === 0) {
    tbody.innerHTML = '<tr><td colspan="4" class="no-data">데이터 없음</td></tr>';
    return;
  }

  tbody.innerHTML = [...allStrategies]
    .map((strategy) => {
      const prev = trans?.previous_weights?.[strategy];
      const curr = trans?.current_weights?.[strategy] ?? activeWeights?.[strategy];
      const target = trans?.target_weights?.[strategy];
      return `<tr>
        <td>${strategy}</td>
        <td>${prev != null ? (prev * 100).toFixed(1) + "%" : "—"}</td>
        <td>${curr != null ? (curr * 100).toFixed(1) + "%" : "—"}</td>
        <td>${target != null ? (target * 100).toFixed(1) + "%" : "—"}</td>
      </tr>`;
    })
    .join("");
}

// ── 섹션 5: 자본 배분 도넛 차트 ─────────────────────────────

function updateAllocationChart(data) {
  const detail = document.getElementById("allocation-detail");
  const ctx = document.getElementById("allocation-chart").getContext("2d");

  const fa = data.fa_allocation;
  const totalEquity = data.total_equity;
  const activeWeights = data.active_weights || {};

  if (!totalEquity) {
    detail.textContent = "포트폴리오 데이터 없음";
    if (allocationChart) {
      allocationChart.destroy();
      allocationChart = null;
    }
    return;
  }

  // 전략별 배분 계산
  const strategyNames = Object.keys(activeWeights);
  const labels = strategyNames.length
    ? strategyNames
    : ["FA", "현금"];
  const values = strategyNames.length
    ? strategyNames.map((s) => (activeWeights[s] || 0) * totalEquity)
    : [fa?.usd || 0, totalEquity - (fa?.usd || 0)];
  const bgColors = strategyNames.length
    ? strategyNames.map((_, i) => Object.values(REGIME_COLORS)[i % 5])
    : [REGIME_COLORS.trending_up, "#455A64"];

  if (allocationChart) {
    allocationChart.destroy();
    allocationChart = null;
  }

  allocationChart = new Chart(ctx, {
    type: "doughnut",
    data: {
      labels,
      datasets: [
        {
          data: values,
          backgroundColor: bgColors,
          borderColor: "#1a1a2e",
          borderWidth: 2,
        },
      ],
    },
    options: {
      responsive: false,
      plugins: {
        legend: {
          position: "bottom",
          labels: { color: "#E0E0E0", padding: 10 },
        },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const val = ctx.raw;
              const pct = totalEquity > 0 ? ((val / totalEquity) * 100).toFixed(1) : "0";
              return ` $${val.toLocaleString()} (${pct}%)`;
            },
          },
        },
      },
    },
  });

  // 상세 텍스트
  const rows = labels
    .map((l, i) => {
      const v = values[i];
      const pct = totalEquity > 0 ? ((v / totalEquity) * 100).toFixed(1) : "0";
      return `<div class="alloc-row">
        <span class="alloc-dot" style="background:${bgColors[i]}"></span>
        <span class="alloc-label">${l}</span>
        <span class="alloc-val">$${v.toLocaleString(undefined, { maximumFractionDigits: 0 })} (${pct}%)</span>
      </div>`;
    })
    .join("");
  detail.innerHTML = `
    <div class="alloc-total">총 자산: <strong>$${totalEquity.toLocaleString(undefined, { maximumFractionDigits: 0 })}</strong></div>
    ${rows}
  `;
}

// ── 섹션 6: 전환 이력 테이블 ─────────────────────────────────

async function renderHistoryTable() {
  try {
    const data = await fetch(
      "/api/internal/regime/transition-history?limit=50"
    ).then((r) => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return r.json();
    });

    const transitions = data.transitions || [];
    const stats = data.stats || {};

    // 통계 박스
    const statsEl = document.getElementById("history-stats");
    const s24 = stats.last_24h || { total: 0, confirmed: 0, fake: 0 };
    const s7d = stats.last_7d || { total: 0, confirmed: 0, fake: 0 };
    statsEl.innerHTML = `
      <div class="stat-group">
        <span class="stat-label">최근 24h</span>
        <span class="stat-item">전환 <strong>${s24.total}</strong></span>
        <span class="stat-item">확정 <strong>${s24.confirmed}</strong></span>
        <span class="stat-item fake">미확정 <strong>${s24.fake}</strong></span>
      </div>
      <div class="stat-group">
        <span class="stat-label">최근 7일</span>
        <span class="stat-item">전환 <strong>${s7d.total}</strong></span>
        <span class="stat-item">확정 <strong>${s7d.confirmed}</strong></span>
        <span class="stat-item fake">미확정 <strong>${s7d.fake}</strong></span>
      </div>
    `;

    // 테이블 행
    const tbody = document.getElementById("history-table-body");
    if (transitions.length === 0) {
      tbody.innerHTML =
        '<tr><td colspan="6" class="no-data">전환 이력 없음</td></tr>';
      return;
    }

    tbody.innerHTML = transitions
      .map(
        (t, idx) => `
      <tr class="${t.confirmed ? "" : "unconfirmed"}">
        <td>${idx + 1}</td>
        <td>${formatDatetime(t.detected_at)}</td>
        <td><span class="regime-badge ${regimeToCssClass(t.previous_regime)}">${regimeLabel(t.previous_regime)}</span></td>
        <td><span class="regime-badge ${regimeToCssClass(t.new_regime)}">${regimeLabel(t.new_regime)}</span></td>
        <td>${t.transition_type || "—"}</td>
        <td>${t.confirmed ? '<span class="badge-yes">확정</span>' : '<span class="badge-no">미확정</span>'}</td>
      </tr>
    `
      )
      .join("");
  } catch (err) {
    console.error("[regime] renderHistoryTable error:", err);
    document.getElementById("history-table-body").innerHTML =
      '<tr><td colspan="6" class="no-data">로드 실패</td></tr>';
  }
}

// ── 초기화 + 폴링 ────────────────────────────────────────────

async function init() {
  console.log("[regime-dashboard] Initializing...");

  // 모든 섹션 초기 렌더링 (병렬)
  await Promise.allSettled([
    renderRawTimeline(),
    renderConfirmedChart(),
    renderHistoryTable(),
    updateCurrentStatus(),
  ]);

  console.log("[regime-dashboard] Initial render complete");

  // 5초마다 현재 상태 갱신
  setInterval(updateCurrentStatus, 5000);

  // 5분마다 타임라인/차트/이력 갱신
  setInterval(() => {
    Promise.allSettled([
      renderRawTimeline(),
      renderConfirmedChart(),
      renderHistoryTable(),
    ]);
  }, 300000);
}

document.addEventListener("DOMContentLoaded", init);
