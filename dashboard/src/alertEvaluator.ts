/**
 * Alert Evaluator — replaces Grafana's 9 unified-alerting rules.
 *
 * Evaluates conditions every 60s and publishes to Redis "ce:alerts:grafana"
 * (the same channel the telegram-bot already consumes from Grafana webhooks),
 * so safety alerting survives Grafana removal.
 *
 * Alert conditions ported from config/grafana/alerting/alert_rules.yaml.
 */

import { Pool } from "pg";
import Redis from "ioredis";

const EVAL_INTERVAL_MS = 60_000;
const PROMETHEUS_URL = process.env.PROMETHEUS_URL || "http://prometheus:9090";

// Track which alerts are currently firing to avoid repeat notifications
const _firing = new Set<string>();

async function promQuery(expr: string): Promise<number | null> {
  try {
    const url = `${PROMETHEUS_URL}/api/v1/query?query=${encodeURIComponent(expr)}`;
    const res = await fetch(url);
    if (!res.ok) return null;
    const json: any = await res.json();
    if (json.status === "success" && json.data?.result?.length > 0) {
      return parseFloat(json.data.result[0].value[1]);
    }
    return null;
  } catch {
    return null;
  }
}

async function fireAlert(
  redis: Redis,
  uid: string,
  severity: "warning" | "critical",
  title: string,
  message: string
): Promise<void> {
  if (_firing.has(uid)) return; // already firing, don't spam
  _firing.add(uid);

  const payload = JSON.stringify({
    uid,
    title,
    message,
    severity,
    fired_at: new Date().toISOString(),
    source: "alert-evaluator",
  });

  try {
    await redis.publish("ce:alerts:grafana", payload);
    console.log(`[alert] FIRED ${severity.toUpperCase()}: ${title}`);
  } catch (err) {
    console.error("[alert] Redis publish error:", err);
  }
}

function clearAlert(uid: string): void {
  if (_firing.has(uid)) {
    _firing.delete(uid);
    console.log(`[alert] RESOLVED: ${uid}`);
  }
}

async function evaluate(pool: Pool, redis: Redis): Promise<void> {
  // ── Rule 1: 펀딩비 급변 (Funding Rate Spike) ──────────────────────
  try {
    const res = await pool.query(`
      SELECT MAX(ABS(rate)) AS value
      FROM funding_rate_history
      WHERE timestamp >= NOW() - INTERVAL '1 hour'
    `);
    const val = res.rows[0]?.value ? parseFloat(res.rows[0].value) : 0;
    if (val > 0.0003) {
      await fireAlert(redis, "alert-funding-spike", "warning", "펀딩비 급변",
        `펀딩비 ${(val * 100).toFixed(4)}%로 임계값(0.03%)을 초과`);
    } else {
      clearAlert("alert-funding-spike");
    }
  } catch { /* table may not exist */ }

  // ── Rule 2: 자산 급감 (Equity Drop >3% in 15 min) ─────────────────
  try {
    const res = await pool.query(`
      SELECT
        COALESCE(
          1 - (
            SELECT total_equity FROM portfolio_snapshots ORDER BY snapshot_at DESC LIMIT 1
          ) / NULLIF(
            (SELECT total_equity FROM portfolio_snapshots WHERE snapshot_at >= NOW() - INTERVAL '15 minutes' ORDER BY snapshot_at ASC LIMIT 1),
            0
          ),
          0
        ) AS drop_ratio
    `);
    const drop = res.rows[0]?.drop_ratio ? parseFloat(res.rows[0].drop_ratio) : 0;
    if (drop > 0.03) {
      await fireAlert(redis, "alert-equity-drop", "critical", "자산 급감",
        `15분 내 자산 ${(drop * 100).toFixed(1)}% 급감 감지`);
    } else {
      clearAlert("alert-equity-drop");
    }
  } catch { /* table may not exist */ }

  // ── Rule 3: 데이터 수집 중단 (OHLCV gap >10 min) ─────────────────
  try {
    const res = await pool.query(`
      SELECT EXTRACT(EPOCH FROM (NOW() - MAX(timestamp))) AS gap_sec
      FROM ohlcv_history
      WHERE exchange = 'bybit' AND symbol = 'BTCUSDT'
    `);
    const gapSec = res.rows[0]?.gap_sec ? parseFloat(res.rows[0].gap_sec) : 0;
    if (gapSec > 600) {
      await fireAlert(redis, "alert-ohlcv-stall", "warning", "데이터 수집 중단",
        `OHLCV 수집 중단: 마지막 업데이트 ${Math.round(gapSec / 60)}분 전`);
    } else {
      clearAlert("alert-ohlcv-stall");
    }
  } catch { /* ok */ }

  // ── Rule 4: Kill Switch 발동 ──────────────────────────────────────
  try {
    const res = await pool.query(`
      SELECT COUNT(*) AS cnt
      FROM kill_switch_events
      WHERE triggered_at >= NOW() - INTERVAL '5 minutes'
    `);
    const cnt = parseInt(res.rows[0]?.cnt || "0", 10);
    if (cnt > 0) {
      await fireAlert(redis, "alert-kill-switch", "critical", "Kill Switch 발동",
        `Kill Switch가 최근 5분 내 ${cnt}건 발동되었습니다`);
    } else {
      clearAlert("alert-kill-switch");
    }
  } catch { /* ok */ }

  // ── Rule 5: Max Drawdown (>10% over period) ────────────────────────
  try {
    const res = await pool.query(`
      SELECT MIN(
        (total_equity - peak) / NULLIF(peak, 0)
      ) AS max_dd
      FROM (
        SELECT total_equity,
               MAX(total_equity) OVER (ORDER BY snapshot_at) AS peak
        FROM portfolio_snapshots
        WHERE snapshot_at >= NOW() - INTERVAL '30 days'
      ) sub
    `);
    const dd = res.rows[0]?.max_dd ? parseFloat(res.rows[0].max_dd) : 0;
    if (dd < -0.10) {
      await fireAlert(redis, "alert-drawdown", "warning", "낙폭 과다",
        `최대 낙폭 ${(dd * 100).toFixed(1)}% 초과`);
    } else {
      clearAlert("alert-drawdown");
    }
  } catch { /* ok */ }

  // ── Rules 6-9: Prometheus infrastructure alerts ───────────────────

  // CPU >85%
  const cpu = await promQuery('100 - (avg(irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)');
  if (cpu !== null && cpu > 85) {
    await fireAlert(redis, "alert-cpu", "warning", "CPU 과다 사용",
      `CPU 사용률 ${cpu.toFixed(1)}% (임계값 85%)`);
  } else {
    clearAlert("alert-cpu");
  }

  // Memory <15% available
  const memUsed = await promQuery('(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100');
  if (memUsed !== null && memUsed > 85) {
    await fireAlert(redis, "alert-memory", "critical", "메모리 부족",
      `메모리 사용률 ${memUsed.toFixed(1)}% (임계값 85%)`);
  } else {
    clearAlert("alert-memory");
  }

  // Disk root <10% free
  const diskUsed = await promQuery('(node_filesystem_size_bytes{mountpoint="/"} - node_filesystem_avail_bytes{mountpoint="/"}) / node_filesystem_size_bytes{mountpoint="/"} * 100');
  if (diskUsed !== null && diskUsed > 90) {
    await fireAlert(redis, "alert-disk", "warning", "디스크 부족",
      `디스크 사용률 ${diskUsed.toFixed(1)}% (임계값 90%)`);
  } else {
    clearAlert("alert-disk");
  }

  // Redis memory >80%
  const redisMem = await promQuery('redis_memory_used_bytes / redis_memory_max_bytes * 100');
  if (redisMem !== null && redisMem > 80) {
    await fireAlert(redis, "alert-redis-mem", "warning", "Redis 메모리 과다",
      `Redis 메모리 사용 ${redisMem.toFixed(1)}% (임계값 80%)`);
  } else {
    clearAlert("alert-redis-mem");
  }
}

export function startAlertEvaluator(pool: Pool, redis: Redis): void {
  console.log("[alert-evaluator] Starting (60s interval)");

  const run = async () => {
    try {
      await evaluate(pool, redis);
    } catch (err) {
      console.error("[alert-evaluator] Evaluation error:", err);
    }
  };

  // Initial run after 10s, then every 60s
  setTimeout(run, 10_000);
  setInterval(run, EVAL_INTERVAL_MS);
}
