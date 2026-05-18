/**
 * Monitor API — absorbed Grafana panel data
 *
 * GET /api/internal/monitor/portfolio    — equity, daily/MTD PnL, drawdown
 * GET /api/internal/monitor/killswitch   — kill-switch status + history
 * GET /api/internal/monitor/regime       — current regime + 7d timeline
 * GET /api/internal/monitor/positions    — open positions + strategy KPIs
 * GET /api/internal/monitor/service      — per-service health (log freshness)
 * GET /api/internal/monitor/infra        — CPU/mem/disk/Redis via Prometheus
 */

import { Router, Request, Response } from "express";
import { Pool } from "pg";
import Redis from "ioredis";

// Prometheus HTTP API client (simple fetch wrapper)
const PROMETHEUS_URL = process.env.PROMETHEUS_URL || "http://prometheus:9090";

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

// Per-service log freshness thresholds (seconds), from thresholds.md
const SERVICE_THRESHOLDS: Record<string, { green: number; yellow: number }> = {
  "market-data":         { green: 120,   yellow: 300 },
  "execution-engine":    { green: 30,    yellow: 120 },
  "supertrend":          { green: 120,   yellow: 300 },
  "strategy-orchestrator": { green: 120, yellow: 300 },
  "telegram-bot":        { green: 300,   yellow: 600 },
  "dashboard":           { green: 60,    yellow: 180 },
};

export function createMonitorRouter(pool: Pool, redis: Redis): Router {
  const router = Router();

  // ── GET /portfolio ────────────────────────────────────────────────────

  router.get("/portfolio", async (req: Request, res: Response) => {
    try {
      const days = parseInt((req.query.days as string) || "30", 10);

      const [snapRes, latestRes] = await Promise.all([
        pool.query(`
          SELECT
            snapshot_at,
            total_equity,
            realized_pnl_today,
            unrealized_pnl,
            daily_drawdown,
            sharpe_ratio_30d
          FROM portfolio_snapshots
          WHERE snapshot_at >= NOW() - $1::int * INTERVAL '1 day'
          ORDER BY snapshot_at ASC
        `, [days]),
        pool.query(`
          SELECT total_equity, realized_pnl_today, unrealized_pnl,
                 daily_drawdown, sharpe_ratio_30d, snapshot_at
          FROM portfolio_snapshots
          ORDER BY snapshot_at DESC LIMIT 1
        `),
      ]);

      // MTD PnL = current equity − first snapshot of current month
      const monthStartRes = await pool.query(`
        SELECT total_equity
        FROM portfolio_snapshots
        WHERE snapshot_at >= date_trunc('month', NOW())
        ORDER BY snapshot_at ASC LIMIT 1
      `);

      const latest = latestRes.rows[0] || {};
      const monthStart = monthStartRes.rows[0];
      const mtd_pnl = monthStart && latest.total_equity
        ? parseFloat(latest.total_equity) - parseFloat(monthStart.total_equity)
        : null;

      // Running max drawdown over the period
      const ddRes = await pool.query(`
        SELECT MIN(
          (total_equity - MAX(total_equity) OVER (ORDER BY snapshot_at)) / NULLIF(MAX(total_equity) OVER (ORDER BY snapshot_at), 0)
        ) AS max_drawdown
        FROM portfolio_snapshots
        WHERE snapshot_at >= NOW() - $1::int * INTERVAL '1 day'
      `, [days]);

      return res.json({
        latest: {
          equity: latest.total_equity ? parseFloat(latest.total_equity) : null,
          daily_pnl: latest.realized_pnl_today ? parseFloat(latest.realized_pnl_today) : null,
          unrealized_pnl: latest.unrealized_pnl ? parseFloat(latest.unrealized_pnl) : null,
          daily_drawdown: latest.daily_drawdown ? parseFloat(latest.daily_drawdown) : null,
          sharpe_30d: latest.sharpe_ratio_30d ? parseFloat(latest.sharpe_ratio_30d) : null,
          snapshot_at: latest.snapshot_at,
          mtd_pnl,
        },
        max_drawdown: ddRes.rows[0]?.max_drawdown ? parseFloat(ddRes.rows[0].max_drawdown) : null,
        curve: snapRes.rows.map((r) => ({
          ts: r.snapshot_at,
          equity: parseFloat(r.total_equity),
          daily_pnl: r.realized_pnl_today ? parseFloat(r.realized_pnl_today) : 0,
          drawdown: r.daily_drawdown ? parseFloat(r.daily_drawdown) : 0,
        })),
      });
    } catch (err) {
      console.error("[monitor] /portfolio error:", err);
      return res.status(500).json({ error: "Failed to fetch portfolio data" });
    }
  });

  // ── GET /killswitch ───────────────────────────────────────────────────

  router.get("/killswitch", async (_req: Request, res: Response) => {
    try {
      const [activeRes, historyRes, redisKs] = await Promise.all([
        pool.query(`
          SELECT id, level, reason, triggered_at, pnl_at_trigger
          FROM kill_switch_events
          WHERE resolved_at IS NULL
          ORDER BY triggered_at DESC
          LIMIT 1
        `),
        pool.query(`
          SELECT id, level, reason, triggered_at, resolved_at, pnl_at_trigger
          FROM kill_switch_events
          ORDER BY triggered_at DESC
          LIMIT 20
        `),
        redis.get("ce:kill_switch:active"),
      ]);

      return res.json({
        is_active: redisKs === "true" || activeRes.rows.length > 0,
        current_event: activeRes.rows[0] || null,
        history: historyRes.rows,
      });
    } catch (err) {
      console.error("[monitor] /killswitch error:", err);
      return res.status(500).json({ error: "Failed to fetch kill switch data" });
    }
  });

  // ── GET /regime ───────────────────────────────────────────────────────

  router.get("/regime", async (req: Request, res: Response) => {
    try {
      const days = parseInt((req.query.days as string) || "7", 10);

      const [currentRes, timelineRes, weightsRaw] = await Promise.all([
        pool.query(`
          SELECT new_regime AS regime, confidence, detected_at, confirmed_at,
                 indicators->>'adx' AS adx, indicators->>'bb_width' AS bb_width
          FROM regime_transitions
          WHERE confirmed = true
          ORDER BY confirmed_at DESC
          LIMIT 1
        `),
        pool.query(`
          SELECT new_regime AS regime, confirmed_at, previous_regime
          FROM regime_transitions
          WHERE confirmed = true
            AND confirmed_at >= NOW() - $1::int * INTERVAL '1 day'
          ORDER BY confirmed_at ASC
        `, [days]),
        redis.get("orchestrator:state"),
      ]);

      let weights = null;
      if (weightsRaw) {
        try { weights = JSON.parse(weightsRaw).weights; } catch { /* ignore */ }
      }

      return res.json({
        current: currentRes.rows[0] || null,
        timeline: timelineRes.rows,
        weights,
      });
    } catch (err) {
      console.error("[monitor] /regime error:", err);
      return res.status(500).json({ error: "Failed to fetch regime data" });
    }
  });

  // ── GET /positions ────────────────────────────────────────────────────

  router.get("/positions", async (_req: Request, res: Response) => {
    try {
      const [posRes, stratRes] = await Promise.all([
        // Runtime positions table (execution-engine schema, no strategy_id/closed_at)
        pool.query(`
          SELECT
            exchange, symbol, side, size, entry_price, unrealized_pnl,
            leverage, liquidation_price, margin_used, updated_at
          FROM positions
          ORDER BY updated_at DESC
        `),
        pool.query(`
          SELECT strategy_id, is_running, allocated_capital, current_pnl, position_count, updated_at
          FROM strategy_states
          ORDER BY strategy_id
        `),
      ]);

      // Compute liquidation distance for each position
      const positions = posRes.rows.map((p: any) => {
        const liqPrice = p.liquidation_price ? parseFloat(p.liquidation_price) : null;
        const entryPrice = p.entry_price ? parseFloat(p.entry_price) : null;
        let liqDistPct = null;
        if (liqPrice && entryPrice) {
          liqDistPct = ((entryPrice - liqPrice) / entryPrice) * 100;
        }
        return {
          ...p,
          liq_distance_pct: liqDistPct,
          size: p.size ? parseFloat(p.size) : null,
          entry_price: entryPrice,
          unrealized_pnl: p.unrealized_pnl ? parseFloat(p.unrealized_pnl) : null,
          leverage: p.leverage ? parseFloat(p.leverage) : null,
          liquidation_price: liqPrice,
          margin_used: p.margin_used ? parseFloat(p.margin_used) : null,
        };
      });

      return res.json({ positions, strategy_states: stratRes.rows });
    } catch (err) {
      console.error("[monitor] /positions error:", err);
      return res.status(500).json({ error: "Failed to fetch positions" });
    }
  });

  // ── GET /service ──────────────────────────────────────────────────────
  // Per-service health based on last log entry age vs thresholds.

  router.get("/service", async (_req: Request, res: Response) => {
    try {
      const result = await pool.query(`
        SELECT service, MAX(timestamp) AS last_log, COUNT(*) FILTER (WHERE level_no >= 40 AND timestamp >= NOW() - INTERVAL '6 hours') AS errors_6h
        FROM service_logs
        WHERE timestamp >= NOW() - INTERVAL '24 hours'
        GROUP BY service
        ORDER BY service
      `);

      const now = Date.now();
      const services = result.rows.map((r: any) => {
        const lastLog = new Date(r.last_log).getTime();
        const staleSec = Math.round((now - lastLog) / 1000);
        const thresholds = SERVICE_THRESHOLDS[r.service] || { green: 300, yellow: 600 };
        const status =
          staleSec <= thresholds.green ? "green" :
          staleSec <= thresholds.yellow ? "yellow" : "red";

        return {
          service: r.service,
          last_log: r.last_log,
          stale_sec: staleSec,
          status,
          errors_6h: parseInt(r.errors_6h, 10),
        };
      });

      // Add known services that have no log entries (dead)
      for (const svc of Object.keys(SERVICE_THRESHOLDS)) {
        if (!services.find((s: any) => s.service === svc)) {
          services.push({ service: svc, last_log: null, stale_sec: Infinity, status: "red", errors_6h: 0 });
        }
      }

      return res.json({ services });
    } catch (err) {
      console.error("[monitor] /service error:", err);
      return res.status(500).json({ error: "Failed to fetch service health" });
    }
  });

  // ── GET /infra ────────────────────────────────────────────────────────
  // Infrastructure metrics via Prometheus HTTP API.

  router.get("/infra", async (_req: Request, res: Response) => {
    try {
      const [cpu, mem, disk, redisMem] = await Promise.all([
        promQuery('100 - (avg(irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)'),
        promQuery('(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100'),
        promQuery('(node_filesystem_size_bytes{mountpoint="/"} - node_filesystem_avail_bytes{mountpoint="/"}) / node_filesystem_size_bytes{mountpoint="/"} * 100'),
        promQuery('redis_memory_used_bytes / redis_memory_max_bytes * 100'),
      ]);

      const threshold = (val: number | null, warn: number, crit: number) =>
        val === null ? "unknown" : val >= crit ? "red" : val >= warn ? "yellow" : "green";

      return res.json({
        cpu_pct: cpu !== null ? Math.round(cpu * 10) / 10 : null,
        cpu_status: threshold(cpu, 70, 85),
        mem_pct: mem !== null ? Math.round(mem * 10) / 10 : null,
        mem_status: threshold(mem, 80, 90),
        disk_pct: disk !== null ? Math.round(disk * 10) / 10 : null,
        disk_status: threshold(disk, 80, 90),
        redis_mem_pct: redisMem !== null ? Math.round(redisMem * 10) / 10 : null,
        redis_mem_status: threshold(redisMem, 70, 80),
        prometheus_url: PROMETHEUS_URL,
      });
    } catch (err) {
      console.error("[monitor] /infra error:", err);
      return res.status(500).json({ error: "Failed to fetch infra metrics" });
    }
  });

  return router;
}
