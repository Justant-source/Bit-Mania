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

// Per-service log freshness thresholds (seconds)
const SERVICE_THRESHOLDS: Record<string, { green: number; yellow: number }> = {
  "market-data":            { green: 3 * 3600,      yellow: 6 * 3600 },   // logs hourly-ish
  "execution-engine":       { green: 30,             yellow: 120 },
  "supertrend":             { green: 4 * 3600 + 600, yellow: 8 * 3600 },  // 4h bar cycle
  "strategy-orchestrator":  { green: 3 * 3600,       yellow: 6 * 3600 },
  "telegram-bot":           { green: 6 * 3600,       yellow: 12 * 3600 }, // event-driven
};

// Redis key fallback for services that may not write to service_logs
const REDIS_HEARTBEAT_KEYS: Record<string, string> = {
  "supertrend":            "strategy:status:supertrend-01",
  "strategy-orchestrator": "orchestrator:state",
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

      // Running max drawdown over the period (window fn must be in subquery)
      const ddRes = await pool.query(`
        SELECT MIN(drawdown_pct) AS max_drawdown
        FROM (
          SELECT
            (total_equity - MAX(total_equity) OVER (ORDER BY snapshot_at))
            / NULLIF(MAX(total_equity) OVER (ORDER BY snapshot_at), 0) AS drawdown_pct
          FROM portfolio_snapshots
          WHERE snapshot_at >= NOW() - $1::int * INTERVAL '1 day'
        ) sub
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

      // Add known services missing from service_logs; try Redis heartbeat fallback
      for (const svc of Object.keys(SERVICE_THRESHOLDS)) {
        const existing = services.find((s: any) => s.service === svc);
        const redisKey = REDIS_HEARTBEAT_KEYS[svc];

        if ((!existing || existing.status === "red") && redisKey) {
          try {
            const raw = await redis.get(redisKey);
            if (raw) {
              const parsed = JSON.parse(raw);
              const ts = parsed.computed_at || parsed.last_tick || parsed.updated_at || parsed.timestamp || parsed.bar_ts;
              if (ts) {
                const staleSec = Math.round((now - new Date(ts).getTime()) / 1000);
                const thresholds = SERVICE_THRESHOLDS[svc];
                const status =
                  staleSec <= thresholds.green ? "green" :
                  staleSec <= thresholds.yellow ? "yellow" : "red";

                if (!existing) {
                  services.push({ service: svc, last_log: ts, stale_sec: staleSec, status, errors_6h: 0 });
                } else if (status !== "red") {
                  existing.status   = status;
                  existing.stale_sec = staleSec;
                  existing.last_log  = ts;
                }
              }
            }
          } catch { /* non-critical */ }
        }

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

  // ── GET /infra/history ───────────────────────────────────────────────
  // Returns 24h of 5-minute snapshots stored in Redis sorted set.

  router.get("/infra/history", async (req: Request, res: Response) => {
    const key = req.query.key as string;
    const validKeys = ["cpu", "mem", "disk", "redis"];
    if (!validKeys.includes(key)) {
      return res.status(400).json({ error: "key must be one of: cpu, mem, disk, redis" });
    }
    try {
      const raw = await redis.zrange(`ce:infra:history:${key}`, 0, -1);
      const history: Array<{ ts: string; val: number }> = [];
      for (const s of raw) {
        try { history.push(JSON.parse(s)); } catch { /* skip malformed */ }
      }
      return res.json({ key, history });
    } catch (err) {
      console.error("[monitor] /infra/history error:", err);
      return res.status(500).json({ error: "Failed to fetch infra history" });
    }
  });

  return router;
}
