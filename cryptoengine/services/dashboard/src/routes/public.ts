/**
 * Public API routes (port 3001) — delayed data (10-minute lag).
 *
 * GET /api/performance   — daily/weekly/monthly returns
 * GET /api/equity-curve  — cumulative equity curve
 * GET /api/stats         — Sharpe, max drawdown, uptime, strategy count
 */

import { Router, Request, Response } from "express";
import { Pool } from "pg";
import Redis from "ioredis";

/** Delay in minutes applied to all public data. */
const PUBLIC_DELAY_MINUTES = 10;

export function createPublicRouter(pool: Pool, redis: Redis): Router {
  const router = Router();

  // ── GET /performance ────────────────────────────────────────

  router.get("/performance", async (req: Request, res: Response) => {
    try {
      const period = (req.query.period as string) || "daily";

      let query: string;
      switch (period) {
        case "weekly":
          query = `
            SELECT
              date_trunc('week', date) AS period,
              SUM(total_pnl) AS pnl,
              AVG(equity) AS avg_equity,
              MIN(equity) AS min_equity,
              MAX(equity) AS max_equity
            FROM daily_pnl
            WHERE date <= NOW() - INTERVAL '${PUBLIC_DELAY_MINUTES} minutes'
            GROUP BY period
            ORDER BY period DESC
            LIMIT 52
          `;
          break;
        case "monthly":
          query = `
            SELECT
              date_trunc('month', date) AS period,
              SUM(total_pnl) AS pnl,
              AVG(equity) AS avg_equity,
              MIN(equity) AS min_equity,
              MAX(equity) AS max_equity
            FROM daily_pnl
            WHERE date <= NOW() - INTERVAL '${PUBLIC_DELAY_MINUTES} minutes'
            GROUP BY period
            ORDER BY period DESC
            LIMIT 24
          `;
          break;
        default:
          query = `
            SELECT
              date AS period,
              total_pnl AS pnl,
              equity AS avg_equity,
              equity AS min_equity,
              equity AS max_equity
            FROM daily_pnl
            WHERE date <= NOW() - INTERVAL '${PUBLIC_DELAY_MINUTES} minutes'
            ORDER BY date DESC
            LIMIT 90
          `;
      }

      const result = await pool.query(query);

      return res.json({
        period,
        delay_minutes: PUBLIC_DELAY_MINUTES,
        data: result.rows,
      });
    } catch (err) {
      console.error("[public] /performance error:", err);
      return res.status(500).json({ error: "Failed to fetch performance" });
    }
  });

  // ── GET /equity-curve ───────────────────────────────────────

  router.get("/equity-curve", async (req: Request, res: Response) => {
    try {
      const days = parseInt((req.query.days as string) || "365", 10);

      const result = await pool.query(
        `
        SELECT date, equity, cumulative_pnl, drawdown_pct
        FROM daily_pnl
        WHERE date >= CURRENT_DATE - $1::int
          AND date <= NOW() - INTERVAL '${PUBLIC_DELAY_MINUTES} minutes'
        ORDER BY date ASC
        `,
        [days]
      );

      return res.json({
        delay_minutes: PUBLIC_DELAY_MINUTES,
        days,
        curve: result.rows,
      });
    } catch (err) {
      console.error("[public] /equity-curve error:", err);
      return res.status(500).json({ error: "Failed to fetch equity curve" });
    }
  });

  // ── GET /stats ──────────────────────────────────────────────

  router.get("/stats", async (_req: Request, res: Response) => {
    try {
      // Sharpe ratio and max drawdown from daily_pnl
      const statsResult = await pool.query(`
        WITH daily AS (
          SELECT
            total_pnl,
            drawdown_pct
          FROM daily_pnl
          WHERE date >= CURRENT_DATE - 90
            AND date <= NOW() - INTERVAL '${PUBLIC_DELAY_MINUTES} minutes'
        )
        SELECT
          CASE
            WHEN STDDEV(total_pnl) > 0
            THEN (AVG(total_pnl) / STDDEV(total_pnl)) * SQRT(252)
            ELSE 0
          END AS sharpe_ratio,
          COALESCE(MIN(drawdown_pct), 0) AS max_drawdown_pct,
          COUNT(*) AS trading_days
        FROM daily
      `);

      const stats = statsResult.rows[0] || {
        sharpe_ratio: 0,
        max_drawdown_pct: 0,
        trading_days: 0,
      };

      // Active strategy count from Redis
      const strategyKeys = await redis.keys("ce:strategy:status:*");
      let activeCount = 0;
      for (const key of strategyKeys) {
        const raw = await redis.get(key);
        if (raw) {
          const parsed = JSON.parse(raw);
          if (parsed.is_running) activeCount++;
        }
      }

      // System uptime
      const uptimeSeconds = Math.floor(process.uptime());

      return res.json({
        delay_minutes: PUBLIC_DELAY_MINUTES,
        sharpe_ratio: parseFloat(stats.sharpe_ratio) || 0,
        max_drawdown_pct: parseFloat(stats.max_drawdown_pct) || 0,
        trading_days: parseInt(stats.trading_days, 10) || 0,
        active_strategies: activeCount,
        total_strategies: strategyKeys.length,
        uptime_seconds: uptimeSeconds,
      });
    } catch (err) {
      console.error("[public] /stats error:", err);
      return res.status(500).json({ error: "Failed to fetch stats" });
    }
  });

  return router;
}
