/**
 * Internal API routes (port 3000) — full, real-time access.
 *
 * GET /api/positions  — current open positions
 * GET /api/pnl        — PnL history
 * GET /api/strategies — strategy status
 * GET /api/system     — system health
 * GET /api/llm        — recent LLM analysis
 */

import { Router, Request, Response } from "express";
import { Pool } from "pg";
import Redis from "ioredis";

export function createInternalRouter(pool: Pool, redis: Redis): Router {
  const router = Router();

  // ── GET /positions ──────────────────────────────────────────

  router.get("/positions", async (_req: Request, res: Response) => {
    try {
      const cached = await redis.get("ce:positions:all");
      if (cached) {
        return res.json({ positions: JSON.parse(cached), source: "cache" });
      }

      const result = await pool.query(`
        SELECT
          exchange, symbol, side, size, entry_price,
          unrealized_pnl, leverage, liquidation_price, margin_used,
          strategy_id, opened_at
        FROM positions
        WHERE closed_at IS NULL
        ORDER BY opened_at DESC
      `);

      return res.json({ positions: result.rows, source: "db" });
    } catch (err) {
      console.error("[internal] /positions error:", err);
      return res.status(500).json({ error: "Failed to fetch positions" });
    }
  });

  // ── GET /pnl ────────────────────────────────────────────────

  router.get("/pnl", async (req: Request, res: Response) => {
    try {
      const days = parseInt((req.query.days as string) || "30", 10);
      const result = await pool.query(
        `
        SELECT
          date, realized_pnl, unrealized_pnl, total_pnl,
          cumulative_pnl, equity, drawdown_pct
        FROM daily_pnl
        WHERE date >= CURRENT_DATE - $1::int
        ORDER BY date ASC
        `,
        [days]
      );

      return res.json({ pnl: result.rows, days });
    } catch (err) {
      console.error("[internal] /pnl error:", err);
      return res.status(500).json({ error: "Failed to fetch PnL" });
    }
  });

  // ── GET /strategies ─────────────────────────────────────────

  router.get("/strategies", async (_req: Request, res: Response) => {
    try {
      // Read strategy status from Redis (set by each strategy heartbeat)
      const keys = await redis.keys("ce:strategy:status:*");
      const strategies = [];

      for (const key of keys) {
        const raw = await redis.get(key);
        if (raw) {
          strategies.push(JSON.parse(raw));
        }
      }

      return res.json({ strategies });
    } catch (err) {
      console.error("[internal] /strategies error:", err);
      return res.status(500).json({ error: "Failed to fetch strategies" });
    }
  });

  // ── GET /system ─────────────────────────────────────────────

  router.get("/system", async (_req: Request, res: Response) => {
    try {
      // Database health
      const dbStart = Date.now();
      await pool.query("SELECT 1");
      const dbLatencyMs = Date.now() - dbStart;

      // Redis health
      const redisStart = Date.now();
      await redis.ping();
      const redisLatencyMs = Date.now() - redisStart;

      // Kill switch status
      const killActive = await redis.get("ce:kill_switch:active");

      // Portfolio state
      const portfolioRaw = await redis.get("ce:portfolio:state");
      const portfolio = portfolioRaw ? JSON.parse(portfolioRaw) : null;

      // Last market data timestamp
      const lastTick = await redis.get("ce:market:last_tick");

      return res.json({
        status: "ok",
        uptime_seconds: Math.floor(process.uptime()),
        database: { connected: true, latency_ms: dbLatencyMs },
        redis: { connected: true, latency_ms: redisLatencyMs },
        kill_switch_active: killActive === "true",
        portfolio_equity: portfolio?.total_equity ?? null,
        last_market_tick: lastTick ?? null,
        memory: process.memoryUsage(),
      });
    } catch (err) {
      console.error("[internal] /system error:", err);
      return res.status(500).json({
        status: "error",
        error: "System health check failed",
      });
    }
  });

  // ── GET /llm ────────────────────────────────────────────────

  router.get("/llm", async (req: Request, res: Response) => {
    try {
      const limit = parseInt((req.query.limit as string) || "10", 10);
      const result = await pool.query(
        `
        SELECT
          id, analysis_type, prompt_summary, response_summary,
          confidence, recommended_action, created_at
        FROM llm_analyses
        ORDER BY created_at DESC
        LIMIT $1
        `,
        [limit]
      );

      return res.json({ analyses: result.rows });
    } catch (err) {
      console.error("[internal] /llm error:", err);
      return res.status(500).json({ error: "Failed to fetch LLM analyses" });
    }
  });

  return router;
}
