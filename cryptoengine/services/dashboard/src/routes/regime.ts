/**
 * Regime API routes — /api/internal/regime/*
 *
 * GET /current              — Redis 기반 현재 레짐 상태
 * GET /raw-timeline         — regime_raw_log 기반 원시 레짐 타임라인
 * GET /confirmed-timeline   — regime_transitions + ohlcv_history 기반 확정 레짐 + BTC 가격
 * GET /transition-history   — regime_transitions 이력 + 통계
 */

import { Router, Request, Response } from "express";
import { Pool } from "pg";
import Redis from "ioredis";

export function createRegimeRouter(pool: Pool, redis: Redis): Router {
  const router = Router();

  // ── GET /current ────────────────────────────────────────────
  router.get("/current", async (_req: Request, res: Response) => {
    try {
      // Redis에서 레짐 관련 데이터 조회
      const [regimeHash, weightTransitionRaw, orchestratorStateRaw] =
        await Promise.all([
          redis.hgetall("cache:regime"),
          redis.get("orchestrator:weight_transition"),
          redis.get("orchestrator:state"),
        ]);

      const regime = regimeHash ?? {};
      const weightTransition = weightTransitionRaw
        ? JSON.parse(weightTransitionRaw)
        : null;
      const orchestratorState = orchestratorStateRaw
        ? JSON.parse(orchestratorStateRaw)
        : null;

      // FA 배분 계산
      let faAllocation = { pct: 0, usd: 0 };
      if (orchestratorState) {
        const weights = orchestratorState.weights ?? {};
        const faPct = (weights["funding-arb"] ?? weights["fa"] ?? 0) * 100;
        const totalEquity = orchestratorState.total_equity ?? 0;
        faAllocation = {
          pct: Math.round(faPct * 100) / 100,
          usd: Math.round(totalEquity * (faPct / 100) * 100) / 100,
        };
      }

      return res.json({
        raw_regime: regime.regime ?? null,
        raw_regime_at: regime.detected_at ?? null,
        confirmed_regime: regime.confirmed_regime ?? null,
        confirmed_at: regime.confirmed_at ?? null,
        consecutive_count: regime.consecutive
          ? parseInt(regime.consecutive, 10)
          : 0,
        confidence: regime.confidence ? parseFloat(regime.confidence) : null,
        indicators: {
          adx: regime.adx ? parseFloat(regime.adx) : null,
          bb_width: regime.bb_width ? parseFloat(regime.bb_width) : null,
          volatility: regime.volatility ? parseFloat(regime.volatility) : null,
        },
        weight_transition: weightTransition
          ? {
              in_progress: weightTransition.in_progress ?? false,
              current_step: weightTransition.current_step ?? 0,
              total_steps: weightTransition.total_steps ?? 0,
              previous_weights: weightTransition.previous_weights ?? {},
              target_weights: weightTransition.target_weights ?? {},
              current_weights: weightTransition.current_weights ?? {},
              started_at: weightTransition.started_at ?? null,
            }
          : null,
        active_regime: orchestratorState?.regime ?? null,
        active_weights: orchestratorState?.weights ?? null,
        total_equity: orchestratorState?.total_equity ?? null,
        fa_allocation: faAllocation,
        timestamp: regime.detected_at ?? new Date().toISOString(),
      });
    } catch (err) {
      console.error("[regime] /current error:", err);
      return res.status(500).json({ error: "Failed to fetch current regime" });
    }
  });

  // ── GET /raw-timeline ───────────────────────────────────────
  router.get("/raw-timeline", async (req: Request, res: Response) => {
    try {
      const hours = parseInt((req.query.hours as string) || "24", 10);

      let rows: any[] = [];
      try {
        const result = await pool.query(
          `
          SELECT id, detected_at, regime, confidence, adx, bb_width,
                 is_confirmed, consecutive_count
          FROM regime_raw_log
          WHERE detected_at >= NOW() - $1 * INTERVAL '1 hour'
            AND symbol = 'BTCUSDT'
          ORDER BY detected_at ASC
          `,
          [hours]
        );
        rows = result.rows;
      } catch (dbErr: any) {
        // 테이블이 없을 수 있으므로 빈 배열 반환
        console.warn("[regime] regime_raw_log table not found, returning empty:", dbErr.message);
        rows = [];
      }

      // 가짜 전환 감지: i번째가 i-1, i+1과 다른 경우 is_fake=true
      const timeline = rows.map((row, i) => {
        let isFake = false;
        if (i > 0 && i < rows.length - 1) {
          const prev = rows[i - 1].regime;
          const next = rows[i + 1].regime;
          if (row.regime !== prev && row.regime !== next) {
            isFake = true;
          }
        }
        return {
          ...row,
          is_fake: isFake,
        };
      });

      // 요약 통계
      const regimeCounts: Record<string, number> = {};
      let fakeCount = 0;
      for (const item of timeline) {
        regimeCounts[item.regime] = (regimeCounts[item.regime] ?? 0) + 1;
        if (item.is_fake) fakeCount++;
      }

      const total = timeline.length;
      const regimeRatios: Record<string, number> = {};
      for (const [regime, count] of Object.entries(regimeCounts)) {
        regimeRatios[regime] = total > 0 ? Math.round((count / total) * 1000) / 10 : 0;
      }

      // 전환 횟수 계산
      let totalTransitions = 0;
      for (let i = 1; i < timeline.length; i++) {
        if (timeline[i].regime !== timeline[i - 1].regime) {
          totalTransitions++;
        }
      }

      return res.json({
        timeline,
        summary: {
          regime_ratios: regimeRatios,
          total_transitions: totalTransitions,
          fake_transitions: fakeCount,
          total_points: total,
          hours,
        },
      });
    } catch (err) {
      console.error("[regime] /raw-timeline error:", err);
      return res.status(500).json({ error: "Failed to fetch raw timeline" });
    }
  });

  // ── GET /confirmed-timeline ─────────────────────────────────
  router.get("/confirmed-timeline", async (req: Request, res: Response) => {
    try {
      const days = parseInt((req.query.days as string) || "7", 10);

      let periods: any[] = [];
      try {
        const transResult = await pool.query(
          `
          SELECT previous_regime, new_regime, detected_at, confirmed_at, transition_type
          FROM regime_transitions
          WHERE detected_at >= NOW() - $1 * INTERVAL '1 day'
            AND symbol = 'BTCUSDT'
            AND confirmed = true
          ORDER BY detected_at ASC
          `,
          [days]
        );
        periods = transResult.rows;
      } catch (dbErr: any) {
        console.warn("[regime] regime_transitions table not found:", dbErr.message);
        periods = [];
      }

      let ohlcv: any[] = [];
      try {
        const ohlcvResult = await pool.query(
          `
          SELECT timestamp, open, high, low, close, volume
          FROM ohlcv_history
          WHERE timestamp >= NOW() - $1 * INTERVAL '1 day'
            AND symbol = 'BTCUSDT'
            AND timeframe = '1h'
          ORDER BY timestamp ASC
          `,
          [days]
        );
        ohlcv = ohlcvResult.rows;
      } catch (dbErr: any) {
        console.warn("[regime] ohlcv_history table not found:", dbErr.message);
        ohlcv = [];
      }

      return res.json({
        periods,
        ohlcv,
        days,
      });
    } catch (err) {
      console.error("[regime] /confirmed-timeline error:", err);
      return res.status(500).json({ error: "Failed to fetch confirmed timeline" });
    }
  });

  // ── GET /transition-history ─────────────────────────────────
  router.get("/transition-history", async (req: Request, res: Response) => {
    try {
      const limit = parseInt((req.query.limit as string) || "50", 10);

      let transitions: any[] = [];
      try {
        const result = await pool.query(
          `
          SELECT id, previous_regime, new_regime, detected_at, confirmed_at,
                 transition_type, confirmed, consecutive_count
          FROM regime_transitions
          WHERE symbol = 'BTCUSDT'
          ORDER BY detected_at DESC
          LIMIT $1
          `,
          [limit]
        );
        transitions = result.rows;
      } catch (dbErr: any) {
        console.warn("[regime] regime_transitions table not found:", dbErr.message);
        transitions = [];
      }

      // 통계 계산
      let stats24h = { total: 0, confirmed: 0, fake: 0 };
      let stats7d = { total: 0, confirmed: 0, fake: 0 };

      try {
        const statsResult = await pool.query(`
          SELECT
            COUNT(*) FILTER (WHERE detected_at >= NOW() - INTERVAL '1 day') AS cnt_24h,
            COUNT(*) FILTER (WHERE confirmed = true AND detected_at >= NOW() - INTERVAL '1 day') AS confirmed_24h,
            COUNT(*) FILTER (WHERE detected_at >= NOW() - INTERVAL '7 days') AS cnt_7d,
            COUNT(*) FILTER (WHERE confirmed = true AND detected_at >= NOW() - INTERVAL '7 days') AS confirmed_7d
          FROM regime_transitions
          WHERE symbol = 'BTCUSDT'
        `);

        const row = statsResult.rows[0] ?? {};
        const c24h = parseInt(row.cnt_24h ?? "0", 10);
        const conf24h = parseInt(row.confirmed_24h ?? "0", 10);
        const c7d = parseInt(row.cnt_7d ?? "0", 10);
        const conf7d = parseInt(row.confirmed_7d ?? "0", 10);

        stats24h = { total: c24h, confirmed: conf24h, fake: c24h - conf24h };
        stats7d = { total: c7d, confirmed: conf7d, fake: c7d - conf7d };
      } catch (dbErr: any) {
        console.warn("[regime] stats query failed:", dbErr.message);
      }

      return res.json({
        transitions,
        stats: {
          last_24h: stats24h,
          last_7d: stats7d,
        },
        limit,
      });
    } catch (err) {
      console.error("[regime] /transition-history error:", err);
      return res.status(500).json({ error: "Failed to fetch transition history" });
    }
  });

  return router;
}
