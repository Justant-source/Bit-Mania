/**
 * Supertrend Expected-vs-Actual API
 *
 * GET /api/internal/supertrend/expected   — per-bar expected signals
 * GET /api/internal/supertrend/actual     — paired actual fills (round-trips)
 * GET /api/internal/supertrend/compare    — expected vs actual diff per trade
 * GET /api/internal/supertrend/equity     — expected vs actual equity curve
 * GET /api/internal/supertrend/status     — live position + latest signal
 * GET /api/internal/supertrend/candles    — 4h OHLCV for charting
 */

import { Router, Request, Response } from "express";
import { Pool } from "pg";
import Redis from "ioredis";

const STRATEGY_ID = "supertrend-01";
const SYMBOL = "BTC/USDT:USDT";

export function createSupertrendRouter(pool: Pool, redis: Redis): Router {
  const router = Router();

  // ── GET /expected ────────────────────────────────────────────────────
  // Returns per-bar expected entry/exit decisions from supertrend_signals.

  router.get("/expected", async (req: Request, res: Response) => {
    try {
      const from = (req.query.from as string) || "30 days";
      const to = (req.query.to as string) || "now";
      const fromClause = from.includes("-") ? `'${from}'::timestamptz` : `NOW() - INTERVAL '${from}'`;
      const toClause = to === "now" ? "NOW()" : `'${to}'::timestamptz`;

      const result = await pool.query(`
        SELECT
          bar_ts,
          computed_at,
          st_dir,
          round(fast_ema::numeric, 2) AS fast_ema,
          round(slow_ema::numeric, 2) AS slow_ema,
          round(dir_ema::numeric, 2)  AS dir_ema,
          round(price::numeric, 2)    AS price,
          round(atr_14::numeric, 2)   AS atr_14,
          allocated_capital,
          had_position,
          entry_ok,
          exit_signal,
          exit_reason,
          expected_action,
          round(expected_qty::numeric, 6)       AS expected_qty,
          round(expected_stop_loss::numeric, 2) AS expected_stop_loss
        FROM supertrend_signals
        WHERE bar_ts BETWEEN ${fromClause} AND ${toClause}
        ORDER BY bar_ts ASC
      `);

      return res.json({ signals: result.rows, count: result.rowCount });
    } catch (err) {
      console.error("[supertrend] /expected error:", err);
      return res.status(500).json({ error: "Failed to fetch expected signals" });
    }
  });

  // ── GET /actual ──────────────────────────────────────────────────────
  // Returns round-trip trades from the runtime orders table.
  // Pairs buy(reduce_only=false) → sell(reduce_only=true) chronologically.
  // Exit reason joined from service_logs (exit_order_submitted context).

  router.get("/actual", async (req: Request, res: Response) => {
    try {
      const from = (req.query.from as string) || "30 days";
      const to = (req.query.to as string) || "now";
      const fromClause = from.includes("-") ? `'${from}'::timestamptz` : `NOW() - INTERVAL '${from}'`;
      const toClause = to === "now" ? "NOW()" : `'${to}'::timestamptz`;

      // Fetch all filled orders for this strategy in the window
      const ordersRes = await pool.query(`
        SELECT
          id, request_id, order_id, side, filled_qty, filled_price, fee,
          reduce_only, status, created_at, updated_at
        FROM orders
        WHERE strategy_id = $1
          AND status IN ('filled', 'closed', 'partial')
          AND created_at BETWEEN ${fromClause} AND ${toClause}
        ORDER BY created_at ASC
      `, [STRATEGY_ID]);

      const orders = ordersRes.rows;

      // Pair entries (buy, reduce_only=false) with exits (sell, reduce_only=true)
      const entries = orders.filter((o: any) => o.side === "buy" && !o.reduce_only);
      const exits = orders.filter((o: any) => o.side === "sell" && o.reduce_only);

      const trips: any[] = [];
      let exitIdx = 0;

      for (const entry of entries) {
        // Find the next exit after this entry
        while (exitIdx < exits.length && exits[exitIdx].created_at <= entry.created_at) {
          exitIdx++;
        }
        const exit = exitIdx < exits.length ? exits[exitIdx] : null;

        // Fetch exit reason from service_logs
        let actualExitReason: string | null = null;
        if (exit) {
          try {
            const logRes = await pool.query(`
              SELECT context->>'reason' AS reason
              FROM service_logs
              WHERE service = 'supertrend'
                AND event = 'exit_order_submitted'
                AND timestamp BETWEEN $1 - INTERVAL '5 minutes' AND $1 + INTERVAL '5 minutes'
              ORDER BY timestamp DESC
              LIMIT 1
            `, [exit.updated_at]);
            if (logRes.rows[0]) actualExitReason = logRes.rows[0].reason;
          } catch { /* non-critical */ }

          const entryPrice = parseFloat(entry.filled_price);
          const exitPrice = parseFloat(exit.filled_price);
          const qty = parseFloat(entry.filled_qty);
          const fees = parseFloat(entry.fee || 0) + parseFloat(exit.fee || 0);
          const realizedPnl = (exitPrice - entryPrice) * qty - fees;

          trips.push({
            entry_order_id: entry.order_id,
            exit_order_id: exit.order_id,
            entry_time: entry.created_at,
            exit_time: exit.created_at,
            entry_fill_time: entry.updated_at,
            exit_fill_time: exit.updated_at,
            entry_price: entryPrice,
            exit_price: exitPrice,
            qty: qty,
            total_fees: fees,
            realized_pnl: realizedPnl,
            exit_reason: actualExitReason,
          });
          exitIdx++;
        } else {
          // Open position — no exit yet
          trips.push({
            entry_order_id: entry.order_id,
            exit_order_id: null,
            entry_time: entry.created_at,
            exit_time: null,
            entry_fill_time: entry.updated_at,
            exit_fill_time: null,
            entry_price: parseFloat(entry.filled_price),
            exit_price: null,
            qty: parseFloat(entry.filled_qty),
            total_fees: parseFloat(entry.fee || 0),
            realized_pnl: null,
            exit_reason: null,
            is_open: true,
          });
        }
      }

      return res.json({ trades: trips, count: trips.length });
    } catch (err) {
      console.error("[supertrend] /actual error:", err);
      return res.status(500).json({ error: "Failed to fetch actual trades" });
    }
  });

  // ── GET /compare ─────────────────────────────────────────────────────
  // Aligns expected signals with actual fills per trade, computes diffs.

  router.get("/compare", async (req: Request, res: Response) => {
    try {
      const from = (req.query.from as string) || "30 days";
      const to = (req.query.to as string) || "now";

      // Fetch from both endpoints
      const [expRes, actRes] = await Promise.all([
        pool.query(`
          SELECT bar_ts, expected_action, price AS expected_price,
                 expected_qty, expected_stop_loss, exit_reason AS expected_exit_reason
          FROM supertrend_signals
          WHERE expected_action IN ('enter','exit')
            AND bar_ts >= NOW() - INTERVAL '${from.includes("-") ? "30 days" : from}'
          ORDER BY bar_ts ASC
        `),
        pool.query(`
          SELECT id, request_id, side, filled_qty, filled_price, fee,
                 reduce_only, created_at, updated_at
          FROM orders
          WHERE strategy_id = $1
            AND status IN ('filled', 'closed', 'partial')
          ORDER BY created_at ASC
        `, [STRATEGY_ID]),
      ]);

      const expected = expRes.rows;
      const actOrders = actRes.rows;

      // Build actual entries/exits list
      const actEntries = actOrders.filter((o: any) => o.side === "buy" && !o.reduce_only);
      const actExits = actOrders.filter((o: any) => o.side === "sell" && o.reduce_only);

      const compared: any[] = [];
      let entryIdx = 0;
      let exitIdx = 0;

      for (const exp of expected) {
        const expTs = new Date(exp.bar_ts).getTime();
        // Allow 4h window: signal at bar_ts, fill should be within the same bar
        const windowEnd = expTs + 14_400_000;

        if (exp.expected_action === "enter") {
          const act = actEntries[entryIdx];
          const actTs = act ? new Date(act.updated_at).getTime() : null;
          const matched = act && actTs! >= expTs && actTs! <= windowEnd;

          compared.push({
            bar_ts: exp.bar_ts,
            expected_action: "enter",
            expected_price: parseFloat(exp.expected_price),
            expected_qty: exp.expected_qty ? parseFloat(exp.expected_qty) : null,
            actual_price: matched ? parseFloat(act.filled_price) : null,
            actual_qty: matched ? parseFloat(act.filled_qty) : null,
            actual_fill_time: matched ? act.updated_at : null,
            timing_lag_ms: matched ? actTs! - expTs : null,
            slippage_pct: matched
              ? ((parseFloat(act.filled_price) - parseFloat(exp.expected_price)) / parseFloat(exp.expected_price)) * 100
              : null,
            qty_diff: matched && exp.expected_qty
              ? parseFloat(act.filled_qty) - parseFloat(exp.expected_qty)
              : null,
            status: matched ? "matched" : "missed",
          });

          if (matched) entryIdx++;
        } else if (exp.expected_action === "exit") {
          const act = actExits[exitIdx];
          const actTs = act ? new Date(act.updated_at).getTime() : null;
          const matched = act && actTs! >= expTs && actTs! <= windowEnd;

          compared.push({
            bar_ts: exp.bar_ts,
            expected_action: "exit",
            expected_exit_reason: exp.expected_exit_reason,
            expected_price: parseFloat(exp.expected_price),
            actual_price: matched ? parseFloat(act.filled_price) : null,
            actual_qty: matched ? parseFloat(act.filled_qty) : null,
            actual_fill_time: matched ? act.updated_at : null,
            timing_lag_ms: matched ? actTs! - expTs : null,
            slippage_pct: matched
              ? ((parseFloat(act.filled_price) - parseFloat(exp.expected_price)) / parseFloat(exp.expected_price)) * 100
              : null,
            status: matched ? "matched" : "missed",
          });

          if (matched) exitIdx++;
        }
      }

      // Extra actual fills not matched to any expected signal
      const extraEntries = actEntries.slice(entryIdx).map((o: any) => ({
        bar_ts: o.created_at,
        expected_action: "enter",
        status: "extra",
        actual_price: parseFloat(o.filled_price),
        actual_qty: parseFloat(o.filled_qty),
      }));
      const extraExits = actExits.slice(exitIdx).map((o: any) => ({
        bar_ts: o.created_at,
        expected_action: "exit",
        status: "extra",
        actual_price: parseFloat(o.filled_price),
        actual_qty: parseFloat(o.filled_qty),
      }));

      const all = [...compared, ...extraEntries, ...extraExits].sort(
        (a, b) => new Date(a.bar_ts).getTime() - new Date(b.bar_ts).getTime()
      );

      const stats = {
        total: all.length,
        matched: all.filter((r) => r.status === "matched").length,
        missed: all.filter((r) => r.status === "missed").length,
        extra: all.filter((r) => r.status === "extra").length,
        avg_slippage_pct: (() => {
          const slippages = all.filter((r) => r.slippage_pct !== null && r.slippage_pct !== undefined).map((r) => r.slippage_pct as number);
          return slippages.length ? slippages.reduce((a, b) => a + b, 0) / slippages.length : 0;
        })(),
        avg_lag_ms: (() => {
          const lags = all.filter((r) => r.timing_lag_ms !== null).map((r) => r.timing_lag_ms as number);
          return lags.length ? lags.reduce((a, b) => a + b, 0) / lags.length : 0;
        })(),
      };

      return res.json({ comparison: all, stats });
    } catch (err) {
      console.error("[supertrend] /compare error:", err);
      return res.status(500).json({ error: "Failed to compare signals" });
    }
  });

  // ── GET /equity ──────────────────────────────────────────────────────
  // Expected equity curve (replay signals) vs actual (portfolio_snapshots).

  router.get("/equity", async (req: Request, res: Response) => {
    try {
      const days = parseInt((req.query.days as string) || "90", 10);

      const [sigRes, snapRes] = await Promise.all([
        pool.query(`
          SELECT
            bar_ts,
            expected_action,
            expected_qty,
            price,
            allocated_capital
          FROM supertrend_signals
          WHERE bar_ts >= NOW() - $1::int * INTERVAL '1 day'
          ORDER BY bar_ts ASC
        `, [days]),
        pool.query(`
          SELECT snapshot_at, total_equity
          FROM portfolio_snapshots
          WHERE snapshot_at >= NOW() - $1::int * INTERVAL '1 day'
          ORDER BY snapshot_at ASC
        `, [days]),
      ]);

      // Reconstruct expected equity curve from signals
      const signals = sigRes.rows;
      let expectedEquity = signals[0]?.allocated_capital ?? 60;
      let inPosition = false;
      let entryPrice = 0;
      let posQty = 0;
      const expectedCurve: { ts: string; equity: number }[] = [];

      for (const s of signals) {
        if (s.expected_action === "enter" && !inPosition) {
          inPosition = true;
          entryPrice = parseFloat(s.price);
          posQty = s.expected_qty ? parseFloat(s.expected_qty) : 0;
        } else if (s.expected_action === "exit" && inPosition) {
          const exitPrice = parseFloat(s.price);
          const pnl = (exitPrice - entryPrice) * posQty;
          expectedEquity += pnl;
          inPosition = false;
          posQty = 0;
        }
        expectedCurve.push({ ts: s.bar_ts, equity: parseFloat(expectedEquity.toFixed(2)) });
      }

      return res.json({
        expected: expectedCurve,
        actual: snapRes.rows.map((r) => ({ ts: r.snapshot_at, equity: parseFloat(r.total_equity) })),
        days,
      });
    } catch (err) {
      console.error("[supertrend] /equity error:", err);
      return res.status(500).json({ error: "Failed to fetch equity curves" });
    }
  });

  // ── GET /status ──────────────────────────────────────────────────────
  // Live: latest signal row + live position from Redis.

  router.get("/status", async (_req: Request, res: Response) => {
    try {
      // Latest signal row
      const sigRes = await pool.query(`
        SELECT
          bar_ts, computed_at, st_dir,
          round(fast_ema::numeric, 2) AS fast_ema,
          round(slow_ema::numeric, 2) AS slow_ema,
          round(dir_ema::numeric, 2)  AS dir_ema,
          round(price::numeric, 2)    AS price,
          round(atr_14::numeric, 2)   AS atr_14,
          allocated_capital, had_position, entry_ok,
          exit_signal, exit_reason, expected_action,
          round(expected_qty::numeric, 6)       AS expected_qty,
          round(expected_stop_loss::numeric, 2) AS expected_stop_loss
        FROM supertrend_signals
        ORDER BY bar_ts DESC
        LIMIT 1
      `);

      // Live strategy status from Redis
      let strategyStatus: any = null;
      try {
        const raw = await redis.get(`strategy:status:${STRATEGY_ID}`);
        if (raw) strategyStatus = JSON.parse(raw);
      } catch { /* Redis optional */ }

      // Live position from Redis cache
      let livePosition: any = null;
      try {
        const posRaw = await redis.get("ce:positions:all");
        if (posRaw) {
          const positions = JSON.parse(posRaw);
          livePosition = Array.isArray(positions)
            ? positions.find((p: any) => p.symbol === SYMBOL || p.symbol === "BTCUSDT") || null
            : null;
        }
      } catch { /* Redis optional */ }

      // Next 4h bar ETA
      const now = Date.now();
      const _4H_MS = 4 * 60 * 60 * 1000;
      const nextBarTs = Math.ceil(now / _4H_MS) * _4H_MS;
      const nextBarEtaMs = nextBarTs - now;

      return res.json({
        latest_signal: sigRes.rows[0] || null,
        strategy_status: strategyStatus,
        live_position: livePosition,
        next_bar_ts: new Date(nextBarTs).toISOString(),
        next_bar_eta_ms: nextBarEtaMs,
      });
    } catch (err) {
      console.error("[supertrend] /status error:", err);
      return res.status(500).json({ error: "Failed to fetch status" });
    }
  });

  // ── GET /candles ─────────────────────────────────────────────────────
  // 4h OHLCV from ohlcv_history for charting.

  router.get("/candles", async (req: Request, res: Response) => {
    try {
      const days = parseInt((req.query.days as string) || "90", 10);
      const result = await pool.query(`
        SELECT
          timestamp AS ts,
          round(open::numeric, 2)   AS open,
          round(high::numeric, 2)   AS high,
          round(low::numeric, 2)    AS low,
          round(close::numeric, 2)  AS close,
          round(volume::numeric, 4) AS volume
        FROM ohlcv_history
        WHERE exchange = 'bybit'
          AND symbol = 'BTCUSDT'
          AND timeframe = '4h'
          AND timestamp >= NOW() - $1::int * INTERVAL '1 day'
        ORDER BY timestamp ASC
      `, [days]);

      return res.json({ candles: result.rows, count: result.rowCount });
    } catch (err) {
      console.error("[supertrend] /candles error:", err);
      return res.status(500).json({ error: "Failed to fetch candles" });
    }
  });

  return router;
}
