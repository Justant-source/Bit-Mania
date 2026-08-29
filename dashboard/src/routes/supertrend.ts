/**
 * Supertrend Expected-vs-Actual API
 *
 * GET /api/internal/supertrend/expected   — per-bar expected signals
 * GET /api/internal/supertrend/actual     — paired actual fills (round-trips)
 * GET /api/internal/supertrend/compare    — expected vs actual diff per trade
 * GET /api/internal/supertrend/equity     — expected vs actual equity curve
 * GET /api/internal/supertrend/status     — live position + latest signal
 * GET /api/internal/supertrend/candles    — 4h OHLCV for charting
 * GET /api/internal/supertrend/candles/in-progress — unconfirmed 4h bar from Redis
 */

import { Router, Request, Response } from "express";
import { Pool } from "pg";
import Redis from "ioredis";

const STRATEGY_ID = "supertrend-01";
const SYMBOL = "BTC/USDT:USDT";

/** Parse ?from= / ?to= query params into safe parameterized-query values.
 *  Returns { days } for interval-based queries, or { ts } for absolute timestamps.
 *  Never interpolates user input directly into SQL.
 */
function parseTimeParam(raw: string | undefined, defaultDays = 30): { days: number } | { ts: string } {
  if (!raw) return { days: defaultDays };
  // Absolute ISO date: "2024-01-01" or "2024-01-01T00:00:00Z"
  if (/^\d{4}-\d{2}-\d{2}/.test(raw)) {
    const d = new Date(raw);
    if (!isNaN(d.getTime())) return { ts: d.toISOString() };
  }
  // "30 days" or "30" or "30d"
  const m = raw.match(/^(\d+)/);
  if (m) return { days: Math.min(Math.max(1, parseInt(m[1], 10)), 365) };
  return { days: defaultDays };
}

export function createSupertrendRouter(pool: Pool, redis: Redis): Router {
  const router = Router();

  // ── GET /expected ────────────────────────────────────────────────────

  router.get("/expected", async (req: Request, res: Response) => {
    try {
      const fromP = parseTimeParam(req.query.from as string, 30);
      const toP   = parseTimeParam(req.query.to   as string,  0);

      let sql: string;
      let params: any[];

      const SELECT_COLS = `SELECT bar_ts, computed_at, st_dir, round(st_line::numeric,2) AS st_line,
                 round(fast_ema::numeric,2) AS fast_ema, round(slow_ema::numeric,2) AS slow_ema,
                 round(dir_ema::numeric,2)  AS dir_ema,  round(price::numeric,2)    AS price,
                 round(atr_14::numeric,2)   AS atr_14,   allocated_capital,
                 had_position, entry_ok, exit_signal, exit_reason, expected_action,
                 round(expected_qty::numeric,6)       AS expected_qty,
                 round(expected_stop_loss::numeric,2) AS expected_stop_loss
               FROM supertrend_signals`;

      if ("ts" in fromP && "ts" in toP) {
        sql = `${SELECT_COLS} WHERE bar_ts BETWEEN $1::timestamptz AND $2::timestamptz ORDER BY bar_ts ASC`;
        params = [fromP.ts, toP.ts];
      } else if ("ts" in fromP) {
        sql = `${SELECT_COLS} WHERE bar_ts >= $1::timestamptz ORDER BY bar_ts ASC`;
        params = [fromP.ts];
      } else {
        const days = "days" in fromP ? fromP.days : 30;
        sql = `${SELECT_COLS} WHERE bar_ts >= NOW() - $1::int * INTERVAL '1 day' ORDER BY bar_ts ASC`;
        params = [days];
      }

      const result = await pool.query(sql, params);
      return res.json({ signals: result.rows, count: result.rowCount });
    } catch (err) {
      console.error("[supertrend] /expected error:", err);
      return res.status(500).json({ error: "Failed to fetch expected signals" });
    }
  });

  // ── GET /actual ──────────────────────────────────────────────────────

  router.get("/actual", async (req: Request, res: Response) => {
    try {
      const fromP = parseTimeParam(req.query.from as string, 30);
      const days  = "days" in fromP ? fromP.days : 30;
      const fromTs = "ts" in fromP ? fromP.ts : null;

      const ordersRes = fromTs
        ? await pool.query(`
            SELECT id, request_id, order_id, side, filled_qty, filled_price, fee,
                   reduce_only, status, created_at, updated_at
            FROM orders
            WHERE strategy_id = $1
              AND status IN ('filled', 'closed', 'partial')
              AND created_at >= $2::timestamptz
            ORDER BY created_at ASC
          `, [STRATEGY_ID, fromTs])
        : await pool.query(`
            SELECT id, request_id, order_id, side, filled_qty, filled_price, fee,
                   reduce_only, status, created_at, updated_at
            FROM orders
            WHERE strategy_id = $1
              AND status IN ('filled', 'closed', 'partial')
              AND created_at >= NOW() - $2::int * INTERVAL '1 day'
            ORDER BY created_at ASC
          `, [STRATEGY_ID, days]);

      const orders = ordersRes.rows;
      const entries = orders.filter((o: any) => o.side === "buy" && !o.reduce_only);
      const exits   = orders.filter((o: any) => o.side === "sell" && o.reduce_only);

      const trips: any[] = [];
      let exitIdx = 0;

      for (const entry of entries) {
        while (exitIdx < exits.length && exits[exitIdx].created_at <= entry.created_at) {
          exitIdx++;
        }
        const exit = exitIdx < exits.length ? exits[exitIdx] : null;

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

          const entryPrice  = parseFloat(entry.filled_price);
          const exitPrice   = parseFloat(exit.filled_price);
          const qty         = parseFloat(entry.filled_qty);
          const fees        = parseFloat(entry.fee || 0) + parseFloat(exit.fee || 0);
          const realizedPnl = (exitPrice - entryPrice) * qty - fees;

          trips.push({
            entry_order_id: entry.order_id, exit_order_id: exit.order_id,
            entry_time: entry.created_at,   exit_time: exit.created_at,
            entry_fill_time: entry.updated_at, exit_fill_time: exit.updated_at,
            entry_price: entryPrice, exit_price: exitPrice,
            qty, total_fees: fees, realized_pnl: realizedPnl, exit_reason: actualExitReason,
          });
          exitIdx++;
        } else {
          trips.push({
            entry_order_id: entry.order_id, exit_order_id: null,
            entry_time: entry.created_at, exit_time: null,
            entry_fill_time: entry.updated_at, exit_fill_time: null,
            entry_price: parseFloat(entry.filled_price), exit_price: null,
            qty: parseFloat(entry.filled_qty),
            total_fees: parseFloat(entry.fee || 0),
            realized_pnl: null, exit_reason: null, is_open: true,
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

  router.get("/compare", async (req: Request, res: Response) => {
    try {
      const fromP = parseTimeParam(req.query.from as string, 30);
      const days  = "days" in fromP ? fromP.days : 30;
      const fromTs = "ts" in fromP ? fromP.ts : null;

      const [expRes, actRes] = await Promise.all([
        fromTs
          ? pool.query(`
              SELECT bar_ts, expected_action, price AS expected_price,
                     expected_qty, expected_stop_loss, exit_reason AS expected_exit_reason,
                     actual_exit_price, actual_exit_at, delay_note
              FROM supertrend_signals
              WHERE expected_action IN ('enter','exit')
                AND bar_ts >= $1::timestamptz
              ORDER BY bar_ts ASC
            `, [fromTs])
          : pool.query(`
              SELECT bar_ts, expected_action, price AS expected_price,
                     expected_qty, expected_stop_loss, exit_reason AS expected_exit_reason,
                     actual_exit_price, actual_exit_at, delay_note
              FROM supertrend_signals
              WHERE expected_action IN ('enter','exit')
                AND bar_ts >= NOW() - $1::int * INTERVAL '1 day'
              ORDER BY bar_ts ASC
            `, [days]),
        pool.query(`
          SELECT id, request_id, side, filled_qty, filled_price, fee,
                 reduce_only, created_at, updated_at, original_signal_ts
          FROM orders
          WHERE strategy_id = $1
            AND status IN ('filled', 'closed', 'partial', 'filled_delayed')
          ORDER BY created_at ASC
        `, [STRATEGY_ID]),
      ]);

      const expected  = expRes.rows;
      const actOrders = actRes.rows;

      const actEntries = actOrders.filter((o: any) => o.side === "buy" && !o.reduce_only);
      const actExits   = actOrders.filter((o: any) => o.side === "sell" && o.reduce_only);

      // One expected signal can match MULTIPLE actual fills (split fills).
      // All fills within [barClose, barClose+4h] are "matched". No "extra" status.
      const compared: any[] = [];
      let entryCursor = 0;
      let exitCursor  = 0;

      for (const exp of expected) {
        const expTs     = new Date(exp.bar_ts).getTime();
        const barClose  = expTs + 14_400_000;       // signal fires at bar close
        const windowEnd = barClose + 14_400_000;    // accept fills up to 4h after bar close

        if (exp.expected_action === "enter") {
          // skip fills before bar close (belong to a prior signal)
          while (entryCursor < actEntries.length &&
                 new Date(actEntries[entryCursor].updated_at).getTime() < barClose) {
            entryCursor++;
          }
          // collect all fills within window
          const fills: any[] = [];
          while (entryCursor < actEntries.length) {
            const actTs = new Date(actEntries[entryCursor].updated_at).getTime();
            if (actTs > windowEnd) break;
            fills.push({ act: actEntries[entryCursor], actTs });
            entryCursor++;
          }

          if (fills.length === 0) {
            compared.push({
              bar_ts: exp.bar_ts, expected_action: "enter",
              expected_price: parseFloat(exp.expected_price),
              expected_qty: exp.expected_qty ? parseFloat(exp.expected_qty) : null,
              actual_price: null, actual_qty: null, actual_fill_time: null,
              timing_lag_ms: null, slippage_pct: null, qty_diff: null,
              status: "missed",
            });
          } else {
            for (const { act, actTs } of fills) {
              compared.push({
                bar_ts: exp.bar_ts, expected_action: "enter",
                expected_price: parseFloat(exp.expected_price),
                expected_qty: exp.expected_qty ? parseFloat(exp.expected_qty) : null,
                actual_price:    parseFloat(act.filled_price),
                actual_qty:      parseFloat(act.filled_qty),
                actual_fill_time: act.updated_at,
                timing_lag_ms:   actTs - barClose,
                slippage_pct:    ((parseFloat(act.filled_price) - parseFloat(exp.expected_price)) / parseFloat(exp.expected_price)) * 100,
                qty_diff: exp.expected_qty ? parseFloat(act.filled_qty) - parseFloat(exp.expected_qty) : null,
                status: "matched",
              });
            }
          }
        } else if (exp.expected_action === "exit") {
          // Delayed execution path: actual_exit_at recorded directly on the signal row.
          // Skip the timing window — use the stored values as a direct match.
          if (exp.actual_exit_at && exp.actual_exit_price) {
            const actualTs  = new Date(exp.actual_exit_at).getTime();
            const lagMs     = actualTs - barClose;
            // Find the matching order by original_signal_ts for actual_qty.
            // original_signal_ts = bar_ts of the signal row.
            const barTsMs = new Date(exp.bar_ts).getTime();
            const delayedOrder = actExits.find((o: any) =>
              o.original_signal_ts &&
              Math.abs(new Date(o.original_signal_ts).getTime() - barTsMs) < 3_600_000
            );
            compared.push({
              bar_ts: exp.bar_ts, expected_action: "exit",
              expected_exit_reason: exp.expected_exit_reason,
              expected_price: parseFloat(exp.expected_price),
              expected_qty: null,
              actual_price:    parseFloat(exp.actual_exit_price),
              actual_qty:      delayedOrder ? parseFloat(delayedOrder.filled_qty) : null,
              actual_fill_time: exp.actual_exit_at,
              timing_lag_ms:   lagMs,
              slippage_pct:    ((parseFloat(exp.actual_exit_price) - parseFloat(exp.expected_price)) / parseFloat(exp.expected_price)) * 100,
              delay_note:      exp.delay_note ?? null,
              status: "matched",
            });
          } else {
            // Normal path: match fills within [barClose, barClose+4h]
            while (exitCursor < actExits.length &&
                   new Date(actExits[exitCursor].updated_at).getTime() < barClose) {
              exitCursor++;
            }
            const fills: any[] = [];
            while (exitCursor < actExits.length) {
              const actTs = new Date(actExits[exitCursor].updated_at).getTime();
              if (actTs > windowEnd) break;
              fills.push({ act: actExits[exitCursor], actTs });
              exitCursor++;
            }

            if (fills.length === 0) {
              compared.push({
                bar_ts: exp.bar_ts, expected_action: "exit",
                expected_exit_reason: exp.expected_exit_reason,
                expected_price: parseFloat(exp.expected_price),
                expected_qty: null,
                actual_price: null, actual_qty: null, actual_fill_time: null,
                timing_lag_ms: null, slippage_pct: null, delay_note: null,
                status: "missed",
              });
            } else {
              for (const { act, actTs } of fills) {
                compared.push({
                  bar_ts: exp.bar_ts, expected_action: "exit",
                  expected_exit_reason: exp.expected_exit_reason,
                  expected_price: parseFloat(exp.expected_price),
                  expected_qty: null,
                  actual_price:    parseFloat(act.filled_price),
                  actual_qty:      parseFloat(act.filled_qty),
                  actual_fill_time: act.updated_at,
                  timing_lag_ms:   actTs - barClose,
                  slippage_pct:    ((parseFloat(act.filled_price) - parseFloat(exp.expected_price)) / parseFloat(exp.expected_price)) * 100,
                  delay_note: null,
                  status: "matched",
                });
              }
            }
          }
        }
      }

      const all = compared.sort(
        (a, b) => new Date(a.bar_ts).getTime() - new Date(b.bar_ts).getTime()
      );

      const slippages = all.filter((r) => r.slippage_pct != null).map((r) => r.slippage_pct as number);
      const lags      = all.filter((r) => r.timing_lag_ms != null).map((r) => r.timing_lag_ms as number);

      return res.json({
        comparison: all,
        stats: {
          total:   all.length,
          matched: all.filter((r) => r.status === "matched").length,
          missed:  all.filter((r) => r.status === "missed").length,
          avg_slippage_pct: slippages.length ? slippages.reduce((a, b) => a + b, 0) / slippages.length : 0,
          avg_lag_ms:       lags.length      ? lags.reduce((a, b) => a + b, 0)      / lags.length      : 0,
        },
      });
    } catch (err) {
      console.error("[supertrend] /compare error:", err);
      return res.status(500).json({ error: "Failed to compare signals" });
    }
  });

  // ── GET /equity ──────────────────────────────────────────────────────

  router.get("/equity", async (req: Request, res: Response) => {
    try {
      const fromP  = parseTimeParam(req.query.from as string || req.query.days as string, 90);
      const fromTs = "ts" in fromP ? fromP.ts : null;
      const days   = "days" in fromP ? fromP.days : 90;

      const [sigRes, snapRes] = await Promise.all([
        fromTs
          ? pool.query(`
              SELECT bar_ts, expected_action, expected_qty, price, allocated_capital
              FROM supertrend_signals
              WHERE bar_ts >= $1::timestamptz
              ORDER BY bar_ts ASC
            `, [fromTs])
          : pool.query(`
              SELECT bar_ts, expected_action, expected_qty, price, allocated_capital
              FROM supertrend_signals
              WHERE bar_ts >= NOW() - $1::int * INTERVAL '1 day'
              ORDER BY bar_ts ASC
            `, [days]),
        fromTs
          ? pool.query(`
              SELECT snapshot_at, total_equity
              FROM portfolio_snapshots
              WHERE snapshot_at >= $1::timestamptz
              ORDER BY snapshot_at ASC
            `, [fromTs])
          : pool.query(`
              SELECT snapshot_at, total_equity
              FROM portfolio_snapshots
              WHERE snapshot_at >= NOW() - $1::int * INTERVAL '1 day'
              ORDER BY snapshot_at ASC
            `, [days]),
      ]);

      const signals = sigRes.rows;
      // Phase 5: $200 USDT initial capital (2026-05-18 live start)
      const INITIAL_EQUITY = 200;
      let expectedEquity = INITIAL_EQUITY;
      let inPosition = false;
      let entryPrice = 0;
      let posQty     = 0;
      const expectedCurve: { ts: string; equity: number }[] = [];

      for (const s of signals) {
        if (s.expected_action === "enter" && !inPosition) {
          inPosition = true;
          entryPrice = parseFloat(s.price);
          posQty     = s.expected_qty ? parseFloat(s.expected_qty) : 0;
        } else if (s.expected_action === "exit" && inPosition) {
          const exitPrice = parseFloat(s.price);
          expectedEquity += (exitPrice - entryPrice) * posQty;
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

  router.get("/status", async (_req: Request, res: Response) => {
    try {
      const sigRes = await pool.query(`
        SELECT bar_ts, computed_at, st_dir,
          round(fast_ema::numeric,2) AS fast_ema, round(slow_ema::numeric,2) AS slow_ema,
          round(dir_ema::numeric,2)  AS dir_ema,  round(price::numeric,2)    AS price,
          round(atr_14::numeric,2)   AS atr_14,   allocated_capital,
          had_position, entry_ok, exit_signal, exit_reason, expected_action,
          round(expected_qty::numeric,6)       AS expected_qty,
          round(expected_stop_loss::numeric,2) AS expected_stop_loss
        FROM supertrend_signals
        ORDER BY bar_ts DESC
        LIMIT 1
      `);

      let strategyStatus: any = null;
      try {
        const raw = await redis.get(`strategy:status:${STRATEGY_ID}`);
        if (raw) strategyStatus = JSON.parse(raw);
      } catch { /* Redis optional */ }

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

      const snapRes = await pool.query(`
        SELECT total_equity FROM portfolio_snapshots ORDER BY snapshot_at DESC LIMIT 1
      `);
      const totalEquity = snapRes.rows[0] ? parseFloat(snapRes.rows[0].total_equity) : null;

      const now       = Date.now();
      const _4H_MS    = 4 * 60 * 60 * 1000;
      const nextBarTs = Math.ceil(now / _4H_MS) * _4H_MS;

      return res.json({
        latest_signal:    sigRes.rows[0] || null,
        strategy_status:  strategyStatus,
        live_position:    livePosition,
        total_equity:     totalEquity,
        next_bar_ts:      new Date(nextBarTs).toISOString(),
        next_bar_eta_ms:  nextBarTs - now,
      });
    } catch (err) {
      console.error("[supertrend] /status error:", err);
      return res.status(500).json({ error: "Failed to fetch status" });
    }
  });

  // ── GET /candles ─────────────────────────────────────────────────────

  router.get("/candles", async (req: Request, res: Response) => {
    try {
      const fromP  = parseTimeParam(req.query.from as string || req.query.days as string, 90);
      const fromTs = "ts" in fromP ? fromP.ts : null;
      const days   = "days" in fromP ? fromP.days : 90;

      const result = fromTs
        ? await pool.query(`
            SELECT timestamp AS ts,
              round(open::numeric,2)   AS open,  round(high::numeric,2)   AS high,
              round(low::numeric,2)    AS low,   round(close::numeric,2)  AS close,
              round(volume::numeric,4) AS volume
            FROM ohlcv_history
            WHERE exchange = 'bybit'
              AND symbol = 'BTCUSDT'
              AND timeframe = '4h'
              AND timestamp >= $1::timestamptz
            ORDER BY timestamp ASC
          `, [fromTs])
        : await pool.query(`
            SELECT timestamp AS ts,
              round(open::numeric,2)   AS open,  round(high::numeric,2)   AS high,
              round(low::numeric,2)    AS low,   round(close::numeric,2)  AS close,
              round(volume::numeric,4) AS volume
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

  // Current in-progress 4h bar from Redis (Bybit unconfirmed kline.240).
  router.get("/candles/in-progress", async (_req: Request, res: Response) => {
    try {
      const now = Date.now();
      const bucketStartMs = Math.floor(now / (4 * 3600_000)) * (4 * 3600_000);
      const cache = await redis.hgetall("cache:ohlcv:bybit:BTCUSDT:4h");
      if (!cache || !cache.ts || !cache.close) {
        return res.json({ candle: null, source: "empty" });
      }
      const cacheTs = parseInt(cache.ts, 10);
      if (cacheTs !== bucketStartMs) {
        return res.json({ candle: null, source: "empty" });
      }
      const candle = {
        ts:             bucketStartMs,
        open:           parseFloat(cache.open),
        high:           parseFloat(cache.high),
        low:            parseFloat(cache.low),
        close:          parseFloat(cache.close),
        volume:         parseFloat(cache.volume || "0"),
        confirmed:      cache.confirmed === "1",
      };
      return res.json({ candle, source: "redis" });
    } catch (err) {
      console.error("[supertrend] /candles/in-progress error:", err);
      return res.status(500).json({ error: "Failed to fetch in-progress candle" });
    }
  });

  return router;
}
