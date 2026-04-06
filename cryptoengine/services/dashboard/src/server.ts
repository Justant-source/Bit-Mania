/**
 * CryptoEngine Dashboard Server
 *
 * Runs two Express servers:
 * - Port 3000 (internal): Full access to positions, PnL, strategies, system health
 * - Port 3001 (public):   Delayed performance data (10-minute delay)
 */

import express from "express";
import cors from "cors";
import path from "path";
import { Pool } from "pg";
import Redis from "ioredis";
import { createInternalRouter } from "./routes/internal";
import { createPublicRouter } from "./routes/public";
import { createRegimeRouter } from "./routes/regime";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_CONFIG = {
  host: process.env.DB_HOST || "localhost",
  port: parseInt(process.env.DB_PORT || "5432", 10),
  database: process.env.DB_NAME || "cryptoengine",
  user: process.env.DB_USER || "cryptoengine",
  password: process.env.DB_PASSWORD || "cryptoengine",
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
};

const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";
const INTERNAL_PORT = parseInt(process.env.DASHBOARD_INTERNAL_PORT || "3000", 10);
const PUBLIC_PORT = parseInt(process.env.DASHBOARD_PUBLIC_PORT || "3001", 10);

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("[dashboard] Starting CryptoEngine Dashboard...");

  // Database pool
  const pool = new Pool(DB_CONFIG);
  await pool.query("SELECT 1");
  console.log("[dashboard] PostgreSQL connected");

  // Redis client
  const redis = new Redis(REDIS_URL);
  await redis.ping();
  console.log("[dashboard] Redis connected");

  // --- Internal server (port 3000) ---
  const internalApp = express();
  internalApp.use(cors());
  internalApp.use(express.json());

  internalApp.use("/api", createInternalRouter(pool, redis));

  // Regime dashboard static files
  internalApp.use(express.static(path.join(__dirname, "../public")));

  // Regime API routes
  internalApp.use("/api/internal/regime", createRegimeRouter(pool, redis));

  // Regime page
  internalApp.get("/regime", (_req, res) => {
    res.sendFile(path.join(__dirname, "../public/regime.html"));
  });

  internalApp.get("/health", (_req, res) => {
    res.json({ status: "ok", service: "dashboard-internal" });
  });

  internalApp.listen(INTERNAL_PORT, () => {
    console.log(`[dashboard] Internal API listening on port ${INTERNAL_PORT}`);
  });

  // --- Public server (port 3001) ---
  const publicApp = express();
  publicApp.use(cors());
  publicApp.use(express.json());

  publicApp.use("/api", createPublicRouter(pool, redis));

  publicApp.get("/health", (_req, res) => {
    res.json({ status: "ok", service: "dashboard-public" });
  });

  publicApp.listen(PUBLIC_PORT, () => {
    console.log(`[dashboard] Public API listening on port ${PUBLIC_PORT}`);
  });

  // --- Graceful shutdown ---
  const shutdown = async (signal: string) => {
    console.log(`[dashboard] ${signal} received, shutting down...`);
    redis.disconnect();
    await pool.end();
    process.exit(0);
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

main().catch((err) => {
  console.error("[dashboard] Fatal error:", err);
  process.exit(1);
});
