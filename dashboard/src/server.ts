import express, { Request, Response, NextFunction } from "express";
import cors from "cors";
import path from "path";
import { Pool } from "pg";
import Redis from "ioredis";
import { createInternalRouter } from "./routes/internal";
import { createSupertrendRouter } from "./routes/supertrend";
import { createMonitorRouter } from "./routes/monitor";
import { startAlertEvaluator } from "./alertEvaluator";

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
const PORT = parseInt(process.env.DASHBOARD_PORT || "3000", 10);
const API_KEY = process.env.DASHBOARD_API_KEY || "";

function apiKeyAuth(req: Request, res: Response, next: NextFunction): void {
  if (!API_KEY) {
    next();
    return;
  }
  const provided =
    (req.headers["x-api-key"] as string | undefined) ||
    (req.query.api_key as string | undefined);
  if (provided && provided === API_KEY) {
    next();
    return;
  }
  res.status(401).json({ error: "Unauthorized", message: "Valid X-Api-Key header required" });
}

async function main(): Promise<void> {
  console.log("[dashboard] Starting Bit-Mania Dashboard...");

  const pool = new Pool(DB_CONFIG);
  await pool.query("SELECT 1");
  console.log("[dashboard] PostgreSQL connected");

  const redis = new Redis(REDIS_URL);
  await redis.ping();
  console.log("[dashboard] Redis connected");

  const app = express();
  app.use(cors());
  app.use(express.json());

  app.use("/api", apiKeyAuth, createInternalRouter(pool, redis));
  app.use("/api/internal/supertrend", apiKeyAuth, createSupertrendRouter(pool, redis));
  app.use("/api/internal/monitor", apiKeyAuth, createMonitorRouter(pool, redis));

  app.use(express.static(path.join(__dirname, "../public")));

  app.get("/", (_req, res) => {
    res.sendFile(path.join(__dirname, "../public/supertrend.html"));
  });
  app.get("/supertrend", (_req, res) => {
    res.sendFile(path.join(__dirname, "../public/supertrend.html"));
  });
  app.get("/monitor", (_req, res) => {
    res.sendFile(path.join(__dirname, "../public/monitor.html"));
  });

  app.get("/health", (_req, res) => {
    res.json({ status: "ok", service: "dashboard" });
  });

  startAlertEvaluator(pool, redis);

  app.listen(PORT, () => {
    console.log(`[dashboard] Listening on port ${PORT}`);
  });

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
