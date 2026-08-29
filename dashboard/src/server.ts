import express, { Request, Response, NextFunction } from "express";
import cors from "cors";
import path from "path";
import { Pool } from "pg";
import Redis from "ioredis";
import { createInternalRouter } from "./routes/internal";
import { createSupertrendRouter } from "./routes/supertrend";
import { createMonitorRouter } from "./routes/monitor";
import { startAlertEvaluator } from "./alertEvaluator";

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) {
    throw new Error(`${name} is required (fail-closed; set in dashboard/.env)`);
  }
  return v;
}

const DB_CONFIG = {
  host: process.env.DB_HOST || "localhost",
  port: parseInt(process.env.DB_PORT || "5432", 10),
  database: process.env.DB_NAME || "cryptoengine",
  user: process.env.DB_USER || "cryptoengine",
  password: requireEnv("DB_PASSWORD"),
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
};

const REDIS_URL = requireEnv("REDIS_URL");
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
  startInfraSnapshots(redis);

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

// 5-minute infra snapshot loop — stores CPU/mem/disk/Redis in Redis sorted sets
function startInfraSnapshots(redis: Redis): void {
  const PROM_URL = process.env.PROMETHEUS_URL || "http://prometheus:9090";
  const EXPIRE_SEC = 26 * 3600;  // 26h TTL ensures full 24h coverage
  const RETAIN_MS  = 24 * 3600 * 1000;

  async function promQ(expr: string): Promise<number | null> {
    try {
      const res = await fetch(`${PROM_URL}/api/v1/query?query=${encodeURIComponent(expr)}`);
      if (!res.ok) return null;
      const j: any = await res.json();
      if (j.status === "success" && j.data?.result?.length > 0)
        return parseFloat(j.data.result[0].value[1]);
    } catch { /* ignore */ }
    return null;
  }

  async function snap(): Promise<void> {
    const [cpu, mem, disk, redisMem] = await Promise.allSettled([
      promQ('100 - (avg(irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)'),
      promQ('(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100'),
      promQ('(node_filesystem_size_bytes{mountpoint="/"} - node_filesystem_avail_bytes{mountpoint="/"}) / node_filesystem_size_bytes{mountpoint="/"} * 100'),
      promQ('redis_memory_used_bytes / redis_memory_max_bytes * 100'),
    ]);

    const vals: Record<string, number | null> = {
      cpu:   cpu.status   === "fulfilled" ? cpu.value   : null,
      mem:   mem.status   === "fulfilled" ? mem.value   : null,
      disk:  disk.status  === "fulfilled" ? disk.value  : null,
      redis: redisMem.status === "fulfilled" ? redisMem.value : null,
    };

    const now    = Date.now();
    const nowIso = new Date(now).toISOString();

    for (const [key, val] of Object.entries(vals)) {
      if (val === null) continue;
      const member = JSON.stringify({ ts: nowIso, val: Math.round(val * 10) / 10 });
      const rKey   = `ce:infra:history:${key}`;
      await redis.zadd(rKey, now, member);
      await redis.expire(rKey, EXPIRE_SEC);
      await redis.zremrangebyscore(rKey, 0, now - RETAIN_MS);
    }
  }

  setTimeout(snap, 15_000);          // first snap after 15s
  setInterval(snap, 5 * 60_000);     // then every 5 minutes
}

main().catch((err) => {
  console.error("[dashboard] Fatal error:", err);
  process.exit(1);
});
