/**
 * Grafana Webhook receiver — no auth required (Docker network internal only).
 *
 * POST /grafana-webhook
 *
 * Grafana AlertManager가 보내는 페이로드를 파싱하여 Redis
 * `ce:alerts:grafana` 채널에 PUBLISH한다.
 * telegram-bot이 이 채널을 구독하여 단일 경로로 Telegram에 전달한다.
 *
 * 중복 방지: telegram-bot 측에서 fingerprint 기반 dedup 처리.
 * Grafana contact point는 `type: telegram`이 아닌 `type: webhook`으로 설정.
 */

import { Router, Request, Response } from "express";
import Redis from "ioredis";

/** Grafana alertname → telegram alert_type 매핑. */
const ALERT_TYPE_MAP: Record<string, string> = {
  "Kill Switch 발동": "kill_switch",
  "자산 급감": "anomaly",
  "펀딩비 급변": "grafana_funding_spike",  // 봇 ce:alerts:funding과 다른 이벤트
  "데이터 수집 중단": "grafana_ohlcv_gap",
  "Sharpe 하락": "grafana_sharpe_drop",
  "CPU 과다 사용": "grafana_high_cpu",
  "메모리 부족": "grafana_low_memory",
  "디스크 용량 부족": "grafana_low_disk",
  "Redis 메모리 한계 근접": "grafana_redis_memory",
};

interface GrafanaAlert {
  status: string;
  labels: Record<string, string>;
  annotations: Record<string, string>;
  fingerprint?: string;
  startsAt?: string;
  endsAt?: string;
  generatorURL?: string;
}

interface GrafanaPayload {
  receiver?: string;
  status?: string;
  alerts?: GrafanaAlert[];
  groupLabels?: Record<string, string>;
  commonLabels?: Record<string, string>;
  commonAnnotations?: Record<string, string>;
  externalURL?: string;
}

export function createGrafanaWebhookRouter(redis: Redis): Router {
  const router = Router();

  router.post("/", async (req: Request, res: Response) => {
    let payload: GrafanaPayload;

    try {
      payload = req.body as GrafanaPayload;
    } catch {
      return res.status(400).json({ error: "Invalid JSON payload" });
    }

    if (!payload || !Array.isArray(payload.alerts)) {
      return res.status(400).json({ error: "Missing alerts array" });
    }

    let published = 0;

    for (const alert of payload.alerts) {
      if (!alert.labels) continue;

      const alertName = alert.labels.alertname || "unknown";
      const alertType = ALERT_TYPE_MAP[alertName] || `grafana_${alertName.toLowerCase().replace(/\s+/g, "_")}`;
      const severity = alert.labels.severity || "info";
      const summary = alert.annotations?.summary || alertName;
      const description = alert.annotations?.description || "";
      const fingerprint = alert.fingerprint || `${alertType}_${Date.now()}`;

      const message = JSON.stringify({
        alert_type: alertType,
        alertname: alertName,
        severity,
        summary,
        description,
        status: alert.status,
        fingerprint,
        source: "grafana",
        started_at: alert.startsAt,
      });

      try {
        await redis.publish("ce:alerts:grafana", message);
        published++;

        console.log(
          `[grafana-webhook] PUBLISH ce:alerts:grafana` +
          ` alertname="${alertName}" type="${alertType}" fp="${fingerprint}"`
        );
      } catch (err) {
        console.error("[grafana-webhook] Redis PUBLISH failed:", err);
      }
    }

    return res.json({ ok: true, published });
  });

  return router;
}
