---
title: 단일 프로세스 봇 — 13개 컨테이너를 bot 1개 + Postgres로 축소
adr_number: "0011"
status: Accepted
date: 2026-11-22
related_code:
  - cryptoengine/bot/
  - cryptoengine/docker-compose.yml
  - cryptoengine/scripts/watchdog.sh
related_adr:
  - "0008"
  - "0010"
---

# ADR-0011. 단일 프로세스 봇

## Status

Accepted (2026-11-22). ADR-0008(C4 SSOT 문서 구조)은 문서 체계를 걷어내면서 Superseded 처리한다.

## Context

운영 구성은 13개 컨테이너였다: Python 서비스 5개(market-data, strategy-orchestrator, execution-engine, supertrend, telegram-bot), Redis, Prometheus 스택(prometheus, node-exporter, redis-exporter), 크론 컨테이너(pg-backup, log-retention, ohlcv-retention), TypeScript 대시보드. 서비스 간 통신은 Redis pub/sub이었다.

전략은 4h 봉 하나당 한 번 결정하는 단일 전략(BTC 1개)이다. 그런데 2026-11에 프로세스 간 조율에서 비롯된 버그가 연달아 나왔다.

- `on_stop` 시점의 거래소 재연결 누락 — 종료 시 포지션 동기화가 실패 (f7a410a3)
- 킬스위치 재발동 루프 — 같은 과거 손실이 쿨다운 후 반복 발동 (787a5c51)
- 긴급 청산이 자기 자신의 게이트(킬 게이트)에 막힘 — 청산 주문이 차단 상태를 확인

원인은 모두 "상태가 여러 프로세스에 흩어져 있고 pub/sub으로 동기화된다"는 구조였다. 4h 주기 전략에는 메시지 버스가 필요 없다(봉당 주문 1건, 평가 지연 몇 초는 무관).

## Decision

하나의 asyncio 프로세스(`cryptoengine/bot/`)와 Postgres만 운영한다.

- 상태는 Postgres(`bot_state` 등)에만 둔다. Redis·pub/sub 없음.
- 데이터는 웹소켓이 아니라 REST 폴링: 4h 경계 +3초에 확정 봉을 가져오고, 15분마다 housekeeping.
- 킬 게이트는 직접 호출 `EntryGate.entries_blocked()`이며 reduce-only 주문에는 적용하지 않는다.
- 외부 dead-man은 호스트 cron의 `scripts/watchdog.sh` 하나(알림 전용).
- 대시보드는 같은 프로세스의 aiohttp 읽기 전용 페이지(토큰 인증)로 교체.
- 전략·킬스위치(`shared/kill_switch.py` 무수정)·사이징·불변 규칙은 그대로.

## Removed

- 컨테이너: market-data, strategy-orchestrator, execution-engine, supertrend, telegram-bot, redis, prometheus, node-exporter, redis-exporter, log-retention, ohlcv-retention, TypeScript 대시보드 (남는 것: postgres, bot, pg-backup)
- 기능: Redis 캐시/pub-sub, 웹소켓, dead-man 하트비트 킬(SYSTEM 레벨), 잔고 기동 게이트(`EXPECTED_INITIAL_BALANCE_USD`), `PHASE5_MODE`/`STRICT_MONITORING_HOURS`, `service_logs` DB 로그, Prometheus/Grafana 메트릭, `.request/.result` 브라우저, `make emergency`의 stdin 주입 스크립트, Doc-Sync 린트와 C4 문서 계층
- 환경변수: REDIS_*, COINGLASS_API_KEY, DASHBOARD_INTERNAL/PUBLIC_PORT, GRAFANA_*, CLAUDE_CODE_PATH, PHASE5_MODE 등

## Consequences

- 조율 버그 클래스(프로세스 간 상태 불일치, 죽은 pub/sub 구독)가 사라진다. 이미지·재빌드 대상이 1개다. 메모리 384M 한도로 운영.
- **수용한 위험**
  - 단일 프로세스·단일 호스트: 봇이 죽으면 다음 재시작(`restart: unless-stopped`)까지 아무것도 평가·청산하지 않는다. 보호는 거래소 측 스톱로스에 의존한다.
  - 외부 감시는 호스트 cron watchdog뿐이며 알리기만 한다. 호스트가 죽으면 감시도 같이 죽는다.
  - 킬스위치는 15분 주기, 지갑 잔고 기준이다 (미실현 손실 미반영).
  - STRATEGY/SYSTEM 킬 레벨은 사용하지 않는다.
  - 긴 주문 처리 중 스케줄러가 막혀 하트비트가 수 분 멈춘다.
- Postgres 스키마는 추가 전용이라 이전 코드로의 롤백이 가능하다. 옛 서비스 코드는 컷오버 때 제거되며 git 히스토리에 남는다.
