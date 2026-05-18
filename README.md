# CryptoEngine

**비트코인 선물 자동매매 시스템** — Supertrend 4h 추세추종, BTC 단일 운영, Long-only 3x

> **상태**: Phase 5 (메인넷 소액 실전) 운영 중 | Supertrend 4h Long-only 3x, $200 USDT

---

## 문서

모든 프로젝트 문서는 `docs/` 에서 관리됩니다.

**시작하기**: [`docs/README.md`](docs/README.md) (Map of Content)

**작업 가이드**: [`CLAUDE.md`](CLAUDE.md) (루트 가이드 + 진입 규칙)

---

## 빠른 시작

```bash
cd cryptoengine

# 1. 환경 설정
cp .env.example .env
# .env 편집: BYBIT_API_KEY, BYBIT_API_SECRET, DB_PASSWORD

# 2. 인프라 기동
docker compose up -d postgres redis

# 3. 서비스 기동
docker compose up -d market-data execution-engine supertrend strategy-orchestrator

# 4. 확인
docker compose ps
```

---

## 주요 포트

| 서비스 | 포트 |
|--------|------|
| 대시보드 (Supertrend) | http://localhost:3000/supertrend |
| 대시보드 (모니터) | http://localhost:3000/monitor |
| PostgreSQL | 5432 |
| Redis | 6379 |

---

## 세부 정보

더 많은 정보는 `docs/README.md` 를 참조하세요.
