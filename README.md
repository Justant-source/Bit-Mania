# CryptoEngine

**비트코인 선물 자동매매 시스템** — 펀딩비 차익거래, BTC 단일 운영, 델타 뉴트럴 전략

> **상태**: Phase 4 (테스트넷 포워드 테스트) 진행 중 | Phase 5 (소액 실전) 준비 중

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
docker compose up -d postgres redis grafana

# 3. 서비스 기동
docker compose up -d market-data execution-engine funding-arb strategy-orchestrator

# 4. 확인
docker compose ps
```

---

## 주요 포트

| 서비스 | 포트 |
|--------|------|
| 내부 대시보드 | http://localhost:3000 |
| 공개 대시보드 | http://localhost:3001 |
| Grafana | http://localhost:3002 |
| PostgreSQL | 5432 |
| Redis | 6379 |

---

## 세부 정보

더 많은 정보는 `docs/README.md` 를 참조하세요.
