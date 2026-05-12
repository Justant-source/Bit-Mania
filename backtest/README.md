---
title: Backtest — 전략 R&D 통합 트리
last_updated: 2026-05-12
---

# Backtest

전략 R&D 전용 영역. 운영 코드(`cryptoengine/`)와 완전히 격리됨.

## 구조

| 디렉토리 | 설명 |
|---|---|
| `data/` | OHLCV/funding/fear_greed 원본 (Parquet, .gitignore) |
| `results/` | 전략별 백테스트 산출물 (Parquet) |
| `runtime/` | Jesse 런타임 캐시 (.gitignore) |
| `scripts/` | 실행 스크립트 단일 소스 (카테고리별 서브디렉토리) |
| `strategies/` | Jesse 전략 코드 |
| `configs/` | Jesse yaml 설정 |
| `dashboards/` | 대시보드 HTML 산출물 |
| `docker/` | 백테스트 인프라 (backtester + wf-scheduler + backtest-postgres) |
| `docs/` | 백테스트 문서 단일 소스 |

## 빠른 시작

```bash
cd backtest/docker
docker compose --profile backtest up -d backtest-postgres
docker compose --profile backtest run --rm backtester \
  python /app/scripts/runners/run_external_backtest.py --strat SupertrendStrategy --tf 1h
```

## 문서

→ `backtest/docs/README.md`

## 경로 헬퍼

모든 스크립트는 `from scripts._paths import BT_ROOT, DATA_ROOT, RESULTS_ROOT, RUNTIME_ROOT`를 사용.
