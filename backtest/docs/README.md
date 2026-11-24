---
title: 백테스트 문서 홈
category: backtest-docs
last_updated: 2026-08-29
---

# 백테스트 문서

> 전략 사양·정본 백테스트 수치: [`/docs/strategy.md`](/docs/strategy.md)

## 빠른 실행

```bash
# 백테스트 환경 기동
cd backtest/docker
docker compose --profile backtest up -d backtest-postgres

# Supertrend 백테스트 실행
docker compose --profile backtest run --rm backtester \
  python /app/scripts/runners/run_external_backtest.py --strat SupertrendStrategy --tf 4h --leverage 3
```

전략 R&D 히스토리: `backtest/docs/strategies/` (현재 라이브 전략 017만 유지, 001–016/018–021은 `docs/adr/0009-legacy-strategy-retirement.md`로 통합)
