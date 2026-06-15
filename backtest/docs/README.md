---
title: 백테스트 문서 홈
category: backtest-docs
last_updated: 2026-06-15
---

# 백테스트 문서

> 백테스트 R&D 전체 문서는 **[`/docs/_index.md`](/docs/_index.md)** 에서 관리됩니다.
>
> 전략 사양·백테스트 방법론: [`docs/70-policy/strategy.md`](/docs/70-policy/strategy.md)

## 빠른 실행

```bash
# 백테스트 환경 기동
cd backtest/docker
docker compose --profile backtest up -d backtest-postgres

# Supertrend 백테스트 실행
docker compose --profile backtest run --rm backtester \
  python main.py --strategy supertrend --tf 4h

# 대시보드 재빌드
docker compose --profile backtest run --rm backtester \
  python /app/dashboards/script/build_supertrend_dashboard.py
```

전략 R&D 히스토리: `backtest/docs/strategies/` (001-021)
