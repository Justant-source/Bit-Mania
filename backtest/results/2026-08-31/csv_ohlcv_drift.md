---
title: CSV 픽스처 ↔ 라이브 ohlcv_history 가격 드리프트 (발견, 미해결 — 후속조사 필요)
date: 2026-08-31
status: OPEN. tripwire_check.py extend-csv를 현재 막고 있음. 원인 미규명.
found_by: backtest/scripts/analysis/tripwire_check.py (extend-csv 정합 검증), 독립 재확인 완료
---

# 요약

`cryptoengine/tests/fixtures/btc_4h_extended.csv`(마지막 봉 2026-08-28 20:00 UTC)의 최근 20봉 종가가 라이브 `ohlcv_history`(`exchange='bybit', symbol='BTCUSDT', timeframe='4h'`)의 동일 timestamp 종가와 **전부 일치하지 않는다.** CSV가 DB보다 **일관되게 $15~53 높다** (2026-08-25 16:00 ~ 2026-08-28 20:00 구간, 20/20봉 불일치, `diff` 항상 음수 = DB < CSV). 상대 오차는 ~0.02~0.07%로 작지만, timestamp가 정확히 일치하는데도 가격이 다르다는 것은 두 소스 중 하나(또는 둘 다)가 사후에 값이 바뀌었거나, 애초에 다른 파이프라인으로 채워졌다는 뜻이다.

재현:
```bash
python3 backtest/scripts/analysis/tripwire_check.py extend-csv --dry-run
```

# 영향

- **`tripwire_check.py extend-csv`가 (의도대로) 중단됨** — 정합 검증이 실패를 정확히 잡아 잘못된 데이터를 이어붙이지 않았다. `backtest/results/tripwire/log.md`는 이 드리프트가 해소되기 전까지 생성되지 않는다(T1/T2 트립와이어의 실제 로그 판정은 보류 상태).
- `backtest/results/2026-08-31/live_slippage_report.md` §5.1(2026-08-19 라이브-리플레이 정합감사)에서도 같은 계열의 드리프트를 확인했다(08-19 12:00봉: CSV 68554.00 vs DB 68523.30, −$30.70). **다만 그 절 자체의 결론에는 영향 없음** — 드리프트 크기($15~53)가 해당 절에서 확인한 진입가 괴리($6,876)보다 두 자릿수 배 작아 설명력이 없다.
- v11/v12/`holdout_reverification.md`의 모든 결론은 **이 드리프트가 생기기 전에 캡처된 CSV**로 계산됐으므로 영향 없다(그 CSV 자체가 당시의 유일한 정본이었음). 문제는 **앞으로** 이 CSV를 신선한 라이브 데이터로 연장하려 할 때만 발생한다.

# 원인 후보 (미확인)

1. **거래소 측 캔들 사후 정정** — Bybit가 실시간 캡처 이후 klines 히스토리를 소폭 재계산해서 서빙할 가능성 (거래 늦게 보고되는 경우 흔함).
2. **CSV와 `ohlcv_history`가 애초에 다른 파이프라인/시점으로 채워짐** — CSV가 언제, 어떤 스크립트로 생성됐는지 이 세션에서 확인하지 못했다(생성 스크립트를 찾지 못함 — `grep`으로 저장소 전체에서 생성 로직 미발견, 수동/1회성 익스포트였을 가능성).
3. `ohlcv_history` 자체의 갱신/정정 로직(있다면) — `market-data` 서비스가 확정봉을 사후에 덮어쓰는지 미확인.

# 다음 조치 (별도 작업, 이 세션 범위 밖)

1. CSV가 어떻게 생성됐는지(스크립트/수동 export/시점) 확인.
2. 드리프트가 최근 구간(08-25~)에만 있는지, 아니면 CSV 전체에 걸쳐 있는지 표본 확인(예: 2025-01, 2020-01 등 임의 구간 대조).
3. 원인 규명 후 `tripwire_check.py extend-csv`를 재실행해 CSV를 라이브 정본으로 재동기화(필요 시 전체 재생성).
4. 그 전까지 트립와이어의 T1/T2 실제 판정(`log.md` 첫 기록)은 **보류**한다 — 이것이 사전등록 절차를 어기는 것은 아니다(사전등록 파일은 이미 커밋됨, 첫 실행 시점만 늦춰지는 것).
