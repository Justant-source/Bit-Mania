# Archived Strategies — DO NOT REVIVE

이 디렉토리의 전략들은 **영구 폐기** 결정됨 (2026-04-12).

## 폐기 이유

V2 → V3 → V4 총 4차례 백테스트에서 모두 FAIL.
버그 수정 후에도 성과 개선 없음 → ETF 이후 압축 시장 구조에서
**근본적으로 작동 불가능**한 전략으로 판단.

상세 분석: CRYPTOENGINE_REBUILD_TASK_PHASE7_8.md (섹션 0.1)

## 폐기 전략 목록

| 파일 | 전략 번호 | 폐기 근거 |
|------|-----------|---------|
| bt_multi_symbol_funding_rotation.py | #3 | 2025년 후반 알트 펀딩비 천장 압축 → 전 조합 음수 수익 |
| bt_funding_extreme_reversal.py | #4 | abs() 버그 수정 후에도 알파 없음 |
| bt_btc_eth_pair_trading.py | #5 | BTC/ETH 공적분 관계 붕괴 (ETF 이후) |
| bt_calendar_spread.py | #6 | 실데이터 연결 불가 + 합성 데이터 오염 |
| bt_liquidation_cascade.py | #7 | 청산 데이터 API 비용 + 패턴 희소성 |
| bt_hmm_llm_meta_strategy.py | #10 | HMM NaN 오염 Sharpe 0.000, 알파 부재 |

## 경고

이 디렉토리의 파일을 절대 `import`하지 말 것.
`__init__.py`는 의도적으로 ImportError를 발생시킴.
부활을 원한다면 새로운 데이터, 새로운 전략 설계부터 시작할 것.
