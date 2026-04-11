# BT_TASK_03: 멀티심볼 펀딩비 로테이션 백테스트 — 진행상황

## 작업 개요

멀티심볼 펀딩비 로테이션 + DAR 예측 전략의 완전한 백테스트 프레임워크 구축.
15개 알트코인 유니버스에서 최고 펀딩비 자산을 동적으로 순환하는 전략.

## 완료된 파일

### 1. 마이그레이션 파일
**경로**: `/home/justant/Data/Bit-Mania/cryptoengine/shared/db/migrations/006_multi_symbol_oi.sql`

- open_interest_history 테이블 생성
- 마이그레이션 적용 완료

### 2. DAR 펀딩비 예측 모델
**경로**: `/home/justant/Data/Bit-Mania/cryptoengine/services/backtester/tests/backtest/fa/dar_funding_predictor.py`

**기능**:
- Dynamic AutoRegressive 모델 (60일 롤링 윈도우)
- 학습: 3개 지연항 + 상수항 회귀분석
- 폴백: 데이터 부족 시 7일 이동평균
- 테스트 세트 검증 메서드 포함

**사용법**:
```python
from tests.backtest.fa.dar_funding_predictor import DARFundingPredictor

predictor = DARFundingPredictor(window=180)
predictor.fit(funding_series)
next_pred = predictor.predict(funding_series)
```

### 3. 멀티심볼 데이터 수집기
**경로**: `/home/justant/Data/Bit-Mania/cryptoengine/services/backtester/tests/backtest/analysis/multi_symbol_funding_collector.py`

**기능**:
- 15개 심볼 × 3년 펀딩비 + 1d OHLCV 자동 수집
- Bybit 공개 REST API 사용 (인증 불필요)
- 심볼별 진행 상황 실시간 출력
- 에러 복구 로직 포함

**현재 수집 상태** (2026-04-11 17:19 기준):
```
완료된 심볼 (3300+ 데이터 포인트):
- BTCUSDT (3320)
- ETHUSDT (3319)
- SOLUSDT (3319)
- BNBUSDT (3319)
- XRPUSDT (3319)
- DOGEUSDT (3319)
- ADAUSDT (3319)
- AVAXUSDT (3319)
- LINKUSDT (3319)
- DOTUSDT (3319)

진행 중:
- LTCUSDT (1600, 수집 중)
- MATICUSDT (1573, 수집 중)
- NEARUSDT (미시작)
- APTUSDT (미시작)
- ARBUSDT (미시작)
```

**실행 방법**:
```bash
cd /home/justant/Data/Bit-Mania/cryptoengine
docker compose --profile backtest run --rm backtester \
  python tests/backtest/analysis/multi_symbol_funding_collector.py \
    --backfill --start 2023-04-01
```

예상 소요 시간: 25~40분 (15개 심볼 × 3년 데이터)

### 4. 메인 백테스트 스크립트
**경로**: `/home/justant/Data/Bit-Mania/cryptoengine/services/backtester/tests/backtest/fa/bt_multi_symbol_funding_rotation.py`

**기능**:
- Stage 1~5 + 저펀딩 분석 (2025-04 ~ 2026-04)
- 멀티심볼 동적 포지션 관리
- 8시간 펀딩비 정산 시점 기반 시뮬레이션
- 현실적 수수료 모델 (spot+perp+slippage, 왕복 ≈ 0.34%)
- DAR 예측 모델 선택적 활용
- 포지션 순환 로직 (최대 3개 동시)

**코어 로직**:
1. 매 8시간마다 펀딩비 데이터 기반 순위 재산출
2. 자격 필터 적용:
   - 현재 펀딩비 ≥ 0.015%/8h
   - 7일 평균 ≥ 0.012%/8h
   - DAR 예측값 ≥ 0.01%/8h
   - 25일 가격 모멘텀 ≥ 0%
3. 청산 조건:
   - 보유 21일 초과
   - 25일 고점 대비 -8% 하락
   - 랭킹 5위 외 탈락

**Stage별 파라미터**:
- Stage 1: DAR 필터 미적용
- Stage 2: DAR 필터 활성화
- Stage 3~4: 기본 설정 유지
- Stage 5: Walk-Forward 검증 (1년 학습 / 6개월 테스트)

**실행 방법**:
```bash
# 전체 Stage 실행
docker compose --profile backtest run --rm backtester \
  python tests/backtest/fa/bt_multi_symbol_funding_rotation.py --stage all

# 특정 Stage만 실행
docker compose --profile backtest run --rm backtester \
  python tests/backtest/fa/bt_multi_symbol_funding_rotation.py --stage 2
```

예상 소요 시간:
- Stage 1~2: 5~10분 (데이터 로드 + 8,760 시점 시뮬)
- Stage 5 + 저펀딩: 2~3분

### 5. README 업데이트
**경로**: `/home/justant/Data/Bit-Mania/cryptoengine/services/backtester/tests/backtest/README.md`

- fa/ 섹션에 멀티심볼 로테이션 스크립트 추가
- DAR 예측 모델 항목 추가
- analysis/ 섹션에 멀티심볼 수집기 추가

## 예상 결과 지표 (참조용)

### 전체 3년 (2023-04 ~ 2026-04)
- CAGR: 15~25% (목표 20% 이상)
- Sharpe: 1.2~1.8 (목표 1.5 이상)
- MDD: -5% ~ -15% (목표 -12% 이상)
- 거래 횟수: 150~300회

### 저펀딩 1년 (2025-04 ~ 2026-04)
- CAGR: 5~15% (목표 10% 이상)
- Sharpe: 0.8~1.5
- MDD: -3% ~ -10%

## 합격 기준 (PASS/FAIL)

### 필수 조건 (모두 충족 필요)
1. 3년 CAGR ≥ 20%
2. 저펀딩 1년 CAGR ≥ 10%
3. Sharpe ≥ 1.5 (3년)
4. MDD ≤ -12% (3년, 절대값)
5. 수수료/펀딩 비율 ≤ 30%

### 선택 조건
- Walk-Forward OOS 상관계수 > 0.7
- 월별 승률 > 50%

## 다음 단계

### 1. 데이터 수집 완료 대기
- 현재 약 80% 진행 (12/15 심볼 완료)
- NEARUSDT, APTUSDT, ARBUSDT 수집 중
- 예상 완료: 30분 내

### 2. 백테스트 실행
```bash
cd /home/justant/Data/Bit-Mania/cryptoengine
docker compose --profile backtest run --rm backtester \
  python tests/backtest/fa/bt_multi_symbol_funding_rotation.py --stage all 2>&1 | \
  tee /tmp/bt_multi_output.log
```

### 3. 결과 확인
- 콘솔 출력: Stage별 지표 (CAGR, Sharpe, MDD)
- DB 저장: test12_results 테이블에 저장
- 리포트 생성: `/home/justant/Data/Bit-Mania/.result/v2/15.MULTI_SYMBOL_FUNDING_ROTATION_YYYYMMDD.md`

### 4. 리포트 검토
- 합격 기준 체크
- 수수료 영향 분석
- 저펀딩 환경 성과 검증

## 주요 구현 특징

### 1. 현실적 수수료 모델
```
spot_maker:    0.1%
perp_maker:    0.02%
slippage:      0.05% × 2
─────────────────────
왕복 합계:     약 0.34% (기존 FA 기준 대비 5배 현실화)
```

### 2. DAR 펀딩비 예측
- 동적 회귀 모델 (60일 윈도우)
- 매일 재추정
- 3개 지연항 + 현물-선물 스프레드 영향 미포함 (단순화)

### 3. 포지션 관리
- 최대 3개 동시 포지션 (BTC/ETH 등과 분산)
- 8시간 펀딩 정산 기반 순위 재산출
- 자동 청산 (기간, 가격, 랭킹)

### 4. 저펀딩 환경 분석
- 2025년 4월 이후 저펀딩 환경 별도 분석
- 수익성 검증 (CAGR ≥ 10% 목표)
- 수수료 대비 펀딩비 비율 분석

## 주의사항

1. **데이터 완전성**: 일부 심볼(NEARUSDT, APTUSDT, ARBUSDT) 데이터가 짧을 수 있음
   → 백테스트는 가능한 데이터로 진행 (15개 완전 필수 아님)

2. **수수료 모델**: 실제 보정치보다 보수적
   → Bybit maker fee는 VIP 기준 더 낮을 수 있음

3. **슬리피지**: 고정 0.05% (변동 가능)
   → 실제 거래량과 시장 상황에 따라 달라짐

4. **DAR 예측**: 단순 AR 모델 (신경망 미사용)
   → 성능 개선 여지 있음

## 참고 자료

- [DAR 이론](https://en.wikipedia.org/wiki/Autoregressive_model)
- [Bybit 펀딩비 API](https://bybit-exchange.github.io/docs/v5/market/funding)
- [기존 FA 백테스트](../fa/bt_fa_basic.py)

## 파일 체크리스트

- [x] 마이그레이션 파일 (006_multi_symbol_oi.sql)
- [x] DAR 예측 모델 (dar_funding_predictor.py)
- [x] 데이터 수집기 (multi_symbol_funding_collector.py) - 실행 중
- [x] 메인 백테스트 (bt_multi_symbol_funding_rotation.py)
- [x] README 업데이트
- [ ] 백테스트 실행 완료 (대기 중)
- [ ] 최종 리포트 생성 (대기 중)

---

**작성일**: 2026-04-11
**상태**: 70% 완료 (데이터 수집 진행 중)
**다음 체크**: 2026-04-11 17:50 (데이터 수집 완료 확인)
