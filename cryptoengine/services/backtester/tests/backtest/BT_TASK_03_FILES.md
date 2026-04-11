# BT_TASK_03: 멀티심볼 펀딩비 로테이션 백테스트 — 파일 목록 및 경로

## 프로젝트 루트
```
/home/justant/Data/Bit-Mania/cryptoengine/
```

---

## 1. 마이그레이션 파일

### 006_multi_symbol_oi.sql
**경로**: `/home/justant/Data/Bit-Mania/cryptoengine/shared/db/migrations/006_multi_symbol_oi.sql`

**목적**: 오픈 인터레스트 히스토리 테이블 정의

**주요 테이블**:
- `open_interest_history` (exchange, symbol, oi_usd, timestamp)

**상태**: ✓ 생성 완료, 마이그레이션 적용 완료

**실행 명령**:
```bash
docker compose exec -T postgres psql -U cryptoengine -d cryptoengine < shared/db/migrations/006_multi_symbol_oi.sql
```

---

## 2. DAR 펀딩비 예측 모델

### dar_funding_predictor.py
**경로**: `/home/justant/Data/Bit-Mania/cryptoengine/services/backtester/tests/backtest/fa/dar_funding_predictor.py`

**목적**: Dynamic AutoRegressive 펀딩비 예측

**핵심 클래스**: `DARFundingPredictor`

**주요 메서드**:
- `__init__(window=180)`: 60일 롤링 윈도우 초기화
- `fit(funding_series)`: 데이터로 모델 학습
- `predict(funding_series)`: 다음 8h 펀딩비 예측
- `validate_on_test_set()`: 백테스트

**파라미터**:
```python
DARFundingPredictor(window=180)  # 180 × 8시간 = 60일
```

**상태**: ✓ 완성, 다른 스크립트에서 임포트 가능

**사용 예**:
```python
from tests.backtest.fa.dar_funding_predictor import DARFundingPredictor

predictor = DARFundingPredictor(window=180)
predictor.fit(funding_series)
next_rate = predictor.predict(funding_series)
```

---

## 3. 멀티심볼 데이터 수집기

### multi_symbol_funding_collector.py
**경로**: `/home/justant/Data/Bit-Mania/cryptoengine/services/backtester/tests/backtest/analysis/multi_symbol_funding_collector.py`

**목적**: 15개 알트코인의 3년치 펀딩비 + OHLCV 수집

**대상 심볼** (15개):
```
BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT, XRPUSDT,
DOGEUSDT, ADAUSDT, AVAXUSDT, LINKUSDT, MATICUSDT,
DOTUSDT, LTCUSDT, NEARUSDT, APTUSDT, ARBUSDT
```

**데이터 소스**: Bybit 공개 REST API v5

**수집 방식**:
- 펀딩비: `/v5/market/funding/history` (8h 정산)
- OHLCV: `/v5/market/kline` (1d 일봉)

**상태**: ✓ 완성, 실행 완료

**수집 결과**:
- 펀딩비: 45,888개 레코드 (14/15 심볼 완전)
- OHLCV 1d: 15,572개 레코드 (15/15 심볼)

**실행 명령**:
```bash
docker compose --profile backtest run --rm backtester \
  python tests/backtest/analysis/multi_symbol_funding_collector.py \
    --backfill --start 2023-04-01
```

**소요 시간**: 25~40분

---

## 4. 메인 백테스트 스크립트

### bt_multi_symbol_funding_rotation.py
**경로**: `/home/justant/Data/Bit-Mania/cryptoengine/services/backtester/tests/backtest/fa/bt_multi_symbol_funding_rotation.py`

**목적**: 멀티심볼 펀딩비 로테이션 전략 백테스트

**핵심 클래스**: `MultiSymbolFundingBacktester`

**Stage 구조**:
- **Stage 1**: 기본 백테스트 (DAR 필터 미적용)
- **Stage 2**: DAR 예측 모델 활성화
- **Stage 3~4**: 기본 설정 유지
- **Stage 5**: Walk-Forward 검증
- **저펀딩 분석**: 2025-04 ~ 2026-04 분리 분석

**주요 파라미터**:
```python
INITIAL_CAPITAL = 5_000.0
MAX_CONCURRENT_SYMBOLS = 3
POSITION_RATIO_PER_SYMBOL = 0.25  # 각 포지션 25%
LEVERAGE = 5.0

# 펀딩비 자격
MIN_CURRENT_FUNDING = 0.00015      # 0.015%/8h
MIN_7D_AVG_FUNDING = 0.00012       # 0.012%/8h
MIN_DAR_PREDICTION = 0.0001        # 0.01%/8h

# 수수료 (현실화)
ROUND_TRIP_FEE ≈ 0.34%  # spot 0.1% + perp 0.02% + slippage 0.05% × 2
```

**백테스트 기간**: 2023-04-01 ~ 2026-04-10 (3년)

**상태**: ✓ 완성, Stage 1~2 실행 완료

**실행 명령**:
```bash
# 전체 Stage
docker compose --profile backtest run --rm backtester \
  python tests/backtest/fa/bt_multi_symbol_funding_rotation.py --stage all

# 특정 Stage
docker compose --profile backtest run --rm backtester \
  python tests/backtest/fa/bt_multi_symbol_funding_rotation.py --stage 2
```

**소요 시간**: 5~10분 (Stage 1~2), 추가 2~3분 (저펀딩 분석)

**데이터 로드**:
- 데이터베이스에서 15개 심볼의 펀딩비 + OHLCV 자동 로드
- `load_funding()`, `load_ohlcv()` core 함수 사용

---

## 5. README 업데이트

### README.md (updated)
**경로**: `/home/justant/Data/Bit-Mania/cryptoengine/services/backtester/tests/backtest/README.md`

**추가 내용**:

#### fa/ 섹션
```
| `bt_multi_symbol_funding_rotation.py` | 멀티심볼 펀딩비 로테이션 | `--stage all` |
| `dar_funding_predictor.py` | DAR 펀딩비 예측 모델 | (내부 사용) |
```

#### analysis/ 섹션
```
| `multi_symbol_funding_collector.py` | 멀티심볼 펀딩비 + OHLCV 수집 | 수집 통계 |
```

**상태**: ✓ 업데이트 완료

---

## 6. 진행 상황 문서

### BT_TASK_03_PROGRESS.md
**경로**: `/home/justant/Data/Bit-Mania/cryptoengine/services/backtester/tests/backtest/BT_TASK_03_PROGRESS.md`

**목적**: 작업 진행 상황 추적, 다음 단계 안내

**주요 내용**:
- 완료된 파일 목록
- 수집 데이터 통계
- 예상 결과 지표
- 합격 기준
- 다음 단계 (권장사항)

**상태**: ✓ 완성

---

## 7. 최종 리포트

### 15.MULTI_SYMBOL_FUNDING_ROTATION_SUMMARY.md
**경로**: `/home/justant/Data/Bit-Mania/.result/v2/15.MULTI_SYMBOL_FUNDING_ROTATION_SUMMARY.md`

**목적**: 백테스트 최종 결과 분석 및 판정

**포함 내용**:
- 3년 백테스트 결과 (Stage 1, 2)
- 저펀딩 환경 분석 (1년)
- 핵심 지표 및 판정
- 성능 미달의 원인 분석
- 개선 방안 제시 (3가지)

**주요 결과**:
```
STAGE 1 & 2:
- 최종 자산가치: $4,965.53
- 총 수익률: -0.69%
- CAGR: -0.23%
- Sharpe: -2.83
- MDD: -0.76%

저펀딩 (2025-04 ~ 2026-04):
- 최종 자산가치: $5,000
- 총 수익률: 0.0%
```

**판정**: ❌ FAIL (기대 성능 미달, 포지션 관리 로직 개선 필요)

**상태**: ✓ 완성

---

## 데이터 통계

### 수집된 펀딩비 데이터
```
심볼별 레코드 수:
- BTCUSDT: 3,320
- ETHUSDT: 3,319
- SOLUSDT: 3,319
- BNBUSDT: 3,319
- XRPUSDT: 3,319
- DOGEUSDT: 3,319
- ADAUSDT: 3,319
- AVAXUSDT: 3,319
- LINKUSDT: 3,319
- DOTUSDT: 3,319
- LTCUSDT: 3,319
- NEARUSDT: 3,319
- APTUSDT: 3,319
- ARBUSDT: 3,315
- MATICUSDT: 1,573

총합: 45,888개 레코드
기간: 2023-04-01 ~ 2026-04-11 (3년)
완전성: 93% (14/15 심볼)
```

### 수집된 OHLCV 데이터
```
심볼별 1d 일봉 수:
- 대부분: 1,102개 (약 3년)
- MATICUSDT: 522개 (약 1.5년)

총합: 15,572개 일봉
기간: 2023-04-01 ~ 2026-04-10
완전성: 100% (15/15 심볼)
```

---

## Docker 실행 환경

### 사용 이미지
- Base: `python:3.12-slim`
- Packages: `asyncpg`, `aiohttp`, `pandas`, `numpy`, `structlog`

### 환경변수
```
DB_USER=cryptoengine
DB_PASSWORD=CryptoEngine2026!
DB_HOST=postgres
DB_PORT=5432
DB_NAME=cryptoengine
PYTHONPATH=/app
```

### 빌드 및 실행
```bash
# 이미지 빌드
docker compose --profile backtest build backtester

# 백테스트 실행
docker compose --profile backtest run --rm backtester \
  python tests/backtest/fa/bt_multi_symbol_funding_rotation.py --stage all
```

---

## 코드 특징

### 1. 비동기 처리
- `asyncio` 기반 백그라운드 작업
- `asyncpg` 비동기 DB 드라이버
- `aiohttp` 비동기 HTTP 클라이언트

### 2. 현실적 수수료 모델
```
Maker 기준 (Bybit VIP0):
- Spot Maker: 0.1%
- Perp Maker: 0.02%
- Slippage (Alt): 0.05%

왕복 합계:
(0.1% + 0.02%) × 2 + 0.05% × 2 = 0.34%
```

### 3. DAR 펀딩비 예측
```
funding_t+1 = β0 + β1·funding_t + β2·funding_t-1 + β3·funding_t-2 + ε

학습 윈도우: 180 × 8시간 = 60일
폴백: 7일 이동평균 (데이터 부족 시)
```

### 4. 포지션 관리
```
진입 조건:
- 펀딩비 현재 >= 0.015%/8h
- 펀딩비 7일평균 >= 0.012%/8h
- DAR 예측값 >= 0.01%/8h
- 가격 모멘텀 (25d) >= 0%

청산 조건:
- 보유 기간 > 21일
- 가격 하락 < -8% (25d 고점 대비)
- 랭킹 5위 외 탈락

동시 포지션: 최대 3개
각 포지션 크기: 자본의 25%
```

---

## 합격 기준 (PASS/FAIL)

### 필수 조건
- [ ] CAGR ≥ 20% (3년): -0.23% ❌
- [ ] CAGR ≥ 10% (저펀딩 1년): 0.0% ❌
- [ ] Sharpe ≥ 1.5: -2.83 ❌
- [x] MDD ≤ -12%: -0.76% ✅

### 현황
```
결론: FAIL
이유: 포지션 관리 로직 미흡으로 펀딩비 수익 취득 실패
```

---

## 다음 단계 (개선 로드맵)

### Phase 1: 로직 검증 (1주)
- [ ] 포지션 진입/청산 상세 디버깅
- [ ] 개별 포지션 PnL 추적
- [ ] 진입/청산 타이밍 검증

### Phase 2: 단기 개선 (2주)
- [ ] 자격 필터 임계값 조정
- [ ] 보유 기간 확대 (21일 → 45일)
- [ ] Stage 3~5 재실행

### Phase 3: 중기 개선 (4주)
- [ ] DAR 모델 성능 평가
- [ ] Walk-Forward 검증
- [ ] 포지션 크기 동적 조정

### Phase 4: 근본 개선 (8주)
- [ ] 기존 FAEngine 통합
- [ ] 복합 전략 연계 (FA + TF)
- [ ] 실운영 파라미터 검증

---

## 참고 자료

### Bybit API
- [Funding History API](https://bybit-exchange.github.io/docs/v5/market/funding)
- [Kline (OHLCV) API](https://bybit-exchange.github.io/docs/v5/market/kline)

### 기존 백테스트
- [FA 기본 백테스트](../fa/bt_fa_basic.py)
- [FA 재투자 분석](../fa/bt_fa_reinvest.py)
- [FA + TF 결합](../combined/bt_fa_plus_tf.py)

### 문서
- [CLAUDE.md](../../../CLAUDE.md) - 프로젝트 전체 가이드
- [README.md](README.md) - 백테스트 스킬 인덱스

---

## 버전 정보

**생성일**: 2026-04-11
**마지막 업데이트**: 2026-04-11
**Python 버전**: 3.12
**의존성 버전**: asyncpg, aiohttp, pandas, numpy (최신)

---

## 문의 및 피드백

모든 스크립트는 `/app` 기준 상대경로로 작성됨.
Docker 컨테이너 내에서만 실행 가능.

로컬 테스트: Docker 없이는 DB 연결 불가 (의도적 설계)
