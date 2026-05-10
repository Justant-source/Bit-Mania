# 백테스트 헬퍼 스크립트 가이드

이 디렉터리는 문서 전용입니다 (pytest가 수집할 `.py` 파일 없음).  
실제 스크립트는 **`cryptoengine/services/jesse_engine/scripts/`** 에서 단일 관리됩니다.

---

## 1. 개요

9-Strategies V4 백테스트 시스템의 모든 헬퍼 스크립트는 `services/jesse_engine/scripts/`에서 관리됩니다.
Docker 컨테이너(jesse 서비스) 내부에서 실행되며, 컨테이너 안에서의 경로는 `/jesse-project/scripts/`에 마운트됩니다.

```
cryptoengine/
├── services/jesse_engine/scripts/    ← 모든 백테스트 헬퍼 스크립트 위치
│   ├── v4_run_all.py                 ← 오케스트레이터 (76건 일괄)
│   ├── run_external_backtest.py      ← 단일 백테스트 진입점
│   ├── build_v4_dashboard.py         ← 대시보드 빌더
│   └── ...
├── tests/
│   ├── unit/                         ← pytest 단위 테스트 (production code)
│   ├── integration/                  ← pytest 통합 테스트 (production code)
│   └── backtest/
│       └── README.md                 ← 이 파일
└── backtest-results/
    └── data/
        └── 9-strategies/             ← 백테스트 결과 저장소
```

---

## 2. 공통 환경

### Docker 실행 패턴

모든 백테스트 스크립트는 jesse 컨테이너 내부에서 실행합니다:

```bash
# 기본 패턴 (cryptoengine/ 에서 실행)
docker compose -f services/jesse_engine/docker-compose.yml run --rm jesse \
    python /jesse-project/scripts/<script>.py [옵션]
```

### 기본값 (2021-04-01 기준)

| 파라미터 | 기본값 | 설명 |
|----------|--------|------|
| `--start` | `2021-04-01` | 실거래 시작 시점 (BTC ~$60K 고점 후 기점) |
| `--end` | `2025-12-31` | 백테스트 종료 시점 |
| `--balance` | `10000` | 초기 잔고 (USDT) |
| `--leverage` | `1` | 레버리지 |
| `--timeframe` | `1h` | 캔들 타임프레임 |

### 결과 저장 위치

```
cryptoengine/backtest-results/data/9-strategies/
└── {strategy_dir}/{timeframe}/{variant}/
    ├── stats.json                  ← 핵심 통계
    ├── trades.csv                  ← 거래 내역
    ├── monthly_returns.csv         ← 월별 수익
    ├── EXECUTION_SUCCESS.marker    ← 완료 마커 (없으면 재실행)
    └── EXECUTION_FAILED.marker     ← 실패 마커
```

---

## 3. 카테고리별 스크립트

### 오케스트레이터 (전체 실행)

| 스크립트 | 설명 | 사용 시점 |
|----------|------|----------|
| `v4_run_all.py` | 76개 백테스트 일괄 실행 (4 TF × 9전략 × 2변형 + 4 BnH) | 전체 매트릭스 재실행 |
| `run_full_validation.sh` | 전략 검증 배치 실행 | 신규 전략 추가 후 검증 |
| `run_batch_1_tests.sh` | Batch 1 (Mean Reversion 3종) 전용 실행 | Batch 1 재실행 |

**`v4_run_all.py` 옵션**:
```bash
# 전체 실행 (직렬)
python /jesse-project/scripts/v4_run_all.py

# 특정 TF만 실행
python /jesse-project/scripts/v4_run_all.py --tf 1h

# 병렬 실행 (RAM 4GB+/워커 필요)
python /jesse-project/scripts/v4_run_all.py --workers 2

# 실행 목록만 확인 (dry-run)
python /jesse-project/scripts/v4_run_all.py --dry-run
```

### 단일 백테스트 러너

| 스크립트 | 설명 | 사용 시점 |
|----------|------|----------|
| `run_external_backtest.py` | 모든 백테스트의 진입점. 단일 전략/TF/변형 실행 | 단일 전략 검증, 파라미터 실험 |
| `walk_forward.py` | Walk-forward 검증 | 전략 과적합 검증 |
| `wf_optimize_skopt.py` | Skopt 기반 하이퍼파라미터 최적화 | Walk-forward 최적화 |

**`run_external_backtest.py` 예시**:
```bash
# 단일 전략 빠른 검증
docker compose -f services/jesse_engine/docker-compose.yml run --rm jesse \
    python /jesse-project/scripts/run_external_backtest.py \
        --strategy SupertrendStrategy \
        --variant long_only \
        --timeframe 1D \
        --start 2021-04-01 --end 2025-12-31 \
        --output /result/9-strategies/supertrend/1D/long_only

# short 기간 테스트 (2021년만)
docker compose -f services/jesse_engine/docker-compose.yml run --rm jesse \
    python /jesse-project/scripts/run_external_backtest.py \
        --strategy BBPBStrategy \
        --variant bidirectional \
        --timeframe 1h \
        --start 2021-04-01 --end 2021-12-31 \
        --output /tmp/test_bbpb
```

### 리포트 생성기

| 스크립트 | 설명 | 출력 |
|----------|------|------|
| `v4_generate_report.py` | MATRIX_REPORT.md + SUMMARY.md 생성 | `9-strategies/*.md` |
| `wf_generate_report.py` | Walk-forward 리포트 | `walk_forward/*.md` |
| `generate_batch_summary.py` | Batch 요약 CSV | `batch_summary.csv` |
| `generate_cross_batch_summary.py` | Cross-batch 비교 리포트 | `cross_batch.md` |
| `analyze_batch1.py` | Batch 1 결과 분석 + decision.md 생성 | `batch_1/{strat}/decision.md` |
| `analyze_batch3.py` | Batch 3 결과 분석 + decision.md 생성 | `batch_3/{strat}/decision.md` |

### 대시보드

| 스크립트 | 설명 | 출력 |
|----------|------|------|
| `build_v4_dashboard.py` | 인터랙티브 HTML 대시보드 빌드 | `9-strategies/dashboard.html` |

```bash
# 대시보드 재빌드 (로컬 Python — Docker 불필요)
python cryptoengine/services/jesse_engine/scripts/build_v4_dashboard.py
# → cryptoengine/backtest-results/data/9-strategies/dashboard.html
```

### 검증 / 감사

| 스크립트 | 설명 | 사용 시점 |
|----------|------|----------|
| `v3_verify_markers.py` | EXECUTION_SUCCESS.marker 정합성 확인 | 결과 디렉터리 정합성 의심 시 |
| `v3_audit.py` | stats.json + trades.csv 교차 검증 | 숫자 이상 의심 시 |
| `sanity_check.py` | BnH 대비 전략 성과 상식 검증 | 배치 완료 후 빠른 검증 |
| `regime_split_analysis.py` | 상승/하락/횡보 구간별 성과 분석 | 전략 취약 구간 파악 |

### 데이터 백필 (1회용)

| 스크립트 | 설명 |
|----------|------|
| `data/backfill_2019_klines.py` | 2019년 BTCUSDT 1h spot 캔들 다운로드 → parquet 저장. 워밍업 데이터 공급용 (1회 실행 후 재실행 불필요) |

### 디버그

| 스크립트 | 설명 |
|----------|------|
| `test_debug.py` | DB OHLCV 로드 및 지표 계산 빠른 검증 (로컬 실행) |

---

## 4. 시나리오별 사용 흐름

### 시나리오 A: 전체 9-strategies 매트릭스 재실행

```bash
# 1. 완료 마커 삭제 (재실행 강제)
find cryptoengine/backtest-results/data/9-strategies -name 'EXECUTION_SUCCESS.marker' -delete

# 2. TF별 병렬 실행 (4개 터미널 또는 background)
docker compose -f services/jesse_engine/docker-compose.yml run --rm jesse \
    python /jesse-project/scripts/v4_run_all.py --tf 1h &
docker compose -f services/jesse_engine/docker-compose.yml run --rm jesse \
    python /jesse-project/scripts/v4_run_all.py --tf 2h &
docker compose -f services/jesse_engine/docker-compose.yml run --rm jesse \
    python /jesse-project/scripts/v4_run_all.py --tf 4h &
docker compose -f services/jesse_engine/docker-compose.yml run --rm jesse \
    python /jesse-project/scripts/v4_run_all.py --tf 1D &
wait

# 3. 대시보드 재빌드
python cryptoengine/services/jesse_engine/scripts/build_v4_dashboard.py
```

### 시나리오 B: 단일 전략만 빠르게 검증

```bash
docker compose -f services/jesse_engine/docker-compose.yml run --rm jesse \
    python /jesse-project/scripts/run_external_backtest.py \
        --strategy SupertrendStrategy \
        --variant long_only \
        --timeframe 1h \
        --start 2021-04-01 --end 2025-12-31 \
        --output /result/9-strategies/supertrend/1h/long_only
```

### 시나리오 C: Walk-forward 최적화

```bash
# 1. 최적화 실행 (컨테이너 내부)
docker compose -f services/jesse_engine/docker-compose.yml run --rm jesse \
    python /jesse-project/scripts/wf_optimize_skopt.py \
        --strategy TrendTypeStrategy --variant long_only

# 2. 리포트 생성
docker compose -f services/jesse_engine/docker-compose.yml run --rm jesse \
    python /jesse-project/scripts/wf_generate_report.py
```

### 시나리오 D: 결과 감사 / 회귀 확인

```bash
# 마커 정합성 확인
docker compose -f services/jesse_engine/docker-compose.yml run --rm jesse \
    python /jesse-project/scripts/v3_audit.py

# 상식 수준 검증 (BnH 대비)
docker compose -f services/jesse_engine/docker-compose.yml run --rm jesse \
    python /jesse-project/scripts/sanity_check.py
```

### 시나리오 E: 특정 전략 첫 거래 날짜 확인

```bash
# trades.csv 첫 행 확인
head -2 "cryptoengine/backtest-results/data/9-strategies/supertrend/1D/long_only/trades.csv"
```

---

## 5. 공통 규칙

1. **시작 시점**: `--start 2021-04-01` (BTC ~$60K 고점 직후, 보수적 장기 시뮬레이션 기준)
2. **멱등성**: `EXECUTION_SUCCESS.marker` 있으면 v4_run_all.py가 자동 SKIP. 강제 재실행은 마커 삭제 후 재구동.
3. **결과 트리**: `{strat_dir}/{timeframe}/{variant}/` — 이 구조를 변경하면 대시보드/리포트가 깨짐.
4. **워밍업 데이터**: 2019년 spot 캔들이 `backtest-results/data/binance_vision/klines/BTCUSDT/1h/2019/` 에 존재해야 1D TF 워밍업이 정상 동작. 누락 시 `RuntimeError`가 발생하므로 silent slicing 대신 명시적 오류로 확인 가능.
5. **9-strategies 목록**: BBPBStrategy, BBWPStrategy, StochStrategy, MomentumMAStrategy, SupertrendStrategy, TradeIQ220320Strategy, TrendTypeStrategy, SupertrendTrendTypeStrategy, TradeIQ220323Strategy (+ BuyAndHoldStrategy 벤치마크).

---

## 6. 관련 문서

- `cryptoengine/services/jesse_engine/scripts/README.md` — 스크립트 디렉터리 상세 설명
- `docs/strategies/` — 각 전략 사양 문서
- `docs/runbook.md` — Docker 운영 가이드
- `cryptoengine/backtest-results/data/9-strategies/dashboard.html` — 최신 백테스트 대시보드
- `cryptoengine/backtest-results/data/9-strategies/MATRIX_REPORT.md` — 자동 생성 매트릭스 리포트
