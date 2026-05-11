# Jesse Engine Scripts

백테스트 파이프라인 스크립트 인덱스. 스크립트 추가·수정·삭제 시 이 파일을 반드시 동기화한다.

## 실행 위치

모든 스크립트는 **프로젝트 루트** (`/home/.../Bit-Mania/`)에서 실행한다:

```bash
python3 cryptoengine/services/jesse_engine/scripts/<script>.py
```

---

## 파이프라인 실행 순서

```
1. (Jesse 백테스트 실행)
2. apply_maker_fee.py      — fee 0.02% 재적용
3. audit_v4_data.py        — 데이터 무결성 검증
4. build_v4_dashboard.py   — HTML 대시보드 생성
5. rolling_window_analysis.py — 롤링 윈도우 전략 평가
```

---

## 스크립트 인덱스

### 분석 / 평가

| 파일 | 목적 | 주요 출력 |
|---|---|---|
| `rolling_window_analysis.py` | **롤링 윈도우 테스트** — 전략을 3·6·12·24·36개월 구간으로 슬라이싱하여 시기별 수익 평가. DELETE/WEAK/KEEP 판정. | `rolling_window/windows_detail.csv`, `combo_summary.csv`, `strategy_verdict.md`, `heatmap_*.png` |
| `regime_split_analysis.py` | 시장 레짐(bull/transition/compressed)별 단일 전략 성능 분석 (V5 파이프라인) | Markdown 리포트 |
| `walk_forward.py` | Walk-Forward (IS 365일 / OOS 180일) | JSON + Markdown WF 리포트 |
| `generate_v5_report.py` | V5 최종 백테스트 리포트 생성 | Markdown |
| `generate_batch_summary.py` | 배치 테스트 요약 | CSV |
| `generate_cross_batch_summary.py` | 여러 배치 교차 비교 | CSV |

### 빌드 / 변환

| 파일 | 목적 | 주요 출력 |
|---|---|---|
| `build_v4_dashboard.py` | 9-strategies 백테스트 결과 → 단일 HTML 대시보드 | `dashboard.html` (~5-8MB) |
| `apply_maker_fee.py` | trades.csv에 Bybit maker fee(0.02%) 재적용 | `trades.csv`, `monthly_returns.csv`, `stats.json` 갱신 |
| `jesse_import.py` | Parquet → Jesse PostgreSQL 캔들 임포트 | DB 로드 |

### 검증

| 파일 | 목적 | 주요 출력 |
|---|---|---|
| `audit_v4_data.py` | trades/monthly_returns/stats.json 무결성 검증 (28개 체크) | `audit_data_report.md` |
| `audit_v4_dashboard.py` | dashboard.html payload vs raw data 교차검증 (15개 체크) | `audit_dashboard_report.md` |
| `sanity_check.py` | V5 기준 사전 검증 | PASS/FAIL 출력 |
| `v3_audit.py` | V3 백테스트 레거시 검증 | — |

### 실행

| 파일 | 목적 | 비고 |
|---|---|---|
| `run_backtest.py` | Jesse 연구 API 백테스트 실행 | |
| `run_fa_backtest.py` | FA 순수 시뮬레이션 (자체 엔진) | |
| `v4_run_all.py` | 9-strategies 전체 배치 실행 | `--leveraged-only` 플래그로 x2/x3만 재실행 |
| `run_external_backtest.py` | 외부 API 백테스트 | |
| `run_batch_1_tests.sh` | 배치1 테스트 Shell | |
| `run_full_validation.sh` | 전체 V5 검증 파이프라인 | ~30분~2시간 |

### 테스트 / 디버그

| 파일 | 목적 |
|---|---|
| `test_funding_pnl.py` | 펀딩비 P&L 단위 테스트 |
| `test_debug.py` | 디버그용 임시 스크립트 |
| `sanity_check.py` | Jesse 정확성 검증 |

### 리포트 (레거시)

| 파일 | 목적 |
|---|---|
| `v3_generate_report.py` | V3 리포트 생성 |
| `v3_verify_markers.py` | V3 마커 검증 |
| `v4_generate_report.py` | V4 리포트 생성 |
| `wf_generate_report.py` | WF 리포트 생성 |
| `wf_optimize_skopt.py` | WF Scikit-optimize 기반 파라미터 탐색 |
| `analyze_batch1.py` | 배치1 분석 |
| `analyze_batch3.py` | 배치3 분석 |

---

## rolling_window_analysis.py 재활용 가이드

9-strategies 이외 다른 백테스트 결과에도 동일한 롤링 윈도우 평가를 적용할 수 있다.

### 다른 데이터셋에 적용하는 법

스크립트 상단의 경로/파라미터 상수만 수정하면 된다:

```python
# rolling_window_analysis.py 상단
RESULT_DIR = CE_ROOT / 'backtest-results' / 'data' / '9-strategies'  # ← 경로 변경
OUT_DIR    = RESULT_DIR / 'rolling_window'

STRATEGIES  = ['bbpb', 'bbwp', ...]   # ← 전략명 변경
VARIANTS_1X = ['bidirectional', 'long_only']  # ← variant 변경
TIMEFRAMES  = ['1h', '2h', '4h', '1D']  # ← TF 변경
BACKTEST_START = '2021-01'   # ← 기간 변경
BACKTEST_END   = '2026-04'
```

또는 CLI 옵션으로 오버라이드:

```bash
python3 scripts/rolling_window_analysis.py \
    --start 2023-01 --end 2026-04 \
    --window-sizes 3 6 12 \
    --variants long_only
```

### 입력 데이터 요구사항

`{RESULT_DIR}/{strategy}/{timeframe}/{variant}/trades.csv` 가 존재해야 한다.

```
trades.csv 필수 컬럼:
  opened_at   (ms epoch float)
  closed_at   (ms epoch float)
  pnl         (float, $)
```

`stats.json` 이 있으면 전체기간 net_profit 검증이 추가로 수행된다 (없어도 실행됨).

### 전략 삭제 판정 변경

`verdict` 로직은 `summarize_strategy()` 함수에서 변경한다 (`rolling_window_analysis.py:245`):

```python
# 현재: 8조합 모두 never_positive → DELETE
verdict = 'DELETE' if never_pos == total else ('WEAK' if never_pos > 0 else 'KEEP')

# 예: fail_strict_ratio 평균 > 70% → DELETE로 강화
verdict = 'DELETE' if grp['fail_strict_ratio'].mean() > 0.70 else ...
```

---

## 데이터 경로 요약

```
cryptoengine/backtest-results/data/9-strategies/
  {strategy}/{tf}/{variant}/
    trades.csv              — 거래 내역 (opener_at, closed_at, pnl, fee, ...)
    monthly_returns.csv     — 월별 pnl_usdt 합산
    stats.json              — 전략 성과 지표 (raw_metrics 포함)
    equity_curve.png        — 자본 곡선
    EXECUTION_SUCCESS.marker — 백테스트 완료 마커

  rolling_window/           — 롤링 윈도우 분석 결과
    windows_detail.csv
    combo_summary.csv
    strategy_verdict.csv
    strategy_verdict.md
    heatmap_*.png

  dashboard.html            — HTML 대시보드 (build_v4_dashboard.py 출력)
  audit_data_report.md      — 데이터 감사 리포트
  audit_dashboard_report.md — 대시보드 감사 리포트
```

---

## 상세 방법론

롤링 윈도우 테스트 방법론 전문: [`docs/test/rolling_window_test.md`](../../../../../docs/test/rolling_window_test.md)
