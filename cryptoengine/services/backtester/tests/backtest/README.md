# Backtest Skill Index

> **Claude Code 전용 스킬 레지스트리**  
> 새 백테스트 스크립트를 작성하기 전에 **이 파일을 먼저 읽어** 기존 스킬을 재사용하세요.  
> 스크립트 추가·수정·삭제 시 반드시 이 파일을 업데이트해야 합니다.

---

## 공유 유틸리티 (`core/`)

새 백테스트 작성 시 중복 코드 없이 아래를 임포트하세요.

```python
# Docker 컨테이너 내부 PYTHONPATH=/app 기준
from tests.backtest.core import load_ohlcv, load_funding   # DB 데이터 로드
from tests.backtest.core import sharpe, mdd, cagr, safe_float  # 지표 계산
from tests.backtest.core import make_pool, save_result      # DB 연결·저장
from tests.backtest.core import FAEngine, SimpleBacktester  # 엔진
```

| 모듈 | 주요 함수/클래스 | 설명 |
|------|----------------|------|
| `core/loader.py` | `load_ohlcv()`, `load_funding()` | DB에서 OHLCV·펀딩비 로드 |
| `core/metrics.py` | `sharpe()`, `mdd()`, `cagr()`, `safe_float()`, `monthly_returns()`, `profit_factor()` | 성과 지표 계산 |
| `core/db.py` | `make_pool()`, `save_result()`, `clear_stage()`, `DB_DSN` | DB 연결·저장·초기화 |
| `core/engine.py` | `FAEngine`, `SimpleBacktester` | FA 엔진·범용 백테스터 |

### FAEngine 파라미터 요약

```python
FAEngine(
    ohlcv, funding,
    initial_capital=10_000,
    fa_capital_ratio=0.50,   # FA에 할당할 자본 비율
    leverage=2.0,             # 선물 레버리지
    reinvest_ratio=0.0,       # 수익 중 BTC 현물 재투자 비율
    fee_rate=0.00055,
    min_funding_rate=0.0001,
    consecutive_intervals=3,
    max_hold_bars=168,
)
result = engine.run()
# result keys: total_profit_pct, sharpe_ratio, max_drawdown_pct,
#              portfolio_ret_pct, spot_btc, spot_value, trades, equity_curve
```

---

## 스킬 디렉토리

### `fa/` — 펀딩비 차익거래 (FA) 전략

| 파일 | 테스트 | 주요 파라미터 | 실행 명령 |
|------|--------|-------------|---------|
| `fa/bt_fa_basic.py` | FA 기본 백테스트 (펀딩비>0.01% 숏 진입) | `--start` `--end` `--symbol` | `python tests/backtest/fa/bt_fa_basic.py --start 2020-04-01 --end 2026-03-31 --symbol BTCUSDT` |
| `fa/bt_fa_variants.py` | FA 변형 4종 비교 (baseline, tolerant_exit, strict_entry, short_hold) | `--start` `--end` | `python tests/backtest/fa/bt_fa_variants.py` |
| `fa/bt_fa_param_sweep.py` | FA 파라미터 그리드 서치 80조합 (min_rate×consecutive×max_hold) | `--start` `--end` | `python tests/backtest/fa/bt_fa_param_sweep.py` |
| `fa/bt_fa_reinvest.py` | FA 수익 재투자 시뮬레이션 (비율 0~100%) | `--start` `--end` `--initial-capital` | `python tests/backtest/fa/bt_fa_reinvest.py` |
| `fa/bt_fa_reinvest_optimization.py` | **[Test 12A]** 재투자 비율 7종 최적화 + 연도별 BTC 축적 추적 | `--reinvest-ratios 0.0,0.30,0.50,0.60,0.70,0.80,1.00` | `python tests/backtest/fa/bt_fa_reinvest_optimization.py` |
| `fa/bt_fa_capital_ratio.py` | **[Test 12B]** FA 자본 비율 확대 (50~95%) + 마진 안전배율 | `--fa-ratios 0.50,0.60,0.70,0.80,0.90,0.95` | `python tests/backtest/fa/bt_fa_capital_ratio.py` |
| `fa/bt_fa_leverage_limit.py` | **[Test 12C]** 레버리지 한계 (2~10배) 청산 시뮬레이션 | `--leverages 2,3,4,5,7,10` | `python tests/backtest/fa/bt_fa_leverage_limit.py` |
| `fa/bt_multi_symbol_funding_rotation.py` | **[BT_TASK_03]** 멀티심볼 펀딩비 로테이션 (15심볼, DAR 예측, 동적 포지션) | `--stage all` | `python tests/backtest/fa/bt_multi_symbol_funding_rotation.py --stage all` |
| `fa/dar_funding_predictor.py` | DAR(Dynamic AutoRegressive) 펀딩비 예측 모델 | - | (멀티심볼 로테이션에서 내부 사용) |
| `fa/basis_calculator.py` | 분기물-무기한 베이시스 계산 유틸리티 | `perp_price`, `quarterly_price`, `days_to_expiry` | (캘린더 스프레드에서 내부 사용) |
| `fa/bt_calendar_spread.py` | **[#06]** 분기물-무기한 캘린더 스프레드 백테스트 (Stage 1~5) | `--stage all` | `python tests/backtest/fa/bt_calendar_spread.py --stage all` |
| `fa/funding_zscore_calculator.py` | **[#04]** 펀딩비 z-score 계산 유틸리티 (극단치 감지) | `--validate` | `python tests/backtest/fa/funding_zscore_calculator.py --validate` |
| `fa/bt_funding_extreme_reversal.py` | **[#04]** BTC 펀딩비 극단치 역발상 전략 (Stage 1~5: Baseline, GridSearch, Ablation, WalkForward, LowFunding) | `--stage all` | `python tests/backtest/fa/bt_funding_extreme_reversal.py --stage all` |

**언제 사용:** FA 전략 단독 성과 측정, 파라미터 최적화, 레버리지/자본비율/재투자 탐색, 멀티심볼 로테이션 검증, 극단치 역발상 신호 검증

---

### `regime/` — 레짐 분석

| 파일 | 테스트 | 설명 |
|------|--------|------|
| `regime/bt_regime_transition.py` | [Test I] 레짐 전환 리밸런싱 3방법 비교 (immediate, natural, gradual) | FA 포지션 레짐 전환 시 처리 방법 최적화 |
| `regime/bt_regime_granularity.py` | [Test J] 레짐 세분화 비교 (4종 vs 6종 vs 8종) | 레짐 분류 세밀도와 수익 관계 분석 |
| `regime/bt_regime_stability.py` | [Test K] 레짐 안정화 (확인 지연 + ADX 히스테리시스) | 가짜 전환 필터링 효과 측정 |
| `regime/bt_volatile_threshold.py` | [Test L] Volatile 레짐 ATR 배수 임계값 탐색 | 전체 봉의 5~10%를 volatile로 분류하는 최적 배수 |
| `regime/bt_volatility_squeeze.py` | **[BT_TASK_02]** 변동성 스퀴즈 + HMM 레짐 필터 | BB/KC 스퀴즈 돌파 진입, GaussianHMM 3-state 고변동 필터, Stage 1~2 (81개 파라미터) |
| `regime/hmm_regime_detector.py` | HMM 레짐 감지기 모듈 | 6h 데이터 기반 GaussianHMM 3-state (저변동/중간/고변동), 월 단위 롤링 재학습 |
| `regime/squeeze_indicator.py` | 스퀴즈 지표 계산 모듈 | BB + KC 스퀴즈 감지, ATR, RSI, 거래량 피처 계산 |

**언제 사용:** 레짐 감지 로직 개선, 전환 처리 방법 비교, 변동성 기반 진입 신호 백테스트

---

### `combined/` — 복합 전략

| 파일 | 테스트 | 설명 |
|------|--------|------|
| `combined/bt_fa_plus_tf.py` | [Stage 2] FA + 추세추종 결합 | 레짐별 가중치 파라미터 서치, 7종 포트폴리오 비교 |
| `combined/bt_fa_tf_onchain.py` | [Stage 3] FA + TF + 펀딩비 필터 | 온체인 필터 대신 펀딩비 필터 적용 효과 |
| `combined/bt_optimal_combination.py` | **[Test 12D]** 최적 조합 탐색 11종 | FA비율 × 레버리지 × 재투자 교차 최적화 |
| `combined/bt_fa80_extended.py` | **[Test 12D2]** FA80 확장 18종 | FA80 lev3~5x × reinvest30~90%, FA90 재투자 확장 포함 |
| `combined/bt_btc_eth_pair_trading.py` | **[#05]** BTC/ETH 공적분 페어 트레이딩 (Stage 1~5) | Engle-Granger 공적분 검정, z-score 신호, Walk-Forward 분석 |

**언제 사용:** 멀티 전략 포트폴리오 구성, 자본 배분 최적화

---

### `trend/` — 추세추종

| 파일 | 테스트 | 설명 |
|------|--------|------|
| `trend/bt_trend_following.py` | [Stage 1] TF 독립 5종 비교 | EMA크로스, 트리플EMA, 돈치안, ADX모멘텀, MACD+BB |
| `trend/bt_tf_walk_forward.py` | [Stage 4] FA+TF 워크포워드 검증 | 22윈도우, 파라미터 민감도 ±20% 포함 |

**언제 사용:** 추세추종 전략 독립 검증, OOS 워크포워드 분석

---

### `stress/` — 스트레스 테스트

| 파일 | 테스트 | 커버하는 시나리오 |
|------|--------|----------------|
| `stress/bt_stress_fa.py` | [Test P] FA 극단 4종 | API다운, 플래시크래시, 펀딩비급등, 고슬리피지 |
| `stress/bt_stress_tf.py` | FA+TF 극단 6종 | 플래시크래시, 장기횡보, API다운타임, 고슬리피지, 펀딩역전, 연속휩소 |
| `stress/bt_stress_optimal.py` | **[Test 12E]** 최적 조합 스트레스 5종 | 베이시스폭발, 폭락+편측체결, 펀딩가뭄, 거래소점검, 연속충격 |
| `stress/bt_liquidation_cascade.py` | **[BT_TASK_07]** 청산 캐스케이드 역발상 (5 Stage) | 청산 후 반등 포착 (4h -3% + RSI <30), Stage 1 Baseline / Stage 2 임계값 서치 / Stage 3 진입/청산 파라미터 / Stage 4 데이터소스 비교 / Stage 5 시간대 분석 |

**언제 사용:** 최종 검증 단계, 극단 시나리오 PASS/FAIL 판정, 청산 캐스케이드 역발상 전략 평가

---

### `analysis/` — 순수 분석 및 신규 전략 테스트

| 파일 | 분석 내용 | 출력 |
|------|----------|------|
| `analysis/cointegration_tester.py` | **[#05]** Engle-Granger 공적분 검정 도구 (월별 ADF 안정성, 베타, 반감기) | `--pair BTCUSDT,ETHUSDT --report-stability` | 30% 안정도만 달성 (기준 60% 미달) |
| `analysis/bt_funding_time_analysis.py` | [Test O] 펀딩비 시간대·요일 분석, 정산 시간 비교, 연속 스트리크 | 콘솔 통계 테이블 |
| `analysis/bt_fee_sensitivity.py` | [Test N] 수수료 시나리오 4종 (taker/maker/혼합/VIP3) | variant 수익률 비교 |
| `analysis/last_entry_simulation.py` | Bybit mainnet 최근 1년 펀딩비로 현재 prod 파라미터 진입/청산 시뮬 | 파라미터 3종(PROD/BT_DEFAULT/PHASE5) 비교, 마지막 진입·청산 시점 |
| `analysis/prod_1y_report.py` | **[★ 실운영 검증]** 현재 prod 파라미터로 1년 전체 시뮬 → Markdown 리포트 | $200 시작, 거래일지+월별+비용분석, `.result/FA_1Y_SIM_YYYYMMDD.md` 저장 |
| `analysis/bt_exact_1y_report.py` | **[★ 백테스터 조건 재검증]** BT_EXACT vs PROD_CURRENT 1년 병렬 시뮬 → Markdown 비교 리포트 | BT조건(fee 0.055% 선물만, 7일 만기) vs Prod조건 비교, `.result/FA_BT_EXACT_vs_PROD_YYYYMMDD.md` 저장 |
| `analysis/multi_symbol_funding_collector.py` | **[BT_TASK_03]** 멀티심볼 펀딩비 + OHLCV 수집기 (15심볼, 3년 백필) | 콘솔 진행율, 수집된 데이터 카운트 |
| `analysis/etf_flow_collector.py` | **[BT_TASK_01]** BTC ETF 순유입 플로우 데이터 수집 (Farside/SoSoValue API + 합성 폴백) | 817행 ETF Flow, 누적 합계 계산 |
| `analysis/macro_event_calendar.py` | **[BT_TASK_01]** FOMC/CPI 매크로 이벤트 캘린더 저장 (2024~2026) | 49개 이벤트 (FOMC 20, CPI 29) |
| `analysis/bt_etf_flow_momentum.py` | **[BT_TASK_01]** ETF Flow Momentum 전략 백테스트 (Stage 1/2/3) | Stage1: CAGR +10.30% Sharpe 1.61 / Stage2: 36조합 / Stage3: 7개 WF 윈도우 / `.result/13_ETF_FLOW_MOMENTUM_YYYYMMDD.md` 저장 |
| `analysis/quarterly_futures_collector.py` | **[#06]** Bybit 분기물 선물 OHLCV 수집기 (API v5, 일봉, 만기 60일전~만기) | `--backfill --start 2023-04-01` \| `--verify-convergence` | 실제 분기물 데이터 없으면 합성 데이터 자동 생성 |
| `analysis/liquidation_collector.py` | **[BT_TASK_07]** 청산 데이터 수집기 (Coinglass/Binance/OHLCV proxy) | `--backfill --start 2023-04-01 --sources all` | 공개 API 신뢰성 부족 → 1h OHLCV 간접 추정으로 보완 |
| `analysis/cascade_detector.py` | **[BT_TASK_07]** 청산 캐스케이드 탐지기 (4h 가격 -3% + 거래량 × 2.0) | `--validate --threshold 500000000` | 알려진 이벤트(2024-08-05 등) 검증, `long_squeeze`/`short_squeeze` 분류 |

**언제 사용:** 전략 구성 전 데이터 탐색, 수수료 최적화 검토, 멀티심볼 데이터 백필, ETF 플로우 기반 모멘텀 전략 검증, 청산 캐스케이드 기반 역발상 전략 검증

---

### `optimization/` — 파라미터 최적화

| 파일 | 최적화 대상 | 설명 |
|------|------------|------|
| `optimization/bt_dca_variants.py` | [Test C] DCA 추세 필터 4종 | baseline, EMA50, 이중필터, graduated |
| `optimization/bt_dca_v2_redesign.py` | [Test I] DCA v2 재설계 6종 | RSI+EMA, RSI+MACD 등 WF 검증 내장 |
| `optimization/bt_weight_sensitivity.py` | [Test E] 레짐별 가중치 민감도 | Stage 3 최적 가중치 ±20% 안정성 |

**언제 사용:** 보조 전략(그리드·DCA) 파라미터 탐색, 가중치 최적화

---

## 새 백테스트 스크립트 작성 가이드

### 1. 카테고리 선택
| 목적 | 디렉토리 |
|------|---------|
| FA 단독 파라미터/성과 | `fa/` |
| 레짐 감지 로직 개선 | `regime/` |
| 복수 전략 조합 | `combined/` |
| 추세추종 전략 | `trend/` |
| 극단 시나리오 검증 | `stress/` |
| 데이터 탐색/분석 | `analysis/` |
| 파라미터 그리드서치 | `optimization/` |

### 2. 네이밍 컨벤션
```
bt_<카테고리>_<목적>.py
예) fa/bt_fa_basis_spread.py
    regime/bt_regime_4state.py
    stress/bt_stress_exchange_halt.py
```

### 3. 필수 구조 (core/ 활용)

```python
"""bt_xxx_yyy.py — [테스트 번호] 한 줄 설명

실행:
    python tests/backtest/카테고리/bt_xxx_yyy.py
"""
import asyncio, argparse, logging, os, sys
import asyncpg, pandas as pd, structlog

# ★ core/ 임포트 (중복 구현 금지)
sys.path.insert(0, "/app")
from tests.backtest.core import (
    load_ohlcv, load_funding,
    sharpe, mdd, cagr, safe_float,
    make_pool, save_result,
    FAEngine,  # 필요시
)

SYMBOL = "BTCUSDT"
START  = "2020-04-01"
END    = "2026-03-31"

async def main(args):
    pool   = await make_pool()
    ohlcv  = await load_ohlcv(pool, SYMBOL, "1h", start_dt, end_dt)
    funding = await load_funding(pool, SYMBOL, start_dt, end_dt)
    # ... 로직 ...
    await save_result(pool, stage="stage_x", variant="variant_name",
                      metrics={...}, params={...})
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main(_parse()))
```

### 4. Docker 실행
```bash
docker compose --profile backtest run --rm backtester \
  python tests/backtest/<카테고리>/bt_<이름>.py
```

### 5. README 업데이트 의무
스크립트 추가/수정/삭제 후 이 README의 해당 카테고리 테이블을 업데이트하세요.

---

## Walk-Forward 월간 자동 파이프라인

### `scripts/monthly_wf_runner.py` — 월간 자동 WF 실행기

매월 1일 02:00 KST(UTC 17:00)에 자동 실행되는 Walk-Forward 분석 파이프라인.

**실행 방식:**
```bash
# wf-scheduler 서비스로 자동 실행 (docker compose up -d wf-scheduler)
# 즉시 실행 (테스트용):
WF_ON_STARTUP=true docker compose up wf-scheduler

# 단독 실행:
docker compose --profile backtest run --rm backtester \
  python scripts/monthly_wf_runner.py
```

**주요 환경변수:**

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `MONTHLY_WF_CRON` | `0 17 1 * *` | 실행 스케줄 (매월 1일 02:00 KST) |
| `WF_ON_STARTUP` | `false` | `true`이면 컨테이너 시작 시 즉시 실행 |
| `WF_LOOKBACK_DAYS` | `180` | 분석할 최근 일수 |
| `WF_TRAIN_DAYS` | `120` | 학습 윈도우 (일) |
| `WF_TEST_DAYS` | `60` | 테스트 윈도우 (일) |
| `WF_FA_RATIO` | `0.80` | FA 자본 비율 (현재 fa80_lev5_r30) |
| `WF_LEVERAGE` | `5.0` | 레버리지 |
| `WF_REINVEST` | `0.30` | 재투자 비율 |

**실행 결과:**
- DB `walk_forward_monthly` 테이블에 OOS 집계 지표 저장
- `windows_json` 컬럼에 윈도우별 상세 결과 (JSONB)
- Redis `ce:alerts:daily_report` 채널에 Telegram 요약 PUBLISH
- 이전 달 결과와 자동 비교 (Sharpe/CAGR/MDD 변화, 파라미터 변경 감지)
- Sharpe < 1.5 이하면 경고 알림

**DB 테이블 구조:** `walk_forward_monthly`
```sql
period_label, data_start, data_end, n_windows,
agg_sharpe, agg_profit_pct, agg_mdd_pct, agg_win_rate, agg_total_trades,
consistency_ratio, mc_sharpe_mean, mc_sharpe_ci_lo, mc_sharpe_ci_hi, mc_win_prob,
sharpe_alert, windows_json (JSONB), params (JSONB)
```

---

## 실행 이력 요약

| 테스트 | 스크립트 | 핵심 결과 | 리포트 |
|--------|---------|----------|--------|
| Test 12A | `fa/bt_fa_reinvest_optimization.py` | 재투자 100% 시 포트수익 +60.4%, 배율 2.17x | `.result/12.` |
| Test 12B | `fa/bt_fa_capital_ratio.py` | FA 80%까지 마진안전배율 5.49x (안전) | `.result/12.` |
| Test 12C | `fa/bt_fa_leverage_limit.py` | 레버리지 10배도 6년간 청산 0회 | `.result/12.` |
| Test 12D | `combined/bt_optimal_combination.py` | FA90%+3x+재투자30% = 연수익+24.57% | `.result/12.` |
| Test 12D2 | `combined/bt_fa80_extended.py` | FA80+5x+30% = 연수익+34.87% (전체 최고), FA80+4x+30% = Sharpe 균형 최적 | `.result/12.` |
| Test 12E | `stress/bt_stress_optimal.py` | 5/5 스트레스 시나리오 전부 PASS | `.result/12.` |
| BT_TASK_01 | `analysis/bt_etf_flow_momentum.py` | Stage1: CAGR +10.30% Sharpe 1.61 MDD -2.74% (70거래) / Stage2: 36조합 최고 CAGR +15.03% / Stage3: WF OOS 불안정 (-0.31 상관) | `.result/13_ETF_FLOW_MOMENTUM_20260411.md` |
| **#04** | `fa/bt_funding_extreme_reversal.py` | **Stage1**: Baseline CAGR -0.90% Sharpe 1.22 (19거래) / **Stage3**: z-score only 최고 Sharpe 3.25 (221거래, -20% 손실) / **Stage5**: 저펀딩 환경 거래 2회만 CAGR -0.43% / **결론**: Phase 5 도입 불가 (0/6 합격기준 FAIL) | `.result/v2/16.FUNDING_EXTREME_REVERSAL_20260411.md` |
| **#05** | `combined/bt_btc_eth_pair_trading.py` + `analysis/cointegration_tester.py` | **공적분 검정**: 30% 안정도만 달성 (기준 60% 미달) / **Stage1**: CAGR 0% Sharpe 0.14 (13거래) / **Stage2**: Top 1 = entry1.5 exit0.3 window30 = CAGR +5.2% Sharpe 0.42 여전히 기준 미달 / **결론**: Phase 5 도입 불가 (0/6 합격기준 FAIL) | `.result/v2/17_BTC_ETH_PAIR_TRADING_20260411.md` |
