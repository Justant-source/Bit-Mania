# AUDIT REPORT — CryptoEngine 프로젝트 전수 조사

> 작성일: 2026-04-12  
> 조사 대상: `/home/justant/Data/Bit-Mania/`  
> 목적: Phase 1 (Audit) — 정리 작업 전 현황 파악

---

## 1. 프로젝트 통계

| 항목 | 수치 |
|------|------|
| 전체 파일 수 | 517개 |
| Python (.py) | 157개 |
| Parquet (.parquet) | 226개 (백테스트 데이터) |
| JSON (.json) | 33개 |
| YAML/YML | 18개 |
| Markdown (.md) | 17개 (소스 내) + 별도 문서 다수 |
| Python 총 라인 수 | 36,178줄 (빈 줄 포함) |

---

## 2. 디렉토리 역할 요약

```
/home/justant/Data/Bit-Mania/
├── cryptoengine/                   # 메인 프로젝트 루트
│   ├── config/                     # 전략·거래소·그라파나·프로메테우스 설정
│   ├── docs/                       # 운영 문서 (긴급 SOP 등)
│   ├── scripts/                    # 운영 스크립트 (DB 초기화, 헬스체크 등)
│   ├── shared/                     # 모든 서비스 공유 라이브러리
│   ├── services/
│   │   ├── market-data/            # WebSocket OHLCV·펀딩비 수집, 레짐 감지
│   │   ├── orchestrator/           # 전략 가중치 관리, 포트폴리오 리스크
│   │   ├── execution/              # 주문 실행, 스탑로스 관리, 안전 검증
│   │   ├── strategies/
│   │   │   ├── funding-arb/        # 핵심 전략: 델타뉴트럴 + 펀딩비 수취
│   │   │   └── adaptive-dca/       # 보조 전략: Fear&Greed 기반 DCA
│   │   ├── llm-advisor/            # Claude 기반 시장 분석 (46개 파일)
│   │   ├── telegram-bot/           # 알림·비상 명령·ACK
│   │   ├── dashboard/              # Express.js 기반 대시보드
│   │   ├── jesse_engine/           # ★ 현재 활성 백테스트 엔진 (Jesse 프레임워크)
│   │   ├── backtester/             # ⚠️ 구 백테스트 프레임워크 잔재 (사실상 빈 디렉토리)
│   │   ├── log-retention/          # 로그 보존 정책 자동화 서비스
│   │   └── wf-scheduler/           # 월간 Walk-Forward 자동 실행 서비스
│   ├── tests/                      # 통합·단위 테스트 + 구 백테스트 스크립트
│   ├── backtest-results/           # 백테스트 입력 데이터 (OHLCV, 펀딩비 등)
│   └── docker-compose.yml 등       # Docker 오케스트레이션
├── .request/                       # 작업 지시서 (Claude에게 내린 요청 문서)
└── .result/                        # 백테스트 실행 결과 분석 문서
```

---

## 3. 파일 분류표

### 3-A. shared/ 모듈

| 파일 경로 | 역할 | Import 횟수 | 분류 | 비고 |
|-----------|------|------------|------|------|
| `shared/log_events.py` | 이벤트 코드 정의 (95개) | **45** | ✅ 유지 | 가장 많이 사용되는 공유 모듈 |
| `shared/logging_config.py` | structlog 표준 설정 | 5 | ✅ 유지 | 서비스 진입점에서 사용 |
| `shared/redis_client.py` | Redis Pub/Sub 헬퍼 | 6 | ✅ 유지 | 전략 모듈에서 사용 |
| `shared/kill_switch.py` | Kill Switch 공통 로직 | 4 | ✅ 유지 | 오케스트레이터·실행·텔레그램 |
| `shared/log_writer.py` | 비동기 DB 로그 라이터 | 4 | ✅ 유지 | 서비스 진입점에서 사용 |
| `shared/timezone_utils.py` | KST 타임존 유틸리티 | 9 | ✅ 유지 | 스크립트·백테스트에서 사용 |
| `shared/risk.py` | 레버리지 검증, 포지션 크기 | **1** (테스트만) | 🔍 검토 필요 | 프로덕션에서 미사용, 테스트만 참조 |
| `shared/config_loader.py` | YAML 설정 로더 | **0** | ❌ 삭제 후보 | 어디서도 import 안 됨 |
| `shared/models/*.py` | 도메인 모델 | 복수 | ✅ 유지 | 공유 데이터 모델 |
| `shared/exchange/*.py` | Bybit/Binance CCXT 래퍼 | 복수 | ✅ 유지 | 거래소 추상화 |
| `shared/db/*.py` | asyncpg 풀, Repository 패턴 | 복수 | ✅ 유지 | DB 접근 계층 |

### 3-B. services/backtester/ (구 백테스트 프레임워크)

| 파일 경로 | 역할 | 분류 | 비고 |
|-----------|------|------|------|
| `services/backtester/tests/backtest/core/__pycache__/db.cpython-312.pyc` | Python 캐시 | ❌ 삭제 | 소스 파일 없음, 캐시만 남음 |
| `services/backtester/tests/backtest/core/__pycache__/engine.cpython-312.pyc` | Python 캐시 | ❌ 삭제 | 소스 파일 없음, 캐시만 남음 |
| `services/backtester/tests/backtest/core/__pycache__/metrics.cpython-312.pyc` | Python 캐시 | ❌ 삭제 | 소스 파일 없음, 캐시만 남음 |
| `services/backtester/tests/backtest/core/__pycache__/loader.cpython-312.pyc` | Python 캐시 | ❌ 삭제 | 소스 파일 없음, 캐시만 남음 |
| **`services/backtester/` 디렉토리 자체** | 구 백테스트 엔진 잔재 | ❌ 삭제 후보 | 소스 없이 __pycache__만 남음 |

> **배경**: Phase 13에서 자체 백테스트 엔진을 제거하고 jesse_engine으로 마이그레이션했으나,
> `services/backtester/` 디렉토리 자체가 남아 있는 상태. 실질적으로 빈 디렉토리.

### 3-C. tests/ (루트 레벨 테스트)

| 파일 경로 | 역할 | 분류 | 비고 |
|-----------|------|------|------|
| `tests/unit/test_safety_guard.py` | SafetyGuard 유닛 테스트 27개 | ✅ 유지 | CLAUDE.md에 명시된 완료 테스트 |
| `tests/unit/test_risk.py` | risk.py 테스트 | 🔍 검토 필요 | shared/risk.py 프로덕션 미사용과 연동 검토 |
| `tests/unit/test_order_manager.py` | 주문 관리 테스트 | ✅ 유지 | 프로덕션 코드 테스트 |
| `tests/unit/test_regime_detector.py` | 레짐 감지 테스트 | ✅ 유지 | 프로덕션 코드 테스트 |
| `tests/integration/test_funding_arb_e2e.py` | 펀딩 중재 E2E | ✅ 유지 | 통합 테스트 |
| `tests/integration/test_execution_flow.py` | 실행 흐름 통합 | ✅ 유지 | 통합 테스트 |
| `tests/integration/test_bybit_connector.py` | Bybit 연결 통합 | ✅ 유지 | 통합 테스트 |
| `tests/backtest/bt_funding_arb.py` | 구 FreqtradeBridge 기반 FA 백테스트 | 🔍 검토 필요 | FreqtradeBridge 사용 — 현재 활성 여부 확인 필요 |

### 3-D. services/jesse_engine/ (현재 활성 백테스트)

| 파일 경로 | 역할 | 분류 | 비고 |
|-----------|------|------|------|
| `services/jesse_engine/strategies/funding_arbitrage.py` | FA 기본 전략 | ✅ 유지 | 현재 사용 중 |
| `services/jesse_engine/strategies/funding_arbitrage_v2.py` | FA + 거시 필터 | 🔍 검토 필요 | v3가 있으면 v2 역할 확인 |
| `services/jesse_engine/strategies/funding_arbitrage_v3.py` | FA + FG 사이징 | 🔍 검토 필요 | 현재 채택 전략과의 관계 확인 |
| `services/jesse_engine/strategies/sanity_check.py` | BtcBuyAndHold 검증 | ✅ 유지 | 엔진 검증용 |
| `services/jesse_engine/scripts/run_backtest.py` | Jesse Research API 러너 | ✅ 유지 | 주요 백테스트 진입점 |
| `services/jesse_engine/scripts/run_fa_backtest.py` | FA 순수 시뮬레이션 | ✅ 유지 | 최근 수정 (2026-04-12) |
| `services/jesse_engine/scripts/jesse_import.py` | Jesse DB 데이터 임포트 | ✅ 유지 | Phase 14에서 완성됨 |
| `services/jesse_engine/scripts/walk_forward.py` | Walk-Forward 검증 | ✅ 유지 | 월간 WF 파이프라인 |
| `services/jesse_engine/scripts/sanity_check.py` | 엔진 건전성 검사 | ✅ 유지 | |
| `services/jesse_engine/scripts/generate_v5_report.py` | V5 결과 리포트 생성 | ✅ 유지 | |
| `services/jesse_engine/scripts/regime_split_analysis.py` | 레짐 분할 분석 | ✅ 유지 | |
| `services/jesse_engine/scripts/data/download_binance_vision.py` | Binance 데이터 다운로드 | ✅ 유지 | 데이터 파이프라인 |
| `services/jesse_engine/scripts/data/fetch_coinalyze_funding.py` | Coinalyze 펀딩 수집 | ✅ 유지 | 데이터 파이프라인 |
| `services/jesse_engine/scripts/data/fetch_fear_greed.py` | Fear&Greed 수집 | ✅ 유지 | 데이터 파이프라인 |
| `services/jesse_engine/scripts/data/fetch_fred_macro.py` | FRED 거시경제 수집 | ✅ 유지 | 데이터 파이프라인 |
| `services/jesse_engine/scripts/data/export_funding_rates.py` | 펀딩비율 내보내기 | ✅ 유지 | |
| `services/jesse_engine/scripts/data/build_macro_calendar.py` | 거시경제 캘린더 구성 | ✅ 유지 | |
| `services/jesse_engine/scripts/test_funding_pnl.py` | FundingTracker 유닛 테스트 | ✅ 유지 | |
| `services/jesse_engine/storage/json/*.json` | 개별 백테스트 실행 결과 (UUID 파일명) | 🔍 검토 필요 | 434KB, 용도 불명확 |
| `services/jesse_engine/storage/results/*.json` | 최종 전략 메트릭 | ✅ 유지 | FundingArbitrage_main.json 등 |
| `services/jesse_engine/storage/regime_split/*.json` | 레짐 분할 분석 결과 | ✅ 유지 | |

### 3-E. .request/ 디렉토리 (작업 지시서)

| 경로 | 크기 | 분류 | 비고 |
|------|------|------|------|
| `.request/init/` | 104KB | 🔍 검토 필요 | 초기 아키텍처 문서 (아직 참고 가치?) |
| `.request/backtest_v2/` | 148KB | ❌ 삭제 후보 | 구 v2 백테스트 설계서 (이미 완료) |
| `.request/backtest_v4/` | 24KB | ❌ 삭제 후보 | v4 재구축 계획 (이미 완료) |
| `.request/backtest_v5/` | 28KB | ❌ 삭제 후보 | v5 재구축 계획 (이미 완료) |
| `.request/phase3/` | 172KB | ❌ 삭제 후보 | 과거 Phase 3 계획 |
| `.request/system/` | 248KB | 🔍 검토 필요 | 시스템 재설계 문서 (아키텍처 참고 가치 있을 수 있음) |
| `.request/BT_TASK_10_hmm_llm_meta.md` | 12KB | ❌ 삭제 후보 | 완료된 과제 |
| `.request/project_cleanup_instructions.md` | 6.5KB | ✅ 유지 | 현재 작업 지시서 |

### 3-F. .result/ 디렉토리 (백테스트 결과 문서)

| 경로 | 크기 | 분류 | 비고 |
|------|------|------|------|
| `.result/backtest_v2/` | 184KB | ❌ 삭제 후보 | v2 결과 분석 (이전 버전) |
| `.result/backtest_v3/` | 156KB | ❌ 삭제 후보 | v3 결과 분석 (이전 버전) |
| `.result/backtest_v4/` | 200KB | 🔍 검토 필요 | v4 결과 (V4_MASTER_SUMMARY.md 등 — 참고 가치?) |
| `.result/backtest_v5/` | 40KB | ✅ 유지 | 현재 채택 전략 V5_STRATEGY_SELECTION.md |
| `.result/phase3/` | 148KB | 🔍 검토 필요 | `12_aggressive_fa_report.md` CLAUDE.md에서 참조 중 |
| `.result/phase9/` | 32KB | 🔍 검토 필요 | fa_live_postmortem 등 |
| `.result/phase10/` | 8KB | 🔍 검토 필요 | |
| `.result/phase12/` | 20KB | 🔍 검토 필요 | data_availability.md |
| `.result/phase13/` | 40KB | ✅ 유지 | 최신 정리 결과 keep_delete_decision.md 등 |
| `.result/BACKTEST_V3_INDEX.txt` | 5.4KB | ❌ 삭제 후보 | v3 인덱스 (이전 버전) |
| `.result/phase9_10_summary.md` | 8KB | 🔍 검토 필요 | |

### 3-G. backtest-results/data/ (백테스트 입력 데이터)

| 경로 | 크기 | 분류 | 비고 |
|------|------|------|------|
| `backtest-results/data/binance_vision/klines/` | 5.6M | ✅ 유지 | 백테스트 필수 입력 데이터 |
| `backtest-results/data/binance_vision/funding/` | 280KB | ✅ 유지 | 펀딩비 전략 필수 데이터 |
| `backtest-results/data/fear_greed/` | 32KB | ✅ 유지 | Fear&Greed 지표 |
| `backtest-results/data/funding_rates/BTCUSDT_8h.csv` | 74KB | ✅ 유지 | 펀딩비율 CSV |
| `backtest-results/data/macro_events/fomc_cpi_calendar.csv` | 2.8KB | ✅ 유지 | 거시 이벤트 캘린더 |

### 3-H. 루트 레벨 문서

| 파일 경로 | 역할 | 분류 | 비고 |
|-----------|------|------|------|
| `cryptoengine/CALENDAR_SPREAD_IMPLEMENTATION.md` | 캘린더 스프레드 구현 문서 | 🔍 검토 필요 | 루트 레벨에 있음 — docs/로 이동 또는 삭제 |
| `cryptoengine/docs/HOME.md` | 문서 인덱스 | ✅ 유지 | |
| `cryptoengine/docs/changelog.md` | 변경 이력 | ✅ 유지 | 최근 수정됨 |
| `cryptoengine/docs/EMERGENCY_MANUAL_CLOSE.md` | 긴급 SOP | ✅ 유지 | CLAUDE.md 명시 |
| `cryptoengine/docs/strategies/` | 전략 문서 | ✅ 유지 | |

---

## 4. Dead Code 요약

| 항목 | 위치 | 설명 |
|------|------|------|
| **config_loader.py** | `shared/config_loader.py` | 정의만 되어 있고 어디서도 import 안 됨. `__init__.py` export만 존재 |
| **__pycache__ 잔재** | `services/backtester/tests/backtest/core/__pycache__/` | 소스 파일 삭제 후 남은 .pyc 파일 4개 |
| **TODO 주석** | `services/strategies/funding-arb/cross_exchange.py:91` | `TODO (Phase 2): Implement CoinGlass API integration.` |
| **type: ignore 과다** | `services/telegram-bot/handlers.py` | 52개 — 외부 라이브러리 타입 호환성 문제 (기능에는 영향 없음) |

---

## 5. 백테스트 아티팩트 (사용자 판단 필요)

### UUID JSON 파일 (jesse_engine/storage/json/)
- 총 **434KB**, **20개 파일**, 파일명은 모두 UUID
- 각 백테스트 실행의 원시 결과물로 추정
- 어떤 실행에 대응하는지 확인 필요

### .request/backtest_v2~v5 및 phase3 (총 ~372KB)
- 이미 완료된 백테스트 계획 문서
- `docs/archive/`로 이동 또는 삭제 후보

### .result/backtest_v2~v3 (총 ~340KB)
- 이전 버전 백테스트 결과 분석 문서
- `docs/archive/`로 이동 또는 삭제 후보

### .result/phase3/12_aggressive_fa_report.md
- ⚠️ **주의**: `CLAUDE.md`에서 직접 참조 중 (`fa80_lev4_r30`, `fa80_lev5_r50` 후보 파라미터 설명)
- 내용을 CLAUDE.md에 인라인 요약 후 삭제하거나 보관 필요

---

## 6. 주요 발견사항 요약

### ✅ 양호한 부분
- 서비스 격리 잘 되어 있음 (backtester 외부에서 import 없음)
- 로깅 표준화 우수 (log_events.py 45개 파일에서 사용)
- TODO/FIXME 주석 1개만 존재 (매우 깔끔)
- 모든 서비스에 명확한 main.py 진입점 존재

### ⚠️ 정리 필요한 부분
1. **`services/backtester/`** — 소스 없이 `__pycache__`만 남아있는 빈 껍데기
2. **`shared/config_loader.py`** — 0회 import, 실질적 dead code
3. **구버전 백테스트 문서** — `.request/` & `.result/` 아래 v2~v4, phase3 등 누적
4. **jesse_engine/storage/json/ UUID 파일** — 용도 불명확한 원시 결과물

---

*다음 단계: 사용자 확인 후 Phase 2 (CLEANUP_PLAN.md 작성) 진행*
