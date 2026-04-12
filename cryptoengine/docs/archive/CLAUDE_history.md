# CryptoEngine 개발 이력 아카이브

> 이 파일은 CLAUDE.md에서 이전된 개발 히스토리입니다.
> 현행 규칙은 CLAUDE.md를 참조하세요.

## 완료된 Phase 이력 (2026-04-12 기준)

- Phase 0 완료: Docker, PostgreSQL, Redis, Grafana 기동
- Phase 1 완료: Bybit 테스트넷 API 키 설정 (10,000 USDT)
- Phase 2 완료: 서비스 기동 + 연결 검증, Redis Pub/Sub 데이터 흐름 확인
- Phase 3 완료: 백테스트 (6년 히스토리 데이터, fa80_lev5_r30 채택, CAGR +34.87% Sharpe 3.583)

## Phase 4 완료 항목 (테스트넷 포워드 테스트)

### 기본 인프라
- 통합 구조화 로깅 (structlog), KST 타임스탬프, OHLCV 보존 정책, 자동 백업 (pg-backup)
- startup gap recovery, 복원력 테스트 자동화
- 포지션 정합성 체크 (reconciliation, 3분 간격)

### 모니터링 및 알림
- 레짐 모니터링 대시보드, Telegram 파일 관리 + 인라인 키보드
- AlertDispatcher (배치·레이트리밋·dedup), Grafana→Telegram 단일 경로, Kill Switch ACK 확인
- log-retention 서비스 (보존 정책 자동화), wf-scheduler 서비스 (월간 WF 자동화)
- SafetyGuard 유닛 테스트 27개, Telegram 포매터 테스트 59개

### 안전장치 및 복구
- 배포 재시작 시 포지션 자동 복구 (service_shutdown → Redis 저장 → 재시작 후 복원, 불필요한 청산 수수료 제거)
- Phase 5 preflight 스크립트 (8개 항목 점검)
- stoploss_on_exchange (진입가 ±2% StopMarket, 자동 배치/취소/복구)

### 포지션 및 위험 관리
- Phase 5 포지션 사이징 재설계 (`fixed_notional` 모드, $150 고정 명목가, 소액 최소 주문 대응)
- Kill Switch 절대값 임계값 (퍼센트 AND 절대값 USD 둘 다 초과해야 발동, 수수료 노이즈 오발동 방지)
- 진입 조건 강화 (연 25% / 4회 연속 / NetProfitabilityCheck BEP 2회)

### 운영 및 전환
- 메인넷 전환 스크립트 (`scripts/switch_to_mainnet.py` 9단계, `scripts/switch_to_testnet.py` 6단계 롤백)
- STRICT_MONITORING 모드 (24시간 강화 모니터링, 1시간 강제 리포트, 마진비율 경고)
- 비상 수동 청산 SOP 문서 (`docs/EMERGENCY_MANUAL_CLOSE.md`, 휴대폰 저장용 5단계 요약 포함)
- 잔고 동기화 검증 (`EXPECTED_INITIAL_BALANCE_USD` 기반, 5% 이상 차이 시 시작 거부)
- orchestrator/core.py Phase 5 Kill Switch 연결 (`_build_kill_switch()`, `equity_at_open` 전달)

## 백테스트 v2 재건 상세 (2026-04-11)

### 근본 원인 진단

문서: `.result/backtest_v2/DIAGNOSIS_REBUILD_ROADMAP.md`

- **확정 버그 3개**: 멀티심볼 펀딩비 필터링, HMM+LLM 수수료 오류, 극단치 역발상
- **합성 데이터 오염**: CoinMetrics, Fear&Greed, Calendar Spread, ETF Flow 전략 데이터 부족
- **Jesse 선정**: 실데이터 백테스트 신뢰성 확보

### Track A: 실데이터 파이프라인

5개 수집 스크립트 완성:
1. `download_binance_vision.py` — Binance Vision 데이터 (OHLCV)
2. `fetch_coinalyze_funding.py` — Coinalyze 펀딩비 API
3. `fetch_fear_greed.py` — Fear & Greed Index
4. `fetch_fred_macro.py` — FRED 거시 지표
5. `export_pg_to_parquet.py` — PostgreSQL → Parquet 변환

### Track B: Jesse 프레임워크 통합

- `jesse_project/` 디렉토리 구조 구축
- FundingArb, MultiFundingRotation 전략 포팅
- jesse >= 0.41.0 설치

### Track C: 버그 수정

1. **멀티심볼 펀딩비 필터링**: `funding≤0` → `funding<MIN_THRESHOLD`
2. **HMM+LLM 수수료 오류**:
   - `TAKER_FEE` 0.0002 → 0.00055 (Bybit 정확한 수수료율)
   - `MIN_HOLD_BARS=4` 설정 (짧은 홀드 거래 필터링)
3. **극단치 역발상**: `abs()` 제거 (정방향 신호만 사용)

### Track D: 합성 데이터 무음 폴백 제거

- CoinMetrics 합성 데이터 제거
- Fear&Greed 무음 처리 제거
- Calendar Spread 합성 데이터 제거
- ETF Flow 합성 데이터 제거

## Phase 13–14 완료: Jesse FA 포팅 (2026-04-12)

### Phase 13: 자체 엔진 제거

- 실패 전략 코드 완전 삭제 (MultiSymbolFunding, HMM+LLM, CalendarSpread 등)
- 데이터 수집 스크립트 `jesse_engine` 디렉토리로 마이그레이션
- 구 백테스트 엔진 (`services/backtester/`) 서비스에서 분리

### Phase 14: Jesse 실데이터 백테스트

- `jesse_import.py` 개선:
  - YYYY/MM.parquet 레이아웃 지원
  - datetime 타임스탬프 처리
- `fa80_lev5_r30` 설정 실데이터 재현 확인
- Jesse 엔진으로 본격 운영 전환

## 남은 백테스트 TODO (Phase 14 이후)

### 1. 실데이터 수집 재실행 (아직 미실행 시)

```bash
docker compose --profile backtest run --rm jesse_engine python scripts/download_binance_vision.py
docker compose --profile backtest run --rm jesse_engine python scripts/fetch_coinalyze_funding.py
docker compose --profile backtest run --rm jesse_engine python scripts/fetch_fear_greed.py
docker compose --profile backtest run --rm jesse_engine python scripts/fetch_fred_macro.py
```

### 2. MultiFundingRotation Jesse 포팅

Reference: `jesse_project/strategies/FundingArb.py`

`Rotation` 전략도 동일 패턴으로 포팅:
- 신호 생성 로직 → Jesse `@strategy.method` 함수
- 자본 배분 → Jesse 포지션 크기 API
- 청산 신호 → Jesse 엑시트 로직

### 3. 수정된 전략 재실행

버그 수정 (Track C) 반영 확인:

1. `fa/bt_multi_symbol_funding_rotation.py` 재실행
   - 진입 DIAGNOSTIC 로그 확인 후 full run
   - 거래 횟수 감소 확인

2. `combined/bt_hmm_llm_meta_strategy.py` 재실행
   - TAKER_FEE, MIN_HOLD_BARS 반영 확인
   - 거래 횟수 ~950 → ~300으로 감소했는지 확인

### 4. Calendar Spread 실데이터 연결

1. `analysis/quarterly_futures_collector.py` 실행
   - 분기 선물 데이터 수집 (Bybit API)

2. `fa/bt_calendar_spread.py` 재실행
   - synthetic-mode 제거
   - 실데이터만 사용하여 재검증
