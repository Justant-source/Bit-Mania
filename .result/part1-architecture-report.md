# CryptoEngine Part 1: 아키텍처 구현 결과 리포트

> **실행일**: 2026-04-03
> **작업지시서**: `.request/cryptoengine-architecture-part1.md`
> **상태**: 완료

---

## 1. 실행 요약

Part 1 설계서에 명시된 시스템 아키텍처의 모든 핵심 컴포넌트를 구현 완료했습니다.

| 항목 | 상태 | 파일 수 |
|------|------|---------|
| Docker Compose 스택 | 완료 | 2 (prod + dev) |
| 인프라 설정 (.env, Makefile) | 완료 | 2 |
| Config 파일 (YAML) | 완료 | 10 |
| Shared 모듈 | 완료 | 21 |
| Market Data Collector | 완료 | 8 |
| Execution Engine | 완료 | 7 |
| Strategy Orchestrator | 완료 | 8 |
| 전략 - Funding Arb | 완료 | 8 |
| 전략 - Grid Trading | 완료 | 6 |
| 전략 - Adaptive DCA | 완료 | 6 |
| LLM Advisor | 완료 | 22 |
| Telegram Bot | 완료 | 5 |
| Dashboard | 완료 | 5 |
| DB 스키마 | 완료 | 4 |

---

## 2. Docker Compose 구성 (13개 서비스)

`docker-compose.yml` — 전체 스택 정의 완료:

```
인프라:       postgres:16-alpine, redis:7-alpine
핵심 서비스:  market-data, strategy-orchestrator, execution-engine
전략:         funding-arb, grid-trading, adaptive-dca
LLM & 분석:  llm-advisor
모니터링:     telegram-bot, dashboard, grafana
백테스트:     backtester (온디맨드, --profile backtest)
```

- 모든 서비스 `restart: always` 적용 (backtester 제외)
- PostgreSQL/Redis 헬스체크 구성
- 서비스 의존성 체인 설정 (depends_on + healthcheck)
- `docker-compose.dev.yml` — 개발용 오버라이드 (debugpy 포트, 소스 마운트)

---

## 3. Shared 모듈 (21개 파일, ~1,830줄)

### 3.1 데이터 모델 (Pydantic v2)
- `models/market.py` — FundingRate, OHLCV, OrderBook, MarketRegime
- `models/order.py` — OrderRequest (UUID 자동생성), OrderResult
- `models/position.py` — Position, PortfolioState
- `models/strategy.py` — StrategyCommand, StrategyStatus

### 3.2 거래소 커넥터
- `exchange/base.py` — ExchangeConnector ABC (13개 추상 메서드)
- `exchange/bybit.py` — BybitConnector (ccxt.pro, async WebSocket, 테스트넷 지원, 레이트 리밋)
- `exchange/binance.py` — 향후 확장용 플레이스홀더
- `exchange/factory.py` — exchange_factory("bybit") 패턴

### 3.3 데이터베이스
- `db/connection.py` — asyncpg 커넥션 풀 관리
- `db/repository.py` — TradeRepo, PositionRepo, FundingRepo, SnapshotRepo (전체 CRUD)
- `db/migrations/` — Alembic 환경 + 초기 마이그레이션

### 3.4 유틸리티
- `redis_client.py` — Pub/Sub + 캐시 (aioredis)
- `config_loader.py` — YAML 로더 (환경변수 치환 지원)
- `kill_switch.py` — 4단계 Kill Switch (전략/포트폴리오/시스템/수동, 4시간 쿨다운)
- `risk.py` — Drawdown, Sharpe, Sortino, 포지션 사이징, 레버리지 체크
- `logging_config.py` — structlog JSON 로깅 (상관관계 ID 포함)

---

## 4. 핵심 서비스 구현

### 4.1 Market Data Collector (8개 파일)
- **collector.py** — Bybit WebSocket 실시간 수집 (오더북, 체결, 캔들, 펀딩비) + REST 폴링 (OI, 롱숏비율, 청산)
- **regime_detector.py** — 시장 레짐 분류 (ADX/BB/ATR/EMA 기반 → ranging/trending_up/trending_down/volatile)
- **funding_monitor.py** — 펀딩비 추적 + CoinGlass 멀티거래소 비교
- **indicators.py** — ta-lib 래퍼 (EMA, RSI, ADX, ATR, BB, MACD)
- **feature_engine.py** — [FreqAI식] 432+ 피처 생성 파이프라인 (멀티TF x 지표 x 상관페어 x 시프트)

### 4.2 Execution Engine (7개 파일)
- **engine.py** — 주문 처리 메인 루프 (Redis 수신 → 검증 → 실행 → 결과 발행)
- **order_manager.py** — 주문 생명주기 관리 (pending→submitted→filled), 멱등성, 재시도
- **position_tracker.py** — 실시간 포지션 동기화 (Redis 캐시 + DB 영구저장 + 장애복구)
- **safety.py** — 안전 검증 (최대주문크기, 레버리지, 마진, 슬리피지 버퍼, 네트워크 체크)

### 4.3 Strategy Orchestrator (8개 파일)
- **core.py** — 5분 주기 메인 루프 (레짐 수신 → 가중치 조절 → 리스크 평가 → Kill Switch)
- **weight_manager.py** — 레짐별 가중치 매트릭스 (smooth transition)
- **portfolio_monitor.py** — 전체 자산/PnL/Drawdown/Sharpe 추적
- **regime_ml_model.py** — [FreqAI식] LightGBM 레짐 분류 (6시간 재학습, 핫스왑)
- **dissimilarity_index.py** — [FreqAI식] DI 이상치 탐지 → "uncertain" 레짐

---

## 5. 전략 서비스 구현

### 5.1 BaseStrategy (컴포저블 컨트롤러 패턴)
- 추상 인터페이스: tick(), on_start(), on_stop(), get_status(), _rebalance()
- Redis Pub/Sub 명령 수신 + 주문 발행
- Hummingbot V2식 컨트롤러 조합 지원

### 5.2 Funding Rate Arbitrage (8개 파일) — 최우선 전략
- **strategy.py** — 현물 매수 + 선물 숏 동시 진입, 한쪽 미체결 복구, 청산 순서 관리
- **delta_neutral.py** — 수량 일치 확인 (±0.1%), 마진 비율 모니터링, 리밸런싱
- **funding_tracker.py** — 펀딩비 수취 기록 + 시간 인지 (수취 30분 전 청산 차단)
- **basis_spread_sm.py** — [Hummingbot식] 2단계 상태 머신 (Closed↔Opened)
- **cross_exchange.py** — Phase 2 거래소간 차익 플레이스홀더

### 5.3 Grid Trading (6개 파일)
- ADX < 20 + BB 스퀴즈 활성화 조건
- 동적 그리드 범위 (ATR × 3), 20-40개 Post-Only 주문
- 체결 시 반대 주문 자동 생성, 범위 이탈/추세 감지 시 자동 정지

### 5.4 Adaptive DCA (6개 파일)
- 주간 매수 (월요일 UTC 00:00)
- F&G 지수 기반 매수량 조절 (0-10: 3.0x ~ 76-100: 20% 매도)
- Alternative.me API 연동, 평균 단가 추적

---

## 6. LLM Advisor 구현 (22개 파일)

### 6.1 핵심 모듈
- **claude_bridge.py** — Claude Code CLI subprocess 호출 (120s 타임아웃, 3회 재시도)
- **model_manager.py** — 토큰 버킷 레이트 리밋, 장애 시 분석 스킵
- **agent_graph.py** — [TradingAgents식] LangGraph StateGraph (7개 노드, 병렬 분석 → 토론 → 판단)
- **vision_chart.py** — [LLM_trader식] matplotlib 차트 → Claude 비전 분석
- **reflection.py** — [TradingAgents식] 일일 회고 (판단 정확도 추적, ChromaDB 저장)

### 6.2 에이전트 (5개)
- Technical Analyst, Sentiment Analyst, Risk Manager
- Bull Researcher, Bear Researcher (2라운드 토론 구조)

### 6.3 메모리 시스템 (5개)
- **trade_memory.py** — ChromaDB 기반 PnL별 유사상황 검색
- **temporal_decay.py** — 반감기 30일 시간 감쇠
- **hybrid_retrieval.py** — Cosine(0.6) + BM25(0.4) 하이브리드 검색
- **semantic_rules.py** — 자동 규칙 추출 → rules.yaml
- **embeddings.py** — all-MiniLM-L6-v2 임베딩

### 6.4 프롬프트 템플릿 (5개)
- 시장 분석, 레짐 판별, 리스크 평가, 일일 리포트, Bull/Bear 토론

---

## 7. 모니터링 & 알림

### 7.1 Telegram Bot
- 관리 명령: /status, /emergency_close, /stop, /start, /weight, /report
- 자동 알림: 진입/청산, 펀딩비 수취, Kill Switch, 시스템 이상, 일일 리포트

### 7.2 Dashboard
- 내부 대시보드 (포트 3000): 실시간 포지션, PnL, 전략 상태, 시스템 헬스
- 공개 퍼포먼스 (포트 3001): 10분 딜레이, 수익률/누적곡선/Sharpe/Drawdown

### 7.3 Grafana
- PostgreSQL 데이터소스 프로비저닝
- 대시보드 프로비저닝 설정

---

## 8. 데이터베이스 스키마 (11개 테이블)

| 테이블 | 용도 |
|--------|------|
| trades | 거래 기록 |
| positions | 포지션 관리 |
| funding_payments | 펀딩비 수취 내역 |
| portfolio_snapshots | 포트폴리오 스냅샷 |
| daily_reports | 일일 리포트 |
| strategy_states | 전략 상태 |
| kill_switch_events | Kill Switch 이벤트 |
| llm_judgments | LLM 판단 기록 + 회고 |
| ohlcv_history | OHLCV 히스토리 |
| funding_rate_history | 펀딩비 히스토리 |
| grid_orders | 그리드 주문 |
| dca_purchases | DCA 매수 내역 |

- Alembic 마이그레이션 (`001_initial_schema.py`)
- 원시 SQL (`init_schema.sql`)

---

## 9. 설정 파일

| 파일 | 내용 |
|------|------|
| `config/strategies/funding_arb.yaml` | 진입/포지션/청산/리스크/슬리피지/교차거래소 |
| `config/strategies/grid_trading.yaml` | 그리드/활성화/안전/리스크 |
| `config/strategies/adaptive_dca.yaml` | DCA/F&G 멀티플라이어/이익실현 |
| `config/orchestrator.yaml` | 레짐 감지/가중치 매트릭스/Kill Switch/ML 모델 |
| `config/exchanges/bybit.yaml` | Bybit 메인넷+테스트넷 연결 |
| `config/exchanges/binance.yaml` | Binance 플레이스홀더 |
| `config/exchanges/okx.yaml` | OKX 플레이스홀더 |
| `config/telegram.yaml` | 봇 설정/알림 카테고리 |
| `config/grafana/*` | 데이터소스/대시보드 프로비저닝 |

---

## 10. 오픈소스 차용 기술 반영 현황

| 오픈소스 | 차용 기술 | 반영 파일 | 상태 |
|----------|-----------|-----------|------|
| Freqtrade/FreqAI | 자기적응형 재학습 | regime_ml_model.py | 완료 |
| Freqtrade/FreqAI | 대규모 피처 엔지니어링 | feature_engine.py | 완료 |
| Freqtrade/FreqAI | 비유사성 지수 (DI) | dissimilarity_index.py | 완료 |
| Freqtrade/FreqAI | Walk-Forward 백테스트 | walk_forward.py | 완료 |
| Hummingbot | Basis Spread 상태 머신 | basis_spread_sm.py | 완료 |
| Hummingbot | 슬리피지 버퍼 | safety.py | 완료 |
| Hummingbot | 펀딩 시간 인지 | funding_tracker.py | 완료 |
| Hummingbot | 컴포저블 컨트롤러 | base_strategy.py | 완료 |
| TradingAgents | Bull/Bear 토론 | agents/bull,bear_researcher.py | 완료 |
| TradingAgents | LangGraph 상태 그래프 | agent_graph.py | 완료 |
| TradingAgents | 5단계 등급 + 신뢰도 | prompt_templates/ | 완료 |
| TradingAgents | 회고 루프 | reflection.py | 완료 |
| LLM_trader | 시간 감쇠 메모리 | temporal_decay.py | 완료 |
| LLM_trader | 시맨틱 규칙 자동 생성 | semantic_rules.py | 완료 |
| LLM_trader | 비전 AI 차트 분석 | vision_chart.py | 완료 |
| LLM_trader | 하이브리드 검색 | hybrid_retrieval.py | 완료 |
| LLM_trader | PnL 기반 유사상황 검색 | trade_memory.py | 완료 |

---

## 11. Redis 채널 설계 반영

```
market:ohlcv:{exchange}:{symbol}:{tf}    — OHLCV 데이터
market:funding:{exchange}:{symbol}       — 펀딩비 업데이트
market:orderbook:{exchange}:{symbol}     — 오더북 스냅샷
market:regime                            — 시장 레짐 변경
strategy:command:{strategy_id}           — Orchestrator → 전략
strategy:status:{strategy_id}            — 전략 → Orchestrator
order:request                            — 전략 → Execution Engine
order:result:{strategy_id}              — Execution Engine → 전략
alert:telegram                           — 알림 메시지
alert:kill_switch                        — Kill Switch 발동
llm:request / llm:response              — LLM 분석 요청/결과
cache:position/portfolio_state/regime    — Key-Value 캐시
```

---

> Part 1 아키텍처 설계서의 모든 요구사항이 코드로 구현되었습니다.
