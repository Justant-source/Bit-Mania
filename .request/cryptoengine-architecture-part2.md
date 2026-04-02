# 비트코인 선물 자동매매 시스템 설계서 — Part 2: 구현 계획

> **프로젝트명**: CryptoEngine
> **이 문서**: 디렉토리 구조, 개발 순서, 테스트 계획, 운영 체크리스트

---

## 8. 프로젝트 디렉토리 구조

```
cryptoengine/
├── docker-compose.yml                    # 전체 스택 정의
├── docker-compose.dev.yml                # 개발용 오버라이드
├── .env.example                          # 환경변수 템플릿
├── .env                                  # 실제 환경변수 (git 제외)
├── Makefile                              # 자주 쓰는 명령 단축
│
├── config/
│   ├── strategies/
│   │   ├── funding_arb.yaml              # 펀딩비 전략 파라미터
│   │   ├── grid_trading.yaml             # 그리드 전략 파라미터
│   │   └── adaptive_dca.yaml             # DCA 전략 파라미터
│   ├── orchestrator.yaml                 # 레짐별 가중치, Kill Switch 임계값
│   ├── exchanges/
│   │   ├── bybit.yaml                    # Bybit 연결 설정
│   │   ├── binance.yaml                  # (향후)
│   │   └── okx.yaml                      # (향후)
│   ├── grafana/
│   │   ├── dashboards/                   # 프로비저닝 대시보드 JSON
│   │   └── datasources/                  # PostgreSQL 데이터소스
│   └── telegram.yaml                     # Telegram Bot 설정
│
├── shared/                               # 모든 서비스가 공유하는 코드
│   ├── __init__.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── market.py                     # OHLCV, FundingRate, OrderBook
│   │   ├── order.py                      # OrderRequest, OrderResult
│   │   ├── position.py                   # Position, PortfolioState
│   │   └── strategy.py                   # StrategyCommand, StrategyStatus
│   ├── exchange/
│   │   ├── __init__.py
│   │   ├── base.py                       # ExchangeConnector ABC
│   │   ├── bybit.py                      # Bybit CCXT 래퍼
│   │   ├── binance.py                    # (향후)
│   │   └── factory.py                    # exchange_factory("bybit") → BybitConnector
│   ├── db/
│   │   ├── __init__.py
│   │   ├── connection.py                 # asyncpg 풀 관리
│   │   ├── repository.py                 # TradeRepo, PositionRepo, FundingRepo
│   │   └── migrations/                   # Alembic 마이그레이션
│   │       ├── env.py
│   │       └── versions/
│   ├── redis_client.py                   # Redis 연결 + Pub/Sub 헬퍼
│   ├── config_loader.py                  # YAML 설정 로더
│   ├── kill_switch.py                    # Kill Switch 공통 로직
│   ├── risk.py                           # 리스크 계산 유틸 (drawdown, sharpe)
│   └── logging_config.py                 # 구조화 로깅 (JSON 포맷)
│
├── services/
│   ├── market-data/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── main.py                       # 진입점
│   │   ├── collector.py                  # WebSocket + REST 데이터 수집
│   │   ├── regime_detector.py            # ADX, BB, ATR → 시장 레짐 분류
│   │   ├── funding_monitor.py            # 펀딩비 추적 + 멀티 거래소 비교
│   │   ├── indicators.py                 # ta-lib 래퍼 (EMA, RSI, ADX, ATR, BB)
│   │   └── feature_engine.py             # [NEW] FreqAI식 대규모 피처 생성 파이프라인
│   │
│   ├── orchestrator/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── main.py
│   │   ├── core.py                       # 전략 조율 메인 루프
│   │   ├── weight_manager.py             # 레짐별 가중치 계산
│   │   ├── portfolio_monitor.py          # 포트폴리오 상태 추적
│   │   ├── regime_ml_model.py            # FreqAI에서 export한 모델 로드
│   │   └── dissimilarity_index.py        # [NEW] FreqAI DI — 학습외 상황 감지
│   │
│   ├── execution/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── main.py
│   │   ├── engine.py                     # 주문 처리 메인 루프
│   │   ├── order_manager.py              # 주문 생명주기 관리
│   │   ├── position_tracker.py           # 실시간 포지션 동기화
│   │   └── safety.py                     # 주문 검증 (크기, 레버리지, 마진)
│   │
│   ├── strategies/
│   │   ├── base_strategy.py              # BaseStrategy ABC
│   │   │
│   │   ├── funding-arb/
│   │   │   ├── Dockerfile
│   │   │   ├── requirements.txt
│   │   │   ├── main.py
│   │   │   ├── strategy.py               # FundingArbStrategy(BaseStrategy)
│   │   │   ├── delta_neutral.py          # 델타 뉴트럴 포지션 관리
│   │   │   ├── funding_tracker.py        # 펀딩비 수취 기록 + 시간 인지
│   │   │   ├── basis_spread_sm.py        # [NEW] Hummingbot식 2단계 상태 머신
│   │   │   └── cross_exchange.py         # Phase 2: 거래소 간 차익
│   │   │
│   │   ├── grid-trading/
│   │   │   ├── Dockerfile
│   │   │   ├── requirements.txt
│   │   │   ├── main.py
│   │   │   ├── strategy.py               # GridStrategy(BaseStrategy)
│   │   │   ├── grid_calculator.py        # 그리드 가격/수량 계산
│   │   │   └── grid_state.py             # 그리드 주문 상태 추적
│   │   │
│   │   └── adaptive-dca/
│   │       ├── Dockerfile
│   │       ├── requirements.txt
│   │       ├── main.py
│   │       ├── strategy.py               # AdaptiveDCAStrategy(BaseStrategy)
│   │       ├── fear_greed.py             # F&G Index 수집
│   │       └── scheduler.py              # 주간 매수 스케줄링
│   │
│   ├── llm-advisor/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── main.py
│   │   ├── claude_bridge.py              # Claude Code CLI 호출
│   │   ├── model_manager.py              # [NEW] Claude Code 호출 관리 + 에러 대응
│   │   ├── agent_graph.py                # [NEW] LangGraph 상태 그래프 오케스트레이션
│   │   ├── vision_chart.py               # [NEW] LLM_trader식 비전 AI 차트 분석
│   │   ├── reflection.py                 # [NEW] TradingAgents식 일일 회고 루프
│   │   ├── prompt_templates/
│   │   │   ├── market_analysis.py        # 시장 분석 프롬프트
│   │   │   ├── regime_assessment.py      # 레짐 판별 프롬프트
│   │   │   ├── risk_evaluation.py        # 리스크 평가 프롬프트
│   │   │   ├── daily_report.py           # 일일 리포트 생성 프롬프트
│   │   │   └── debate_prompts.py         # [NEW] Bull/Bear 토론 프롬프트
│   │   ├── agents/                       # TradingAgents 구조 차용
│   │   │   ├── technical_analyst.py
│   │   │   ├── sentiment_analyst.py
│   │   │   ├── risk_manager.py
│   │   │   ├── bull_researcher.py        # [NEW] 강세 관점 에이전트
│   │   │   └── bear_researcher.py        # [NEW] 약세 관점 에이전트
│   │   └── memory/                       # LLM_trader RAG 구조 차용
│   │       ├── trade_memory.py           # ChromaDB 저장/검색 + PnL 기반
│   │       ├── embeddings.py             # sentence-transformers
│   │       ├── temporal_decay.py         # [NEW] 시간 감쇠 가중치
│   │       ├── semantic_rules.py         # [NEW] 자동 규칙 추출 + 저장
│   │       └── hybrid_retrieval.py       # [NEW] Cosine + BM25 하이브리드 검색
│   │
│   ├── telegram-bot/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── main.py
│   │   ├── handlers.py                   # 명령어 핸들러
│   │   └── formatters.py                 # 메시지 포매팅
│   │
│   ├── dashboard/
│   │   ├── Dockerfile
│   │   ├── package.json
│   │   ├── src/
│   │   │   ├── server.ts                 # Express/Fastify 서버
│   │   │   ├── routes/
│   │   │   │   ├── internal.ts           # 내부 대시보드 API
│   │   │   │   └── public.ts             # 공개 퍼포먼스 API
│   │   │   └── frontend/
│   │   │       ├── internal/             # React — 내부 관리 화면
│   │   │       └── public/               # React — 공개 수익 페이지
│   │   └── tsconfig.json
│   │
│   └── backtester/
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── main.py
│       ├── freqtrade_bridge.py           # Freqtrade 백테스트 실행
│       ├── walk_forward.py               # Walk-Forward 분석
│       └── report_generator.py           # 백테스트 결과 리포트
│
├── scripts/
│   ├── init_db.py                        # DB 초기화 + 마이그레이션
│   ├── seed_historical.py                # 과거 데이터 시딩
│   ├── export_trades.py                  # 거래 내역 CSV 추출
│   ├── health_check.py                   # 전체 시스템 헬스 체크
│   └── generate_monthly_report.py        # 월간 리포트 생성
│
├── tests/
│   ├── unit/
│   │   ├── test_delta_neutral.py
│   │   ├── test_grid_calculator.py
│   │   ├── test_regime_detector.py
│   │   ├── test_risk.py
│   │   ├── test_kill_switch.py
│   │   └── test_order_manager.py
│   ├── integration/
│   │   ├── test_bybit_connector.py       # Bybit 테스트넷
│   │   ├── test_execution_flow.py        # 주문 → 체결 → 기록
│   │   ├── test_funding_arb_e2e.py       # 펀딩비 전략 E2E
│   │   └── test_orchestrator_regime.py   # 레짐 변경 → 가중치 전환
│   ├── backtest/
│   │   ├── bt_funding_arb.py             # 펀딩비 백테스트
│   │   ├── bt_grid.py                    # 그리드 백테스트
│   │   └── bt_combined.py               # 복합 전략 백테스트
│   └── conftest.py                       # pytest 공통 fixture
│
├── docs/
│   ├── architecture.md                   # 이 문서 (Part 1)
│   ├── implementation.md                 # 이 문서 (Part 2)
│   ├── strategies/
│   │   ├── funding_arb.md                # 전략 상세 문서
│   │   ├── grid_trading.md
│   │   └── adaptive_dca.md
│   ├── runbook.md                        # 운영 매뉴얼 (장애 대응)
│   ├── api.md                            # 내부 API 문서
│   └── changelog.md                      # 변경 이력
│
└── notebooks/                            # 리서치 & 분석
    ├── funding_rate_analysis.ipynb       # 펀딩비 데이터 분석
    ├── regime_detection.ipynb            # 레짐 감지 모델 실험
    ├── backtest_results.ipynb            # 백테스트 결과 시각화
    └── grid_optimization.ipynb           # 그리드 파라미터 최적화
```

---

## 9. BaseStrategy 인터페이스

모든 전략이 준수하는 공통 인터페이스. 이것이 있어야 Orchestrator가 균일하게 전략을 제어할 수 있다.

```python
# services/strategies/base_strategy.py

from abc import ABC, abstractmethod
from shared.models.strategy import StrategyCommand, StrategyStatus
from shared.models.order import OrderRequest
from shared.redis_client import RedisClient

class BaseStrategy(ABC):
    """모든 전략의 기반 클래스"""

    def __init__(self, strategy_id: str, config: dict):
        self.strategy_id = strategy_id
        self.config = config
        self.is_running = False
        self.allocated_capital = 0.0
        self.max_drawdown = config.get("max_drawdown", 0.02)
        self.redis = RedisClient()

    async def run(self):
        """메인 루프 — Orchestrator 명령 수신 + 전략 틱 실행"""
        await self.redis.subscribe(
            f"strategy:command:{self.strategy_id}",
            self._handle_command
        )
        while True:
            if self.is_running:
                await self.tick()
            await asyncio.sleep(self.config.get("tick_interval", 5))

    @abstractmethod
    async def tick(self):
        """전략의 메인 로직. 매 틱마다 호출."""
        pass

    @abstractmethod
    async def on_start(self, capital: float, params: dict):
        """전략 시작 시 초기화"""
        pass

    @abstractmethod
    async def on_stop(self, reason: str):
        """전략 정지 시 포지션 정리"""
        pass

    @abstractmethod
    async def get_status(self) -> StrategyStatus:
        """현재 상태 반환"""
        pass

    async def submit_order(self, order: OrderRequest):
        """Execution Engine에 주문 전달"""
        order.strategy_id = self.strategy_id
        await self.redis.publish("order:request", order.to_json())

    async def _handle_command(self, command: StrategyCommand):
        """Orchestrator 명령 처리"""
        if command.action == "start":
            self.allocated_capital = command.allocated_capital
            self.max_drawdown = command.max_drawdown
            await self.on_start(command.allocated_capital, command.params)
            self.is_running = True

        elif command.action == "stop":
            self.is_running = False
            await self.on_stop(reason="orchestrator_command")

        elif command.action == "adjust_weight":
            self.allocated_capital = command.allocated_capital
            # 전략이 실행 중에 자본 배분 변경 시 포지션 조절
            await self._rebalance(command.allocated_capital)

    @abstractmethod
    async def _rebalance(self, new_capital: float):
        """자본 배분 변경 시 포지션 재조정"""
        pass
```

---

## 10. 설정 파일 예시

```yaml
# config/strategies/funding_arb.yaml
strategy:
  id: "funding_arb"
  enabled: true
  tick_interval: 60               # 초 단위 (1분마다 체크)

  # 진입 조건
  entry:
    min_funding_rate: 0.005       # 0.5% 이상 시 진입 (연 5.5% 환산)
    max_spot_futures_spread: 0.003  # 현물-선물 괴리 0.3% 이내
    position_mode: "hedge"         # 헤지 모드

  # 포지션 관리
  position:
    leverage: 2                    # 선물 레버리지 (보수적)
    margin_safety_ratio: 3.0       # 유지마진 대비 3배 여유
    rebalance_threshold: 0.001     # 현물-선물 수량 0.1% 이상 차이 시 리밸런싱

  # 청산 조건
  exit:
    funding_rate_reversal: -0.005  # -0.5% 이하 시 청산
    max_spread_divergence: 0.01    # 현물-선물 1% 이상 괴리 시 청산
    max_hours_negative: 24         # 24시간 연속 음의 펀딩 시 청산

  # 리스크
  risk:
    max_drawdown: 0.03             # 3% 최대 낙폭
    max_position_pct: 0.5          # 배분 자본의 50% 이하로 포지션

  # Phase 2: 거래소 간 차익 (향후)
  cross_exchange:
    enabled: false
    exchanges: ["bybit", "binance"]
    min_rate_diff: 0.02            # 거래소 간 0.02% 이상 차이 시
```

```yaml
# config/strategies/grid_trading.yaml
strategy:
  id: "grid_trading"
  enabled: true
  tick_interval: 30

  grid:
    num_grids: 30
    range_multiplier: 3.0          # ATR(14) × 3 = 상하 범위
    min_grid_spacing_pct: 0.002    # 최소 그리드 간격 0.2%
    order_type: "limit"
    post_only: true

  activation:
    max_adx: 20                    # ADX 20 이하에서만 가동
    min_bb_squeeze: true           # 볼린저 밴드 수축 시 우선 가동

  safety:
    range_escape_buffer: 0.05      # 범위 이탈 5% 시 정지
    trend_detection_adx: 25        # ADX 25 초과 시 정지
    max_open_orders: 60            # 동시 미체결 주문 한도

  risk:
    max_drawdown: 0.05
    max_position_pct: 0.7
```

```yaml
# config/orchestrator.yaml
orchestrator:
  tick_interval: 300               # 5분마다 레짐 + 가중치 체크

  regime_detection:
    adx_period: 14
    atr_period: 14
    bb_period: 20
    lookback_candles: 100
    timeframe: "1h"

  weights:
    ranging:
      funding_arb: 0.50
      grid_trading: 0.35
      adaptive_dca: 0.05
      cash: 0.10
    trending_up:
      funding_arb: 0.55
      grid_trading: 0.15
      adaptive_dca: 0.15
      cash: 0.15
    trending_down:
      funding_arb: 0.55
      grid_trading: 0.15
      adaptive_dca: 0.20
      cash: 0.10
    volatile:
      funding_arb: 0.40
      grid_trading: 0.10
      adaptive_dca: 0.15
      cash: 0.35

  kill_switch:
    daily_loss_pct: -0.01          # 일일 -1%
    weekly_loss_pct: -0.03         # 주간 -3%
    monthly_loss_pct: -0.05        # 월간 -5%
    cooldown_hours: 4              # Kill Switch 후 4시간 대기
    auto_resume: true              # 쿨다운 후 자동 재개
```

---

## 11. 개발 로드맵 (Phase별)

### Sprint 0: 기반 인프라 (1주)

```
목표: Docker 스택 기동 + DB + Redis + 거래소 연결 확인

[ ] Docker Compose 파일 작성 및 전체 스택 기동 테스트
[ ] PostgreSQL 스키마 생성 (Alembic 마이그레이션)
[ ] Redis 연결 테스트 (Pub/Sub 동작 확인)
[ ] shared/ 모듈: 모델 정의, Redis 클라이언트, DB 커넥션 풀
[ ] Bybit 테스트넷 API 키 발급 + CCXT 연결 테스트
[ ] 구조화 로깅 설정 (JSON → stdout → Docker logs)
[ ] .env 파일 구성 (API 키, DB 비밀번호 등)
[ ] Makefile 작성 (make up, make down, make logs, make test 등)
```

### Sprint 1: Market Data + Execution Engine (1주)

```
목표: 실시간 데이터 수집 + 주문 실행 기능 완성

[ ] market-data 서비스: Bybit WebSocket (OHLCV, 오더북, 체결)
[ ] market-data 서비스: Bybit REST (펀딩비, OI, 롱숏비율)
[ ] market-data 서비스: indicators.py (ta-lib: EMA, RSI, ADX, ATR, BB)
[ ] market-data 서비스: regime_detector.py (시장 레짐 분류)
[ ] execution 서비스: OrderManager (지정가/시장가 주문 처리)
[ ] execution 서비스: PositionTracker (실시간 포지션 동기화)
[ ] execution 서비스: Safety 체크 (최대 주문 크기, 레버리지 한도)
[ ] 통합 테스트: 데이터 수집 → Redis 발행 → Execution 수신 확인
[ ] Bybit 테스트넷에서 주문 실행 테스트
```

### Sprint 2: 펀딩비 차익거래 전략 (2주) ← 최우선

```
목표: 펀딩비 차익거래 전략 완성 + 테스트넷 검증

Week 1:
[ ] BaseStrategy 인터페이스 구현 (컴포저블 컨트롤러 패턴)
[ ] FundingArbStrategy 핵심 로직
    - 펀딩비 모니터링 + 진입 조건 판별
    - 현물 매수 + 선물 숏 동시 진입 로직
    - Post-Only 주문 + 미체결 처리
    - 한쪽만 체결 시 복구 로직
[ ] DeltaNeutral 포지션 매니저
    - 현물-선물 수량 일치 확인
    - 마진 비율 모니터링
    - 리밸런싱 로직
[ ] [Hummingbot 차용] BasisSpreadStateMachine
    - Closed → Opened 2단계 상태 전환
    - min_divergence / min_convergence 파라미터
[ ] [Hummingbot 차용] FundingPaymentScheduler
    - 수취 30분 전 청산 차단, 수취 직후 복귀
[ ] [Hummingbot 차용] 슬리피지 버퍼 (spot/perp 양쪽)

Week 2:
[ ] FundingTracker: 펀딩비 수취 기록 + 수익 계산
[ ] 청산 로직 (펀딩비 역전환, basis divergence, Kill Switch)
[ ] Bybit 테스트넷 E2E 테스트 (진입 → 펀딩비 수취 → 청산)
[ ] 백테스트: 최근 6개월 펀딩비 + basis spread 시뮬레이션
[ ] 스트레스 테스트: 급등/급락 시나리오
[ ] 단위 테스트 전체 작성
```

### Sprint 3: Orchestrator + Kill Switch (1주)

```
목표: 전략 조율 + 안전장치 완성

[ ] Orchestrator 메인 루프 (레짐 감지 → 가중치 조절)
[ ] WeightManager (레짐별 가중치 매트릭스 적용)
[ ] PortfolioMonitor (전체 자본, PnL, Drawdown 추적)
[ ] [FreqAI 차용] LightGBM 레짐 분류 모델 탑재
    - feature_engine.py에서 432+ 피처 생성
    - 백그라운드 스레드에서 6시간마다 재학습
[ ] [FreqAI 차용] Dissimilarity Index 구현
    - 학습 외 상황 감지 → "uncertain" 레짐 전달
[ ] Kill Switch 구현
    - Level 1: 전략 내 손절
    - Level 2: 포트폴리오 레벨 (-1% 일일)
    - Level 3: 시스템 레벨 (헬스체크 실패)
    - Level 4: Telegram 비상 명령
[ ] Kill Switch 테스트 (각 레벨별 시나리오)
[ ] 자본 배분 로직 테스트 (레짐 전환 시 가중치 변경)
```

### Sprint 4: 그리드 트레이딩 + DCA (1주)

```
목표: 보조 전략 완성

[ ] GridStrategy 구현
    - 그리드 가격/수량 계산 (ATR 기반 동적 범위)
    - 그리드 주문 배치 + 상태 추적
    - 체결 시 반대 주문 자동 생성
    - 범위 이탈 시 자동 정지 + 청산
[ ] AdaptiveDCAStrategy 구현
    - F&G Index 수집 (Alternative.me API)
    - 지수 구간별 매수량 자동 조절
    - 주간 스케줄러
[ ] 두 전략 테스트넷 검증
[ ] Orchestrator와 통합 테스트 (레짐 변경 시 가동/정지)
```

### Sprint 5: LLM Advisor 통합 (2주) ← 기간 확대

```
목표: 4개 오픈소스의 검증된 LLM 기술을 모두 통합

Week 1 — 기본 인프라 + 에이전트:
[ ] model_manager.py: Claude Code 호출 관리 + 에러 시 분석 스킵 로직
    - Claude Code Max 단일 사용 (비용 무료, 최고 품질)
    - LLM 장애 시 전략 실행은 독립 동작 유지
[ ] ClaudeCodeBridge 구현 (subprocess + JSON 파싱)
[ ] [TradingAgents 차용] LangGraph agent_graph.py
    - StateGraph 기반 에이전트 오케스트레이션
    - 분석(병렬) → 토론(순차) → 판단 파이프라인
[ ] [TradingAgents 차용] Bull/Bear 토론 구조
    - bull_researcher.py / bear_researcher.py
    - debate_prompts.py (2라운드 토론 프롬프트)
    - 5단계 등급 (Strong Buy ~ Strong Sell) + 신뢰도
[ ] [LLM_trader 차용] vision_chart.py
    - matplotlib 차트 이미지 생성 → Claude 비전 분석

Week 2 — 메모리 + 학습 시스템:
[ ] [LLM_trader 차용] ChromaDB 트레이딩 메모리
    - PnL 기반 유사 상황 검색 (성공/실패 분리)
    - 시장 피처 + 전략 + 결과를 벡터로 저장
[ ] [LLM_trader 차용] temporal_decay.py
    - 반감기 30일 시간 감쇠 가중치
[ ] [LLM_trader 차용] hybrid_retrieval.py
    - Cosine + BM25 하이브리드 검색
[ ] [TradingAgents 차용] reflection.py
    - 매일 00:00 회고: LLM 판단 vs 실제 결과 비교
    - 정확/부정확 사례를 ChromaDB에 구분 저장
[ ] [LLM_trader 차용] semantic_rules.py
    - 주간 자동 규칙 추출 + rules.yaml 업데이트
[ ] LLM 응답 → Orchestrator 가중치 조정 연동
    - confidence > 0.5 일 때만 반영
[ ] 프롬프트 템플릿 전체 작성 (시장분석, 레짐, 리스크, 리포트)
```

### Sprint 6: Dashboard + Telegram + 모니터링 (1주)

```
목표: 투명한 수익 공개 시스템 완성

[ ] Dashboard 백엔드 (포트폴리오 API, 거래 내역 API)
[ ] 공개 퍼포먼스 페이지 (일별/주별/월별 수익률, 누적 곡선)
[ ] 내부 관리 대시보드 (실시간 포지션, 시스템 상태)
[ ] Grafana 대시보드 프로비저닝
    - 펀딩비 수취 히스토리
    - 체결률, 슬리피지 분석
    - 시스템 메트릭 (CPU, RAM)
[ ] Telegram Bot 구현 (명령어 + 자동 알림)
[ ] 10분 딜레이 로직 (공개 API에서만 적용)
```

### Sprint 7: 백테스트 검증 + 실전 준비 (2주)

```
목표: 전체 시스템 최종 검증 + 소액 실전 전환

Week 1 — 백테스트 & 스트레스 테스트:
[ ] 펀딩비 전략: 2023~2026 데이터 백테스트
[ ] 그리드 전략: 횡보/추세 구간별 백테스트
[ ] 복합 전략: Orchestrator 포함 전체 시뮬레이션
[ ] Walk-Forward 분석 (6개월 In-Sample → 3개월 Out-of-Sample)
[ ] 몬테카를로 시뮬레이션 (100회 랜덤 리샘플링)
[ ] 스트레스 테스트: 2022 LUNA 폭락, 2025 Bybit 해킹 시나리오

Week 2 — 실전 전환 준비:
[ ] Bybit 실계좌 API 키 발급 (trade-only, IP 제한, 출금 불가)
[ ] 소액 ($500) 실전 테스트 시작
[ ] 24시간 × 3일 무중단 가동 테스트
[ ] Docker 자동 재시작 + 헬스체크 검증
[ ] 장애 시나리오 훈련 (네트워크 끊김, API 다운, DB 크래시)
[ ] 운영 매뉴얼(runbook.md) 작성
```

---

## 12. 핵심 기술 스택 정리

```
언어:          Python 3.12 (서비스 전체), TypeScript (Dashboard)
비동기:        asyncio + aiohttp + websockets
거래소:        CCXT Pro (비동기 WebSocket 지원)
DB:            PostgreSQL 16 + asyncpg
캐시/MQ:       Redis 7 (Pub/Sub + 캐시)
벡터 DB:       ChromaDB (LLM 메모리 + PnL 기반 검색)
ML:            scikit-learn + LightGBM (레짐 분류), ta-lib (지표)
딥러닝:        PyTorch (LSTM 가격예측, Phase 2), stable-baselines3 (RL, Phase 3)
LLM:           Claude Code Max (단일 — 모든 분석/토론/회고/리포트 처리)
에이전트:      LangGraph (상태 그래프 기반 멀티 에이전트 오케스트레이션)
임베딩:        sentence-transformers (all-MiniLM-L6-v2)
하이브리드검색: rank-bm25 (BM25) + ChromaDB (dense)
피처엔진:      FreqAI 패턴의 멀티TF × 상관페어 × 시프트 확장
백테스트:      Freqtrade (독립 도구로 활용)
Dashboard:     React + Recharts + TailwindCSS
모니터링:      Grafana + PostgreSQL datasource
알림:          python-telegram-bot
컨테이너:      Docker Compose (WSL Ubuntu)
테스트:        pytest + pytest-asyncio
린트:          ruff + mypy
```

---

## 13. Makefile

```makefile
# 자주 쓰는 명령

.PHONY: up down logs test backtest status

# 전체 스택 기동
up:
	docker compose up -d

# 전체 스택 중지
down:
	docker compose down

# 로그 확인
logs:
	docker compose logs -f --tail=100

# 특정 서비스 로그
logs-%:
	docker compose logs -f --tail=100 $*

# 전체 테스트
test:
	docker compose run --rm backtester pytest tests/ -v

# 단위 테스트만
test-unit:
	docker compose run --rm backtester pytest tests/unit/ -v

# 백테스트 실행
backtest:
	docker compose --profile backtest run --rm backtester python main.py

# 시스템 상태 확인
status:
	docker compose ps
	@echo "---"
	python scripts/health_check.py

# DB 마이그레이션
migrate:
	docker compose exec orchestrator alembic upgrade head

# Kill Switch (비상 전체 청산)
emergency:
	docker compose exec execution-engine python -c \
		"from engine import emergency_close_all; emergency_close_all()"

# 월간 리포트 생성
monthly-report:
	docker compose exec llm-advisor python scripts/generate_monthly_report.py
```

---

## 14. 환경 변수

```bash
# .env.example

# ─── 거래소 API ───
BYBIT_API_KEY=
BYBIT_API_SECRET=
BYBIT_TESTNET=true              # 실전 전환 시 false

# (향후 추가)
# BINANCE_API_KEY=
# BINANCE_API_SECRET=
# OKX_API_KEY=
# OKX_API_SECRET=
# OKX_PASSPHRASE=

# ─── 외부 데이터 ───
COINGLASS_API_KEY=               # 펀딩비 멀티거래소 비교

# ─── 인프라 ───
DB_PASSWORD=
REDIS_URL=redis://redis:6379

# ─── Telegram ───
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=                # 본인 채팅 ID

# ─── LLM ───
CLAUDE_CODE_PATH=/usr/local/bin/claude

# ─── 대시보드 ───
DASHBOARD_INTERNAL_PORT=3000
DASHBOARD_PUBLIC_PORT=3001
GRAFANA_ADMIN_PASSWORD=

# ─── 기타 ───
LOG_LEVEL=INFO
ENVIRONMENT=testnet              # testnet | production
```

---

## 15. 운영 체크리스트

### 일일 루틴

```
[ ] 06:00 — Telegram 일일 리포트 자동 수신 확인
[ ] 06:05 — 대시보드에서 전일 수익/손실 확인
[ ] 06:10 — 펀딩비 수취 내역 확인 (3회/일 × 수취금액)
[ ] 06:15 — Kill Switch 발동 이력 확인 (없어야 정상)
[ ] 수시  — Telegram 알림으로 비정상 상황 즉시 대응
```

### 주간 루틴

```
[ ] 전략별 주간 수익률 리뷰
[ ] Sharpe Ratio 30일 롤링 확인 (목표 ≥ 2.0)
[ ] Max Drawdown 확인 (목표 ≤ 3%)
[ ] Docker 컨테이너 업타임 확인
[ ] 디스크 사용량 + DB 사이즈 확인
[ ] Bybit API Rate Limit 소비량 확인
```

### 월간 루틴

```
[ ] LLM 자동 생성 월간 리포트 검토
[ ] 백테스트 재실행 (최근 데이터 포함)
[ ] Walk-Forward 분석으로 파라미터 재검증
[ ] 전략 파라미터 미세 조정 (필요 시)
[ ] 공개 퍼포먼스 페이지 수익 곡선 스크린샷 저장
[ ] Bybit 거래 내역 CSV 다운로드 → 대시보드 데이터와 교차 검증
```

---

## 16. 확장 계획 (Phase별)

```
Phase 1 (현재):
  거래소: Bybit만
  전략:   펀딩비 차익(단일 거래소) + 그리드 + DCA
  자본:   $500~$5,000

Phase 2 (3개월 후):
  거래소: Bybit + Binance
  전략:   + 거래소 간 펀딩비 차익
  자본:   $5,000~$20,000
  추가:   유튜브 시작, 공개 대시보드 론칭

Phase 3 (6개월 후):
  거래소: Bybit + Binance + OKX
  전략:   + 평균회귀, LLM 판단 강화
  자본:   $20,000+
  추가:   레퍼럴 본격화, 커뮤니티 개설

Phase 4 (12개월 후):
  거래소: 4~5개소 연동
  전략:   멀티 거래소 차익, 옵션 헤지
  추가:   SaaS화, 팀 빌딩
```

---

## 17. 핵심 리스크 대응 매트릭스

| 리스크 | 확률 | 영향 | 대응 | 코드 위치 |
|--------|------|------|------|----------|
| 거래소 API 다운 | 중 | 높음 | 오픈 포지션 보호 모드 전환, Telegram 즉시 알림 | `shared/kill_switch.py` |
| 펀딩비 방향 전환 | 중 | 중 | 24시간 연속 음수 시 자동 청산 | `strategies/funding-arb/strategy.py` |
| 급격한 가격 변동 (±10%) | 낮 | 높음 | 현금 비중 확대, 그리드 정지, 레버리지 자동 축소 | `orchestrator/core.py` |
| Docker 컨테이너 크래시 | 낮 | 높음 | restart: always + 헬스체크 + Telegram 알림 | `docker-compose.yml` |
| DB 장애 | 낮 | 높음 | Redis 캐시로 포지션 유지, DB 복구 후 동기화 | `shared/db/connection.py` |
| 네트워크 단절 | 낮 | 높음 | 30초 이상 끊김 시 Execution Engine이 신규 주문 차단 | `services/execution/safety.py` |
| WSL 크래시/PC 재시작 | 중 | 높음 | Docker 자동 재시작 + 재기동 시 포지션 동기화 | `services/execution/position_tracker.py` |
| Bybit 해킹/출금 정지 | 낮 | 치명적 | 자본 50% 이상 단일 거래소 금지 (Phase 2~) | `config/orchestrator.yaml` |

---

> **"적더라도 극도로 안정적으로 계속해서 수익을 발생"** — 이 시스템의 모든 설계 결정은 이 한 문장에 기반합니다.
> 화려한 수익률보다 매일 쌓이는 작은 수익과 투명한 공개가 만드는 신뢰가 진짜 자산입니다.
