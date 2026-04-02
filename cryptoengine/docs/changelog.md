# 변경 이력 (Changelog)

이 문서는 CryptoEngine의 주요 변경 사항을 기록합니다.

형식: [시맨틱 버전](https://semver.org/lang/ko/) 기준

---

## [1.0.0] - 2026-04-03

### 추가 (Added)

#### 핵심 시스템
- PostgreSQL 16 기반 데이터 저장소 (asyncpg 연결 풀)
- Redis 7 기반 이벤트 버스 및 캐시 레이어
- Docker Compose 기반 마이크로서비스 배포
- structlog 기반 구조화된 JSON 로깅 (correlation ID 포함)
- YAML 설정 로더 (환경변수 치환 지원)
- Alembic 데이터베이스 마이그레이션

#### 거래소 연동
- Bybit 선물 커넥터 (ccxt.pro 기반, REST + WebSocket)
- Binance 커넥터 (기본 구조)
- 거래소 팩토리 패턴 (`exchange_factory`)

#### 전략
- **펀딩 레이트 차익거래**: 델타 중립 포지션 관리, Basis Spread State Machine, 펀딩비 추적
- **그리드 트레이딩**: 기하급수/등차 그리드, 레짐 기반 활성화, 자동 리밸런싱
- **적응형 DCA**: Fear & Greed 멀티플라이어, RSI/이동평균/변동성 기반 적응, 이익 실현 래더

#### 오케스트레이터
- 시장 레짐 기반 가중치 매트릭스
- EMA 스무딩 가중치 전환
- 포트폴리오 모니터링
- ML 레짐 분류 모델 통합
- 비유사도 지수 (Dissimilarity Index)
- LLM 어드바이저 가중치 조정 통합

#### 실행 엔진
- Redis pub/sub 기반 주문 수신
- 안전성 검사 (Safety Guard)
- 동시 주문 제한 (세마포어)
- 재시도 로직 (지수 백오프, 최대 3회)
- 멱등성 보장 (request_id 중복 검사)

#### 리스크 관리
- 4단계 Kill Switch (전략/포트폴리오/시스템/수동)
- 쿨다운 및 자동 복구 메커니즘
- Sharpe/Sortino 비율 계산
- 최대 낙폭 추적
- 포지션 크기 계산기 (고정 분수법)
- 레버리지 검증

#### 시장 데이터
- 실시간 OHLCV 수집기
- 기술적 지표 계산 (ADX, ATR, BB, EMA)
- 시장 레짐 분류기 (ranging/trending/volatile)
- Feature Engine (ML 모델용)
- 펀딩레이트 모니터

#### LLM 어드바이저
- Claude Code CLI 기반 분석 엔진
- 다단계 분석 그래프
- 차트 비전 분석
- 일일 회고 (Daily Reflection)
- 신뢰도 기반 가중치 조정

#### 백테스트 시스템
- FreqtradeBridge (CLI + 내부 이벤트 엔진)
- Walk-Forward Analysis (슬라이딩 윈도우)
- Monte Carlo 시뮬레이션 (신뢰구간)
- HTML/Markdown 리포트 생성기

#### 운영 도구
- Telegram Bot (알림 + 수동 명령)
- Express.js Dashboard (내부/외부 API)
- Grafana 연동
- 데이터베이스 초기화 스크립트 (`init_db.py`)
- 히스토리 데이터 시드 스크립트 (`seed_historical.py`)
- 거래 내역 CSV 내보내기 (`export_trades.py`)
- 시스템 상태 점검 스크립트 (`health_check.py`)
- 월간 리포트 생성 스크립트 (`generate_monthly_report.py`)

#### 테스트
- pytest 기반 테스트 스위트
- 단위 테스트: 델타 중립, 그리드 계산, 레짐 감지, 리스크, Kill Switch, 주문 관리
- 통합 테스트: Bybit 테스트넷, 실행 파이프라인, 펀딩 차익 E2E, 오케스트레이터 레짐
- 백테스트: 펀딩 차익, 그리드 트레이딩, 복합 전략

#### 문서
- 시스템 아키텍처 문서
- 전략별 상세 문서 (펀딩 차익, 그리드 트레이딩, 적응형 DCA)
- 운영 매뉴얼 (Runbook)
- 내부 API 문서
- 변경 이력

---

## [미정] - 향후 계획

### 계획 (Planned)
- OKX 거래소 커넥터 추가
- 옵션 전략 (커버드 콜) 구현
- 웹 기반 대시보드 프론트엔드 (React)
- Prometheus + AlertManager 모니터링 통합
- CI/CD 파이프라인 (GitHub Actions)
- 실시간 성과 알림 고도화
- 다중 계정 지원
