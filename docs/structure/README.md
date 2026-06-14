---
title: 코드 구조 가이드
category: structure
last_updated: 2026-06-14
---

# 코드 구조 가이드

CryptoEngine은 비트코인 선물 자동매매 시스템으로, 마이크로서비스 아키텍처와 Redis 기반 메시징을 통해 느슨하게 결합된 서비스 간 통신을 구현합니다.

## 아키텍처 원칙

### 1. 마이크로서비스 설계
- **독립 배포**: 각 서비스는 Docker 컨테이너로 독립적 배포
- **느슨한 결합**: Redis Pub/Sub을 통한 이벤트 기반 통신
- **높은 응집도**: 각 서비스는 단일 책임만 담당

### 2. 상태 관리
- **영속성 상태**: PostgreSQL에 저장 (거래, 포지션, 로그)
- **휘발성 상태**: Redis에 캐시 (전략 상태, TTL 1시간)
- **메모리 상태**: 서비스 메모리 (포지션 추적, 주문 큐)

### 3. 통신 패턴
- **Pub/Sub**: 서비스 간 비동기 메시징 (Redis 채널)
- **Request/Response**: 필요 시 HTTP/gRPC (현재 미사용)
- **이벤트 로그**: 모든 중요 이벤트는 DB 기록 (감시 & 디버깅)

### 4. 로깅 체계
- **구조화 로그**: structlog 기반 JSON 형식
- **95개 표준 이벤트**: log_events.py에 정의된 상수 사용
- **레벨 자동 매핑**: 이벤트별 권장 로그 레벨 (EVENT_LEVELS dict)
- **비차단 DB 저장**: LogWriter 큐 기반, 서비스 성능 영향 최소화

---

## 문서 목록

### 1. [서비스 아키텍처](./services.md) ⭐
**19개 마이크로서비스 상세**
- 각 서비스의 입력/출력/파라미터
- Redis Pub/Sub 채널 정의
- 리소스 제한 (CPU, 메모리)
- Docker 의존성 그래프
- 서비스 간 데이터 흐름

### 2. [공유 라이브러리](./shared-libraries.md) ⭐
**핵심 유틸리티 및 도메인 모델**
- 도메인 모델: OrderRequest, OrderResult, Position, PortfolioState 등
- Bybit CCXT 래퍼 (거래소 연결)
- Redis 클라이언트 (Pub/Sub, 캐시)
- 설정 로더 (환경 변수 치환)
- Kill Switch 4계층 (긴급 청산)
- 이벤트 로그 시스템 (95개 코드)
- 비동기 로그 라이터 (배치 DB 저장)
- structlog 로깅 설정
- 타임존 유틸리티 (UTC/KST)

### 3. [디렉토리 구조](./directory-layout.md)
**파일 트리 및 각 파일 설명**
- 프로젝트 루트 레이아웃
- config/, shared/, services/, migrations/ 상세
- 각 마이크로서비스 디렉토리
- DB 테이블 목록

---

## 핵심 개념

### Single Source of Truth
| 계층 | 역할 | 기술 | TTL |
|-----|------|------|-----|
| **Persistent** | 거래 기록, 로그, 리포트 | PostgreSQL | 무제한 |
| **Ephemeral** | 전략 상태, 메시지 | Redis | 1시간 (전략), 5분 (시장) |
| **Transient** | 현재 포지션, 주문 | 메모리 | 서비스 종료 시 |

### 느슨한 결합의 이점
```
전략 → (주문) → Redis → 실행 엔진 → Bybit → 체결
↓ (구독)      ↓ (발행)      ↓ (저장)
로그 레코더   오케스트레이터  DB

각 화살표는 독립적 → 서비스 장애 격리 가능
```

### 타입 안전성
Pydantic BaseModel 사용으로:
- 필드 검증 (값 범위, 타입)
- 자동 JSON 직렬화/역직렬화
- IDE 자동완성 + 정적 분석

---

## 빠른 참조

| 항목 | 경로 | 설명 |
|-----|------|------|
| **Docker 스택** | cryptoengine/docker-compose.yml | 핵심 서비스 정의 |
| **환경 설정** | cryptoengine/.env | API 키, DB 비밀번호 (git 제외) |
| **전략 파라미터** | cryptoengine/config/strategies/supertrend.yaml | Supertrend 4h 전략 |
| **핵심 라이브러리** | cryptoengine/shared/ | 모든 서비스 공용 유틸 |
| **핵심 전략** | cryptoengine/services/strategies/supertrend/ | Supertrend 4h Long-only 3x #7908 |
| **실행 엔진** | cryptoengine/services/execution/ | 주문 실행 + 포지션 추적 |
| **DB 마이그레이션** | cryptoengine/migrations/ | 스키마 버전 관리 |
| **운영 스크립트** | cryptoengine/scripts/ | Phase 5 전환, 백업 등 |

---

## 개발 워크플로우

### 새 기능 추가
1. **모델 정의** → shared/models/ 추가
2. **로그 이벤트** → shared/log_events.py 추가
3. **구현** → 해당 서비스 파일 수정
4. **통신** → Redis 채널 또는 DB 테이블
5. **테스트** → 로컬 docker-compose up 후 검증
6. **배포** → `docker compose up -d --build <service>`

### shared/ 수정 시 (모든 서비스 재빌드)
```bash
docker compose build market-data execution-engine supertrend strategy-orchestrator telegram-bot
docker compose up -d --no-deps market-data execution-engine supertrend strategy-orchestrator telegram-bot
```

### 문제 디버깅
```bash
# 로그 확인
docker compose logs -f <service>

# DB 쿼리
docker compose exec postgres psql -U cryptoengine -d cryptoengine

# Redis 모니터
docker compose exec redis redis-cli SUBSCRIBE "market:*"
```

---

**최종 수정**: 2026-06-14
