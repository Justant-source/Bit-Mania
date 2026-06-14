---
title: 아키텍처 문서
category: architecture
last_updated: 2026-06-14
when_to_update: |
  - 시스템 구조 변경 시 (서비스 추가/제거)
  - 데이터 흐름 변경 시 (채널, 이벤트 순서)
  - 배포 인프라 변경 시 (Docker, 포트, 환경변수)
  - 스키마 마이그레이션 시 (테이블 추가/수정)
  - 리스크 관리 정책 변경 시 (Kill Switch 조건)
---

# 아키텍처

비트코인 선물 자동매매 시스템의 6계층 Loosely-coupled Microservices 아키텍처.

## 아키텍처 철학

- **느슨한 결합 (Loosely Coupled)**: Redis Pub/Sub을 통한 비동기 메시징으로 서비스 간 직접 의존성 최소화
- **Source of Truth**: PostgreSQL을 중앙 데이터 저장소, Redis를 실시간 상태 캐시로 사용
- **Fail-Safe 설계**: 모든 주문 실행은 안전 검증 7단계 통과 후 실행, Kill Switch 4단계 계층으로 보호
- **관찰 가능성 (Observability)**: structlog 구조화 로깅 + Prometheus 메트릭 + Grafana 시각화

## 6계층 아키텍처

| 계층 | 역할 | 주요 서비스 | 기술 스택 |
|------|------|-----------|----------|
| **1. 인프라 (Infrastructure)** | 데이터 영구 저장, 메시징 브로커, 모니터링 | postgres, redis, prometheus, grafana | PostgreSQL 16, Redis 7, Prometheus, Grafana |
| **2. 시장 데이터 (Market Data)** | WebSocket 실시간 수집 | market-data | CCXT, Pandas, TA-Lib, asyncio |
| **3. 전략 조율 (Orchestration)** | 자본 배분, Kill Switch | strategy-orchestrator | structlog, asyncio, Redis Pub/Sub |
| **4. 전략 구현 (Strategy)** | 주문 신호 생성 | supertrend | Custom Strategy Framework |
| **5. 실행 엔진 (Execution)** | 주문 검증, 거래소 전송, 포지션 추적 | execution-engine | CCXT Async, asyncpg |
| **6. 인터페이스 (Interfaces)** | 웹 UI, 알림, 대시보드 | dashboard, telegram-bot, grafana | FastAPI, Telegram Bot API |

## 문서 구조

| 문서 | 설명 | 읽는 사람 |
|------|------|---------|
| **[system-overview.md](system-overview.md)** | 6계층 구조, 서비스 책임, 공유 라이브러리, 디자인 패턴 | 아키텍트, 신입 온보딩 |
| **[data-flow.md](data-flow.md)** | 시장→전략→주문의 전체 데이터 흐름, Redis Pub/Sub 채널 명세, 주문 실행 시퀀스 | 백엔드 엔지니어, 전략 개발자 |
| **[database-schema.md](database-schema.md)** | 22개 테이블 완전 명세, 컬럼 타입, 인덱스, 보존 정책, SQL DDL | DBA, 데이터 분석가 |
| **[deployment.md](deployment.md)** | Docker Compose 19서비스, 빌드 컨텍스트, 환경 설정, 포트/볼륨/리소스 제한 | DevOps, 배포 담당자 |
| **[risk-management.md](risk-management.md)** | Kill Switch 4단계, 안전 검증 7단계, 레버리지 제한 | 리스크 담당자, 운영팀 |

## Mermaid 다이어그램 위치

- 모든 아키텍처 다이어그램은 `.md` 파일 내에 **인라인 Mermaid 코드블록**으로 작성됨
- 별도의 `.puml` 파일 없음 (Git에서 변경 추적 용이, 문서와 다이어그램 동기화 유지)
- `graph`, `sequenceDiagram`, `erDiagram` 등 다양한 형식 사용

## 주요 설계 결정사항

### 1. Redis Pub/Sub vs Message Queue
- **선택**: Redis Pub/Sub (at-most-once semantics)
- **이유**: 실시간 필요, 메시지 손실 허용 (trade execution은 별도 DB 트랜잭션으로 보호)
- **대안**: RabbitMQ (도입 미보류 — 복잡도 vs 이득 트레이드오프)

### 2. PostgreSQL asyncpg 풀 사이징
- **설정**: min=2, max=10, command_timeout=30s
- **근거**: 동시 서비스 수(6개) × 2~3 연결 = 12~18 필요, 10개로 충분
- **모니터링**: Grafana에서 풀 포화도 추적

### 3. Kill Switch 4단계 계층
- **레벨별**: 전략(strategy) → 포트폴리오(portfolio) → 시스템(system) → 수동(manual)
- **의도**: 급락이 심할수록 더 빠른 중단, 시스템 장애는 즉시 중단
- **구현**: orchestrator가 5분마다 체크, dead man's switch로 service_health 모니터링

