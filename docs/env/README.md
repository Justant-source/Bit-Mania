---
title: 환경 설정 가이드
category: env
last_updated: 2026-06-14
---

# 환경 설정 가이드

CryptoEngine 시스템의 환경 변수, Docker 설정, 의존성 문서.

## 문서 목록

### 1. [환경 변수](./env-vars.md)
- BYBIT_API_KEY, BYBIT_TESTNET 설정
- 데이터베이스 자격증명
- Phase 5 메인넷 운영 환경 변수
- 초기화 및 검증 방법

### 2. [Docker 설정](./docker.md)
- docker-compose.yml 구조
- 자주 쓰는 명령 (빌드, 실행, 로그)
- 빌드 컨텍스트 규칙
- shared/ 수정 시 재빌드 순서

### 3. [Python 의존성](./dependencies.md)
- asyncpg, ccxt, structlog, aiohttp, redis, jesse
- 각 의존성 역할 및 버전 제약
- requirements.txt 구조

## 빠른 시작

### 1. .env 파일 생성
```bash
cp cryptoengine/.env.example cryptoengine/.env
# 편집: BYBIT_API_KEY, BYBIT_SECRET_KEY, DB_PASSWORD 입력
```

### 2. Docker 기동
```bash
cd cryptoengine
docker compose up -d

# 상태 확인
docker compose ps
```


### 3. 데이터베이스 초기화
- 마이그레이션 자동 실행 (컨테이너 시작 시)
- 확인: `docker compose exec postgres psql -U cryptoengine -d cryptoengine -c "\dt"`

---

**최종 수정**: 2026-06-14
