---
title: Docker 배포 절차
category: policies/operations
related_code:
  - cryptoengine/docker-compose.yml
  - cryptoengine/Dockerfile.* 
last_updated: 2026-05-01
when_to_update: |
  - Docker 빌드 컨텍스트 변경 시
  - 서비스 의존성 변경 시
  - 배포 순서 변경 시
---

# Docker 배포 절차

---

## 빌드 컨텍스트 규칙

모든 서비스의 build context는 **프로젝트 루트(`.`)**로 설정되어 있다.

Dockerfile 내 COPY 경로는 반드시 프로젝트 루트 기준으로 작성해야 한다.

### ✅ 올바른 예

```dockerfile
# funding-arb Dockerfile
FROM python:3.11

COPY cryptoengine/shared /app/shared
COPY cryptoengine/services/strategies/base_strategy.py /app/
COPY cryptoengine/services/strategies/funding-arb /app/strategy
COPY cryptoengine/config /app/config

WORKDIR /app
ENV PYTHONPATH=/app
CMD ["python", "-m", "strategy.main"]
```

### ❌ 잘못된 예

```dockerfile
# 빌드 실패 (context 외부 참조)
COPY ../../shared /app/shared      # ← 상대경로 사용 금지
COPY ../../../config /app/config   # ← 상대경로 사용 금지
```

---

## 단일 서비스 배포

포지션을 유지하면서 단일 서비스를 재빌드/재시작한다.

```bash
# 1. 코드 수정
cd ~/Data/Bit-Mania/cryptoengine
vi services/strategies/funding-arb/strategy.py

# 2. 이미지 재빌드 (깨끗한 빌드)
docker compose build --no-cache funding-arb

# 3. 서비스 재시작 (포지션 자동 복구)
docker compose up -d --no-deps funding-arb

# 4. 로그 확인 (1-2분 대기, 포지션 복구 시간)
sleep 30
docker compose logs --tail=50 funding-arb | grep -E "position|restored|recovered"

# 5. 정상 운영 확인
docker compose logs -f funding-arb | head -20
```

---

## 공유 라이브러리 (shared/) 변경

`shared/` 변경은 모든 서비스에 영향을 준다. 순차 재시작 필수.

### 영향 받는 서비스

```
shared/ 변경
├─ market-data       (데이터 수집, logging)
├─ execution-engine  (주문 실행, logging)
├─ funding-arb       (전략, logging)
├─ strategy-orchestrator (오케스트레이션, logging)
└─ telegram-bot      (메시징, logging)
```

### 배포 순서 (의존성)

```
1. market-data           ← 데이터 공급자 (먼저 시작)
        ↓
2. execution-engine      ← 주문 엔진
        ↓
3. strategy-orchestrator ← 오케스트레이터
        ↓
4. funding-arb           ← 전략 (포지션 복구)
        ↓
5. telegram-bot          ← 알림
```

### 안전한 배포 절차

```bash
# 1단계: 모든 서비스 이미지 재빌드
docker compose build --no-cache \
  market-data \
  execution-engine \
  funding-arb \
  strategy-orchestrator \
  telegram-bot

# 2단계: market-data 시작 (모든 다른 서비스가 데이터 필요)
docker compose up -d --no-deps market-data
sleep 15  # 데이터 수집 안정화

# 3단계: execution-engine 시작
docker compose up -d --no-deps execution-engine
sleep 10

# 4단계: strategy-orchestrator 시작
docker compose up -d --no-deps strategy-orchestrator
sleep 10

# 5단계: funding-arb 시작 (포지션 복구)
docker compose up -d --no-deps funding-arb
sleep 60  # 포지션 복구 시간 충분히 확보

# 6단계: telegram-bot 시작
docker compose up -d --no-deps telegram-bot

# 7단계: 안정화 대기
sleep 60

# 8단계: 모든 서비스 상태 확인
docker compose ps

# 9단계: 로그 확인 (에러 확인)
docker compose logs --tail=20 market-data | grep -E "ERROR|ready"
docker compose logs --tail=20 execution-engine | grep -E "ERROR|ready"
docker compose logs --tail=20 funding-arb | grep -E "ERROR|recovered"
docker compose logs --tail=20 strategy-orchestrator | grep -E "ERROR|ready"
```

---

## 인프라만 재시작

데이터베이스나 캐시 문제가 있을 때.

```bash
# 모든 서비스 정지
docker compose down

# PostgreSQL + Redis + Grafana 시작 (앱 서비스 제외)
docker compose up -d postgres redis grafana

# 헬스 체크 (3-5분 대기)
sleep 10
docker compose exec postgres pg_isready
docker compose exec redis redis-cli ping

# 이제 서비스 시작 가능
docker compose up -d market-data execution-engine strategy-orchestrator funding-arb telegram-bot
```

---

## 전체 스택 재시작 (비상 상황)

모든 컨테이너를 재시작한다 (포지션 자동 복구).

```bash
# 1. 현재 상태 확인
docker compose ps

# 2. 그레이스풀 종료 (포지션 Redis 저장)
docker compose down

# 3. 전체 시스템 시작
docker compose up -d

# 4. 안정화 대기 (3-5분)
sleep 120

# 5. 상태 확인
docker compose ps
docker compose logs --tail=30 funding-arb | grep -E "position|recovered"
```

---

## 긴급 정지

포지션을 **즉시 청산**한다 (Kill Switch 발동).

```bash
# Make 명령어 사용
make emergency

# 또는 직접 명령
docker compose kill funding-arb execution-engine strategy-orchestrator

# 포지션 상태 확인
docker compose exec postgres psql -U cryptoengine -d cryptoengine -c \
  "SELECT id, status, exit_reason FROM positions WHERE status='open' LIMIT 5;"
```

---

## 배포 중 문제 해결

### 서비스가 시작되지 않음

```bash
# 로그 확인
docker compose logs <서비스명> --tail=100

# 의존성 서비스 확인
docker compose logs postgres   # DB OK?
docker compose logs redis      # 캐시 OK?

# 환경변수 확인
docker compose config | grep <서비스명> -A 10

# 이미지 재빌드
docker compose build --no-cache <서비스명>
docker compose up -d <서비스명>
```

### 포지션 복구 실패

```bash
# Redis에서 저장된 상태 확인
docker compose exec redis redis-cli GET strategy:saved_state:funding_arb | jq .

# TTL 확인 (1시간 이내인지)
docker compose exec redis redis-cli TTL strategy:saved_state:funding_arb

# Bybit에서 포지션 확인
# → 포지션 남아있으면 수동 정리 필요

# 강제 상태 초기화
docker compose exec redis redis-cli DEL strategy:saved_state:funding_arb
```

---

## 배포 체크리스트

```markdown
배포 전:
- [ ] 로컬 테스트 완료
- [ ] docker compose ps (현재 상태 확인)
- [ ] 포지션 확인 (필요 시)
- [ ] shared/ 변경 여부 확인

배포 중:
- [ ] docker compose build --no-cache <service>
- [ ] 순서대로 서비스 재시작
- [ ] 각 단계 후 안정화 대기 (10-60초)

배포 후:
- [ ] docker compose ps (모두 Running?)
- [ ] docker compose logs <service> (에러 있나?)
- [ ] 포지션 복구 메시지 확인
- [ ] 텔레그램 알림 수신 확인
- [ ] 거래 정상 진행 확인
```

---

## 자주 쓰는 명령어

```bash
# 모든 서비스 상태
docker compose ps

# 특정 서비스 로그 (최근 50줄)
docker compose logs --tail=50 funding-arb

# 실시간 로그 (Ctrl+C로 종료)
docker compose logs -f strategy-orchestrator

# 서비스 재시작
docker compose restart funding-arb

# 서비스 정지
docker compose stop funding-arb

# 서비스 시작
docker compose start funding-arb

# 이미지 재빌드 (깨끗하게)
docker compose build --no-cache funding-arb

# 컨테이너 삭제 후 재시작
docker compose up -d --force-recreate funding-arb

# 전체 시스템 정지
docker compose down

# 전체 시스템 시작
docker compose up -d
```

---

## 관련 문서

- [../deployment-position.md](../deployment-position.md) — 배포 시 포지션 보호 (Redis TTL)
- [runbook.md](runbook.md) — 운영 매뉴얼
