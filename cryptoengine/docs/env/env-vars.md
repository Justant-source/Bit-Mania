---
title: 환경 변수 설정
category: env
related_code:
  - cryptoengine/.env
  - cryptoengine/.env.example
last_updated: 2026-05-01
---

# 환경 변수 설정

CryptoEngine의 모든 환경 변수 목록 및 설정 가이드.

## 파일 위치

**로컬 설정 (git 제외)**:
```bash
cryptoengine/.env
```

**템플릿 (git 추적)**:
```bash
cryptoengine/.env.example
```

---

## Bybit 거래소 설정 (필수)

**상태**: Phase 4 (테스트넷) 또는 Phase 5 (메인넷)

### BYBIT_API_KEY
```bash
BYBIT_API_KEY=<your_api_key>
```
- **필수**: 예
- **설명**: Bybit API 공개 키
- **권한**: 읽기, 거래 (출금 권한 없음)
- **환경**: 테스트넷 + 메인넷 API 키 모두 필요
- **보안**: .env 파일에만 저장, git 제외

### BYBIT_SECRET_KEY
```bash
BYBIT_SECRET_KEY=<your_secret_key>
```
- **필수**: 예
- **설명**: Bybit API 비공개 키
- **권한**: 읽기, 거래 (출금 권한 없음)
- **보안**: .env 파일에만 저장, git 제외

### BYBIT_TESTNET
```bash
BYBIT_TESTNET=true
```
- **필수**: 예
- **기본값**: `true` (Phase 4)
- **값**: `true` (테스트넷) | `false` (메인넷)
- **주의**: **Phase 5 메인넷 진입까지 절대 false로 변경하지 않음**
- **전환**: `scripts/switch_to_mainnet.py` 실행 후 변경 (이중 확인 포함)

**Phase별 설정**:
| Phase | BYBIT_TESTNET | 설명 |
|-------|---------------|------|
| 0-4 | true | 테스트넷 (현재) |
| 5 | false | 메인넷 전환 |

---

## 데이터베이스 설정

### DB_HOST
```bash
DB_HOST=postgres
```
- **필수**: 예
- **기본값**: `postgres` (Docker 컨테이너명)
- **로컬 개발**: `localhost`

### DB_PORT
```bash
DB_PORT=5432
```
- **필수**: 예
- **기본값**: `5432` (PostgreSQL 기본 포트)

### DB_USER
```bash
DB_USER=cryptoengine
```
- **필수**: 예
- **기본값**: `cryptoengine`

### DB_PASSWORD
```bash
DB_PASSWORD=***REMOVED***
```
- **필수**: 예
- **설명**: PostgreSQL 암호 (변경 권장)
- **보안**: .env 파일에만 저장, git 제외

### DB_NAME
```bash
DB_NAME=cryptoengine
```
- **필수**: 예
- **기본값**: `cryptoengine`

**연결 문자열 (asyncpg)**:
```python
postgresql://cryptoengine:***REMOVED***@postgres:5432/cryptoengine
```

---

## Grafana 설정

### GRAFANA_PASSWORD
```bash
GRAFANA_PASSWORD=***REMOVED***
```
- **필수**: 예
- **기본값**: `***REMOVED***`
- **로그인**: admin / ***REMOVED***
- **URL**: http://localhost:3002
- **보안**: 프로덕션 환경에서 변경 필수

---

## Redis 설정

### REDIS_HOST
```bash
REDIS_HOST=redis
```
- **기본값**: `redis` (Docker 컨테이너명)

### REDIS_PORT
```bash
REDIS_PORT=6379
```
- **기본값**: `6379` (Redis 기본 포트)

---

## Telegram 알림 설정 (선택)

### TELEGRAM_BOT_TOKEN
```bash
TELEGRAM_BOT_TOKEN=<your_telegram_bot_token>
```
- **필수**: 아니오 (선택)
- **설명**: Telegram Bot API 토큰
- **획득**: BotFather (`@BotFather`) 통해 생성

### TELEGRAM_CHAT_ID
```bash
TELEGRAM_CHAT_ID=<your_chat_id>
```
- **필수**: 아니오 (선택)
- **설명**: 알림 수신 Chat ID
- **획득**: 봇 메시지 전송 후 `/getMe` 명령으로 확인

---

## Phase 5 메인넷 진입 환경 변수

### EXPECTED_INITIAL_BALANCE_USD
```bash
EXPECTED_INITIAL_BALANCE_USD=200
```
- **Phase**: 5 (메인넷)
- **설명**: 초기 예상 잔고 (USD)
- **목적**: 메인넷 진입 시 잔고 검증 (오류 방지)
- **검증**: 실제 잔고가 ±10% 범위에 있는지 확인
- **설정**: 메인넷 진입 직전 명시적 입력 필수

### STRICT_MONITORING_HOURS
```bash
STRICT_MONITORING_HOURS=24
```
- **Phase**: 5 (메인넷)
- **설명**: 강화 모니터링 시간 (첫 24시간)
- **목적**: 메인넷 첫 24시간 동안 더 엄격한 Kill Switch 임계값 적용
- **임계값**:
  - 일일 손실: -$10 (절대값, 상대값 아님)
  - 최대 낙폭: -$20 (절대값)
- **설정**: `PHASE5_MODE=true`와 함께 사용

### PHASE5_MODE
```bash
PHASE5_MODE=true
```
- **Phase**: 5 (메인넷)
- **설명**: Phase 5 특수 모드 활성화
- **기능**:
  - fixed_notional 포지션 사이징 (백테스트 기준)
  - 절대값 기반 Kill Switch (상대값 대신)
  - 강화 모니터링 (처음 24시간)
  - 사이즈 증분 제한 (하루 최대 +10%)
- **주의**: 메인넷 전환 후에만 `true`로 설정

---

## 로깅 & 모니터링 설정

### LOG_LEVEL
```bash
LOG_LEVEL=INFO
```
- **기본값**: INFO
- **값**: DEBUG | INFO | WARNING | ERROR | CRITICAL
- **설명**: 최소 로그 레벨 (DEBUG는 매우 verbose)
- **docker-compose**: `environment` → `<<: *common-env`

### LOG_DB_MIN_LEVEL
```bash
LOG_DB_MIN_LEVEL=20
```
- **기본값**: 20 (INFO)
- **값**: 10 (DEBUG) | 20 (INFO) | 30 (WARNING) | 40 (ERROR) | 50 (CRITICAL)
- **설명**: PostgreSQL service_logs 테이블에 저장할 최소 레벨
- **용도**: 로그 저장소 크기 제어

### ENVIRONMENT
```bash
ENVIRONMENT=testnet
```
- **기본값**: testnet
- **값**: testnet | mainnet | development
- **설명**: 배포 환경 (로그에 포함됨)

---

## 기능 플래그 & 공급자 API

### COINGLASS_API_KEY
```bash
COINGLASS_API_KEY=<your_api_key>
```
- **필수**: 아니오 (선택)
- **설명**: Coinglass Fear & Greed 지수 API
- **용도**: adaptive-dca 전략의 공포/탐욕 지수 조회
- **획득**: https://www.coinglass.com/api

---

## x-common-env 앵커 (docker-compose.yml)

docker-compose.yml에서 모든 서비스는 공통 환경 변수를 상속받습니다:

```yaml
x-common-env: &common-env
  DB_HOST: postgres
  DB_PORT: 5432
  DB_NAME: cryptoengine
  DB_USER: cryptoengine
  DB_PASSWORD: ${DB_PASSWORD}                    # .env에서 읽음
  REDIS_URL: ${REDIS_URL:-redis://...}         # .env 또는 기본값
  LOG_LEVEL: ${LOG_LEVEL:-INFO}
  ENVIRONMENT: ${ENVIRONMENT:-testnet}

services:
  market-data:
    environment:
      <<: *common-env                           # 위의 변수들 모두 상속
      BYBIT_API_KEY: ${BYBIT_API_KEY}          # 추가 변수
  
  execution-engine:
    environment:
      <<: *common-env                           # 동일 상속
      BYBIT_API_SECRET: ${BYBIT_API_SECRET}
      BYBIT_TESTNET: ${BYBIT_TESTNET:-true}
```

**이점**: 중복 제거, 일관성 유지, 환경 변수 중앙 관리

---

## 설정 예시

### Phase 4 (테스트넷, 현재)

```bash
# Bybit
BYBIT_API_KEY=testnet_api_key_xxxxx
BYBIT_SECRET_KEY=testnet_secret_key_xxxxx
BYBIT_TESTNET=true

# Database
DB_HOST=postgres
DB_PORT=5432
DB_USER=cryptoengine
DB_PASSWORD=***REMOVED***
DB_NAME=cryptoengine

# Grafana
GRAFANA_PASSWORD=***REMOVED***

# Redis
REDIS_HOST=redis
REDIS_PORT=6379

# Telegram (선택)
TELEGRAM_BOT_TOKEN=123456789:ABCDefGHIJKlmnopQRStUVwxyz
TELEGRAM_CHAT_ID=987654321

# Phase 4 설정 (미사용)
# EXPECTED_INITIAL_BALANCE_USD=
# STRICT_MONITORING_HOURS=
# PHASE5_MODE=false
```

### Phase 5 (메인넷, 진입 예정)

```bash
# Bybit (메인넷)
BYBIT_API_KEY=mainnet_api_key_xxxxx
BYBIT_SECRET_KEY=mainnet_secret_key_xxxxx
BYBIT_TESTNET=false

# Database (동일)
DB_HOST=postgres
DB_PORT=5432
DB_USER=cryptoengine
DB_PASSWORD=***REMOVED***
DB_NAME=cryptoengine

# Grafana
GRAFANA_PASSWORD=***REMOVED***

# Redis
REDIS_HOST=redis
REDIS_PORT=6379

# Telegram (권장)
TELEGRAM_BOT_TOKEN=123456789:ABCDefGHIJKlmnopQRStUVwxyz
TELEGRAM_CHAT_ID=987654321

# Phase 5 강화 설정
EXPECTED_INITIAL_BALANCE_USD=200
STRICT_MONITORING_HOURS=24
PHASE5_MODE=true
```

#### Phase 4 vs Phase 5 전환 흐름

```mermaid
graph TD
    subgraph phase4["Phase 4: 테스트넷 (현재)"]
        ENV4A["BYBIT_TESTNET=true"]
        ENV4B["BYBIT_API_KEY=testnet_xxx"]
        ENV4C["BYBIT_SECRET_KEY=testnet_xxx"]
        ENV4D["로그 레벨: DEBUG/INFO"]
        ENV4E["상대값 Kill Switch<br>-5% daily, -10% weekly"]
    end

    subgraph preflight["Phase 5 전환 전"]
        CHK["scripts/phase5_preflight.py<br>8개 항목 검증"]
        CHK1["✓ Bybit 메인넷 키 테스트"]
        CHK2["✓ 초기 잔고 검증 ±10%"]
        CHK3["✓ 레버리지 5배 이하"]
        CHK4["✓ DB/Redis 연결"]
        PASS["모든 항목 PASS"]
    end

    subgraph switch["Phase 5 전환"]
        MAIN["scripts/switch_to_mainnet.py<br>9단계 + 이중 확인"]
        MAIN1["1-2. 현재 상태 백업"]
        MAIN3["3-5. 메인넷 키 설정"]
        MAIN6["6. 환경변수 업데이트"]
        MAIN9["9. 최종 확인"]
    end

    subgraph phase5["Phase 5: 메인넷 (진입 후)"]
        ENV5A["BYBIT_TESTNET=false"]
        ENV5B["BYBIT_API_KEY=mainnet_xxx"]
        ENV5C["BYBIT_SECRET_KEY=mainnet_xxx"]
        ENV5D["EXPECTED_INITIAL_BALANCE_USD=200"]
        ENV5E["STRICT_MONITORING_HOURS=24"]
        ENV5F["PHASE5_MODE=true"]
        ENV5G["절대값 Kill Switch<br>-$10 daily, -$20 weekly"]
    end

    phase4 --> CHK
    CHK --> CHK1
    CHK --> CHK2
    CHK --> CHK3
    CHK --> CHK4
    CHK1 --> PASS
    CHK2 --> PASS
    CHK3 --> PASS
    CHK4 --> PASS
    PASS --> MAIN
    MAIN --> MAIN1
    MAIN --> MAIN3
    MAIN --> MAIN6
    MAIN --> MAIN9
    MAIN9 --> phase5

    style phase4 fill:#c8e6c9,color:#1b5e20
    style PASS fill:#a5d6a7,color:#1b5e20
    style MAIN fill:#fff9c4,color:#f57f17
    style phase5 fill:#ffccbc,color:#bf360c
```

---

## 환경 변수 검증

### 1. .env 파일 존재 확인
```bash
ls -la cryptoengine/.env
```

### 2. 필수 변수 확인
```bash
docker compose exec execution-engine python -c "
import os
required = ['BYBIT_API_KEY', 'DB_PASSWORD', 'BYBIT_TESTNET']
for var in required:
    if not os.getenv(var):
        print(f'ERROR: {var} not set')
    else:
        print(f'OK: {var}={os.getenv(var)[:10]}...')
"
```

### 3. Bybit 연결 테스트
```bash
docker compose exec execution-engine python -c "
import asyncio
from shared.exchange.bybit import BybitExchange
from dotenv import load_dotenv

load_dotenv()

async def test():
    exchange = BybitExchange()
    rate = await exchange.fetch_funding_rate('BTCUSDT')
    print(f'Funding rate: {rate}')

asyncio.run(test())
"
```

### 4. 데이터베이스 연결 테스트
```bash
docker compose exec postgres psql -U cryptoengine -d cryptoengine \
  -c "SELECT version();"
```

### 5. Grafana 접속 테스트
```bash
curl -u admin:***REMOVED*** \
  http://localhost:3002/api/datasources
```

---

## 보안 주의사항

### .env 파일 보호
```bash
# 읽기 권한만 설정
chmod 600 cryptoengine/.env

# git에서 제외 (이미 .gitignore에 등록됨)
grep .env cryptoengine/.gitignore
```

### API 키 관리
- **절대 git에 커밋하지 않음**
- **로그 출력 금지** — 민감한 정보 (처음 10자) 마스킹
- **정기적 로테이션** — 3개월마다 갱신 권장
- **권한 최소화** — 출금 권한 없음 (의도적)

### 암호 변경
```bash
# DB 암호 변경 (메인넷 전환 시)
docker compose exec postgres psql -U postgres \
  -c "ALTER USER cryptoengine WITH PASSWORD 'NewPassword2026!';"
```

### 환경 변수 로깅
- 로그에서 민감한 정보 자동 마스킹:
  ```json
  {
    "BYBIT_API_KEY": "***key_xxxxx",
    "DB_PASSWORD": "***word_!"
  }
  ```

---

## 문제 해결

### "BYBIT_API_KEY not set" 오류
```bash
# 1. .env 파일 존재 확인
ls cryptoengine/.env

# 2. 파일 내용 확인 (민감한 정보 포함)
head -5 cryptoengine/.env | grep BYBIT_API_KEY

# 3. Docker 컨테이너 재빌드
docker compose up -d --build
```

### "Can't connect to database" 오류
```bash
# 1. PostgreSQL 상태 확인
docker compose ps postgres

# 2. 연결 문자열 검증
# 기본: postgresql://cryptoengine:***REMOVED***@postgres:5432/cryptoengine

# 3. 암호 확인
echo $DB_PASSWORD

# 4. PostgreSQL 로그 확인
docker compose logs postgres | tail -20
```

### "Redis connection timeout" 오류
```bash
# 1. Redis 상태 확인
docker compose ps redis

# 2. Redis 핑 테스트
docker compose exec redis redis-cli ping
# 예상 응답: PONG

# 3. Redis 로그 확인
docker compose logs redis | tail -20
```

### Grafana 로그인 실패
```bash
# 1. 기본 자격증명 확인
# 기본값: admin / ***REMOVED***

# 2. Grafana 컨테이너 재시작
docker compose restart grafana

# 3. Grafana 로그 확인
docker compose logs grafana | tail -20
```

---

**최종 수정**: 2026-05-01
