# CryptoEngine

비트코인 선물 자동매매 시스템. Supertrend 4h 추세추종 단일 전략(combo #7908), BTCUSDT 롱 전용 3x, Bybit 메인넷 소액 실전 운영.

**한 개의 Python asyncio 프로세스(`bot`) + Postgres**로 돌아간다. 4h 봉이 확정되면 REST로 받아 신호를 계산하고, post-only 지정가로 진입·청산하며, 킬스위치·Telegram 알림/명령·읽기 전용 대시보드를 같은 프로세스가 담당한다.

## 빠른 시작

```bash
cd cryptoengine
cp .env.example .env        # BYBIT_API_KEY/SECRET, DB_PASSWORD, TELEGRAM_*, DASHBOARD_TOKEN 입력. BYBIT_TESTNET=false
make up                     # postgres + bot + pg-backup 빌드·기동
make logs                   # 봇 로그
make status                 # 컨테이너 + 봇 상태 (포지션, 에쿼티, 차단 여부)
make test                   # 호스트에서 단위/e2e 테스트
```

기동 후 Telegram에 "봇 시작" 메시지가 오고 `/status`로 상태를 볼 수 있다. 대시보드는 `http://localhost:3000/?t=<DASHBOARD_TOKEN>`.

배포(재시작)는 포지션을 청산하지 않는다. 긴급 청산은 Telegram `/emergency_close` 또는 `make emergency`.

## 구성

```
cryptoengine/
  bot/               봇 본체 (python -m bot.main) + Dockerfile, requirements.txt
  shared/            kill_switch.py(수정 금지), models/ 등
  config/bot.yaml    킬스위치 임계값, max_capital_usd
  docker-compose.yml postgres + bot + pg-backup
  scripts/           watchdog.sh(호스트 cron), sh/pg_backup*.sh
  tests/             bot/ 단위, e2e/, fakes/, fixtures/ (+ replay 백테스트)
backtest/            Jesse 백테스트 스택 (별도 compose, profile backtest)
docs/                architecture, operations, strategy, safety, adr/
```

## 문서

| 문서 | 내용 |
|---|---|
| [docs/architecture.md](docs/architecture.md) | 프로세스 구조, 스케줄러, 데이터 흐름, Postgres 테이블 |
| [docs/operations.md](docs/operations.md) | 배포, Telegram 명령, 긴급 청산, watchdog, 백업, 대시보드, 롤백 |
| [docs/strategy.md](docs/strategy.md) | 전략 사양, 정본 백테스트 수치 |
| [docs/safety.md](docs/safety.md) | 킬스위치, 주문 전 검사, 알려진 한계 |
| [docs/adr/](docs/adr/) | 아키텍처 결정 기록 |
| [AGENTS.md](AGENTS.md) | AI 에이전트 작업 가이드 (불변 규칙 포함) |
