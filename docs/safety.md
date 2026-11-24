# 안전 정책

생존 우선: 수익보다 포지션 보호. 구현은 `cryptoengine/shared/kill_switch.py`(수정 금지), `bot/risk.py`, `bot/safety.py`, `bot/execution.py`, `bot/state.py`. 임계값은 `cryptoengine/config/bot.yaml`.

## 불변 규칙

1. `BYBIT_TESTNET=false` — 메인넷 실전. `bot/main.py`는 이 값이 정확히 `false`가 아니면 기동을 거부한다. 테스트넷 전환 시 포지션을 수동 청산한 뒤 진행.
2. `shared/kill_switch.py` 로직 수정 금지.
3. 레버리지 3x 하드캡 (`SAFETY_LEVERAGE_LIMIT=3.0`; 더 큰 값을 넣어도 `SafetyGuard`가 3.0으로 잘라낸다).
4. BTC 단일 심볼 (`BTC/USDT:USDT`), Bybit 단일.
5. 지정가(post-only) 우선. 시장가는 re-peg 소진 후 잔량 폴백, 킬스위치 청산, 긴급 청산에만.
6. 전액 95% × 3x 고정 배분.
7. 재시작·배포는 포지션을 청산하지 않는다.

## 킬스위치 (`risk.py` + `shared/kill_switch.py`)

`RiskManager.evaluate`가 housekeeping 때(15분마다) 호출된다.

### 입력

- 에쿼티 = Bybit **지갑 잔고**(미실현 손익 제외 — `exchange.py::get_balance` 주석 기준; 실제 거래소 동작은 이 환경에서 검증하지 못함).
- 기간 고점: 일(UTC 자정), 주(UTC 월요일), 월(1일)에 현재 에쿼티로 시작해 올라가는 방향으로만 갱신. `bot_state['risk_peaks']`에 저장. 저장이 없으면 최근 35일 `portfolio_snapshots`로 재구성(마지막 재설정 이후만).
- 낙폭 = (고점 − 현재) / 고점. `KillSwitch.check`에는 음수 분수와 `equity_at_open = 현재 에쿼티`, `system_healthy=True`가 전달된다.

### 임계값 (`config/bot.yaml`)

| 주기 | 낙폭 % | 절대 손실 USD | 조건 |
|---|---|---|---|
| 일 | 5.0 | 10 | AND |
| 주 | 10.0 | 20 | AND |
| 월 | 15.0 | 30 | AND |

쿨다운 60분. 절대 손실 = `낙폭 × 현재 에쿼티` (kill_switch.py의 계산). 퍼센트와 절대값을 **모두** 넘어야 발동한다(소액 계좌의 노이즈 방지).

### 레벨

`KillLevel`: 0 NONE, 1 STRATEGY, 2 PORTFOLIO, 3 SYSTEM, 4 MANUAL. 새 봇에서 실제로 쓰이는 것은 **PORTFOLIO(2)와 MANUAL(4)뿐**이다 — `check_strategy`는 호출되지 않고 `system_healthy`는 항상 `True`라서 STRATEGY·SYSTEM은 도달하지 않는다.

### PORTFOLIO 발동 시 동작

| 상황 | 동작 |
|---|---|
| **포지션 없음 (flat)** | **알림만.** 진입은 계속 허용 (2026-11-07 결정). `alert_only=true`로 저장 |
| **포지션 있음** | `bot_state['portfolio_block_until']` = 발동시각 + 쿨다운 기록 → reduce-only 시장가로 청산(`executor.close_position("kill_switch")`) → 쿨다운 동안 신규 진입 차단 |
| 알림만 상태에서 포지션이 생김 | 다음 평가에서 "승격": 청산 + 차단 |
| **쿨다운 종료** | 일/주/월 고점을 **현재 에쿼티로 재설정**(같은 과거 손실이 재발동하지 않게), 차단·킬 상태 삭제, 진입 허용. 재설정 시각은 `peak_rebase_at`에 저장 |

상태(`kill_switch`, `portfolio_block_until`, `risk_peaks`)는 `bot_state`에 저장되어 재시작 후 복원된다. 차단 기한이 미래이면 계속 차단한다. 쿨다운 만료 후의 정리(고점 재설정 등)는 housekeeping 주기(15분)에 일어나지만, 차단 자체는 기한이 지나는 즉시 풀린다(`BotState.entries_blocked`가 시각을 직접 비교).

### 수동 정지 (MANUAL)

- `BotControl.emergency_close`(Telegram `/emergency_close` 또는 CLI): `bot_state['manual_halt']` 설정(진입 차단) → `KillSwitch.trigger_manual`(Telegram 경로) → `executor.close_position` 최대 2회 시도 → 거래소 flat 확인 → 알림.
- 정지는 `/resume`(또는 CLI `resume`)까지 유지된다. 자동 해제 없음. 재시작 후에도 `manual_halt`가 남아 있으면 계속 차단한다.

### 차단 게이트와 reduce-only

`SafetyGuard.check_order`는 `EntryGate.entries_blocked()`를 **reduce-only가 아닌 주문에만** 적용한다. reduce-only 주문(청산)은 정지·쿨다운 중에도, 가격을 못 읽어도, 속도 제한에 걸려도 통과한다 — 청산이 자기 자신의 게이트에 막히지 않는다(2026-11 사고 원인이었다).

## 주문 전 검사 (`safety.py`)

| 검사 | 규칙 | reduce-only |
|---|---|---|
| 진입 게이트 | 수동 정지 또는 포트폴리오 차단 중이면 거부, 게이트 오류도 거부(fail-closed) | 면제 |
| 시세 | `get_ticker`(5초 캐시). 못 읽으면 진입 거부 | 통과 |
| 최대 주문 크기 | 명목가 > 100,000 USD 거부 | 가격 있으면 적용 |
| 레버리지 | (기존 포지션 명목가 + 주문 명목가) / 에쿼티 > `leverage_limit`(≤3.0) 거부. 포지션·잔고 조회 실패 시 거부 | 면제 |
| 증거금 | 가용 잔고 < 50 USD 거부 | 면제 |
| 슬리피지 | 지정가의 시세 대비 불리한 편차 > 0.5% 거부 (post-only 지정가는 검사 생략) | 해당 시 적용 |
| 속도 제한 | 최근 60초 주문 ≥ 108건(한도 120의 90%) 거부 | 면제 |

## 포지션 보호 장치

- **인라인 스톱로스**: 모든 진입 주문이 `진입가 × (1 − 0.70/3)`의 포지션 단위 스톱을 거래소에 싣는다. 봇이 죽어 있어도 거래소에서 작동한다. 청산 후 잔여 조건부 주문 취소를 시도하지만, 포지션 스톱이 청산 시 자동 해제되는지는 실거래소에서 검증하지 못했다(`execution.py` 모듈 docstring).
- **거래소 진실 동기화**: 매 봉 신호 전, 시작 시, 거부/미체결 후, housekeeping마다 거래소 포지션과 대조. 사라짐(스톱 체결·수동 청산)/출현(수동 진입)은 정합 + 알림.
- **중복 진입 방지**: 포지션 조회가 실패하면 진입 보류 후 재시도.
- **고아 주문 정리**: 시작 시 미체결 주문 취소.
- **로그 → 알림**: ERROR/CRITICAL 로그는 Telegram 이상 알림으로도 나간다.

## 옛 dead-man을 대체한 것

옛 시스템은 execution-engine 하트비트가 5분 끊기면 오케스트레이터가 SYSTEM 킬스위치를 발동했다. 새 봇은 한 프로세스이므로 프로세스 간 하트비트가 없다.

| 장치 | 역할 |
|---|---|
| `restart: unless-stopped` | 프로세스 사망 시 자동 재시작 (포지션 유지, 시작 시 동기화·놓친 봉 처리) |
| Docker `HEALTHCHECK` | `/tmp/heartbeat_ok`가 25분 이상 갱신 안 되면 unhealthy (자동 재시작은 하지 않음 — 표시용) |
| 호스트 cron `scripts/watchdog.sh` | 5분마다 컨테이너 상태 + `bot_state` 하트비트(<25분) 확인, 실패 시 Telegram 1회 알림 ([operations.md](operations.md)) |
| 일일 리포트·`/status` | 하트비트 나이 표시 |

watchdog은 **알리기만** 한다. 포지션을 청산하거나 봇을 재시작하지 않는다.

## 알려진 한계

- **단일 프로세스, 단일 호스트**: 호스트나 Docker 데몬이 죽으면 봇도, 외부 감시(cron)도 같이 멈출 수 있다. 감시는 호스트 cron 하나뿐이다.
- **킬스위치는 15분 주기이고 지갑 잔고 기준**이다. 미실현 손실은 에쿼티에 반영되지 않으므로(코드 주석 기준) 열린 포지션의 평가손은 킬스위치를 발동시키지 못한다. 그 구간의 보호는 거래소 스톱(−23.33%)과 EMA/ATR 청산 신호뿐이다. 낙폭은 손실이 실현·정산된 뒤 잡힌다.
- **flat일 때 PORTFOLIO 발동은 알림만**이다(결정 사항). 이후 진입이 허용되므로 같은 낙폭 상태에서 새 포지션을 열 수 있다.
- **STRATEGY·SYSTEM 레벨 미사용**: API 장애·하트비트 단절로 인한 자동 청산은 없다.
- **재시작 시 잔고 게이트 없음**: 옛 `EXPECTED_INITIAL_BALANCE_USD` 검증이 없어 잘못된 계정/잔고로 떠도 막지 않는다.
- **긴 주문 중 하트비트 정지**: 주문 처리 동안 스케줄러가 막혀 하트비트가 수 분 멈춘다(최대 ~7분, 주문 타임아웃 420초). 25분 한도 안이지만, 그 동안 스케줄러(housekeeping·리스크 평가)는 돌지 않는다. `stop_grace_period`는 450초로 주문 타임아웃보다 길어, 정상 종료는 진행 중인 주문을 마친 뒤 이뤄진다.
- **CLI 긴급 청산**은 봇이 살아 있을 때 `KillSwitch.trigger_manual`을 호출하지 않는다(`manual_halt`만 기록). 진입 차단은 ≤60초 내 반영된다.
- 가정·미검증: Bybit 포지션 스톱의 청산 시 자동 해제, 지갑 잔고가 미실현 손익을 제외한다는 점은 이 문서 작성 시점에 실거래소로 검증하지 못했다.
