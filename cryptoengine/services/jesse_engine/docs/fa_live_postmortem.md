# FA Live Trading Postmortem — 근본 원인 분석 템플릿

## 개요

본 문서는 FA(Funding Arbitrage) 전략의 라이브 트레이딩 결과에 대한 A/B/C/D 근본 원인 분석(RCA, Root Cause Analysis) 구조를 정의합니다.

**평가 기간**: [PENDING: 사용자 제공 라이브 거래 데이터]  
**환경**: Bybit 테스트넷 또는 메인넷  
**전략 설정**: fa80_lev5_r30 (자본비율 80%, 레버리지 5x, 재투자 30%)

---

## 상태

🔴 **BLOCKED** — 사용자의 라이브 거래 로그(CSV/JSON) 필요

RCA를 진행하려면 다음 데이터가 필수입니다:

### 필요한 데이터

| 항목 | 형식 | 출처 | 예시 |
|------|------|------|------|
| **체결 기록** | CSV 또는 JSON | PostgreSQL `trades` 테이블 | `entry_ts, exit_ts, pnl, fee, ...` |
| **펀딩비 수익** | CSV | `funding_payments` 테이블 | `timestamp, amount_usd, direction, ...` |
| **포지션 히스토리** | CSV | `positions` 테이블 | `open_ts, close_ts, size, entry_price, ...` |
| **서비스 로그** | JSON Lines | `service_logs` 테이블 | `event, level, message, timestamp, ...` |
| **알림 기록** | JSON | Telegram 또는 로그 | `kill_switch, regime_change, margin_warning, ...` |

### 데이터 추출 예시

```bash
# PostgreSQL에서 CSV 내보내기
docker compose exec postgres psql -U cryptoengine -d cryptoengine \
  -c "COPY (SELECT * FROM trades ORDER BY entry_ts) TO STDOUT WITH CSV HEADER" \
  > live_trades.csv

docker compose exec postgres psql -U cryptoengine -d cryptoengine \
  -c "COPY (SELECT * FROM funding_payments ORDER BY timestamp) TO STDOUT WITH CSV HEADER" \
  > live_funding.csv

docker compose exec postgres psql -U cryptoengine -d cryptoengine \
  -c "COPY (SELECT * FROM positions ORDER BY open_ts) TO STDOUT WITH CSV HEADER" \
  > live_positions.csv
```

---

## 이미 알려진 사실 (기초 정보)

### 1. 데이터 갭으로 인한 기존 결과 과장

**자체 엔진 6년 백테스트 (CAGR +34.87%, Sharpe 3.583)의 문제점:**

| 기간 | 데이터 | 신뢰성 | 영향 |
|------|--------|--------|------|
| 2020-04 ~ 2023-03 | 합성 폴백 (0.0001 고정) | ❌ 매우 낮음 | **결과 극도 부풀려짐** |
| 2023-04 ~ 2026-03 | 실제 Bybit | ✅ 검증됨 | 참고 가능 |

**의미:**
- 2020-2022 비트코인 약세장 구간의 실제 펀딩비는 음수 또는 0.0001보다 훨씬 낮음
- 6년 평균 CAGR은 0.0001 합성값에 크게 의존 → 과장
- 라이브 결과 vs 백테스트 결과 비교 시 이를 감안해야 함

### 2. 예상되는 라이브 성과 범위 (3년 실데이터 기준)

기존 자체 엔진의 2023-2026 구간만 분석 결과 (공식 발표 미함, 추정):
- **CAGR 예상값**: +15~25% (6년 평균 +34.87%보다 낮음)
- **Sharpe 예상값**: 2.0~3.0
- **MDD 예상값**: -2% ~ -6%

### 3. 라이브 환경의 추가 리스크 (백테스트와 차이)

| 요소 | 백테스트 | 라이브 | 영향 |
|------|----------|--------|------|
| **체결 가격** | 정확 (close) | 슬리피지 | -0.1% ~ -0.3% |
| **펀딩비 변동성** | 평균값 | 극단 스파이크 | ±50% 변동 |
| **마진 호출** | 시뮬레이션만 | 실제 가능 | 청산 리스크 |
| **기술 장애** | 없음 | WebSocket 끊김, API 지연 | 주문 미체결 |
| **시장 충격** | 무시 | 극단 변동 (8시간 갭) | -5% 이상 |

---

## A/B/C/D 근본 원인 분석 (RCA) 구조

### Track A: 펀딩비 수익 분석

**질문**: 라이브에서 예상한 펀딩비 수익을 얻었는가?

#### A-1. 예상 vs 실제 펀딩비

```
[PENDING: 데이터 분석]

라이브 펀딩비 분포:
  - 평균: [PENDING]
  - 중위: [PENDING]
  - 표준편차: [PENDING]
  - 양수 구간: [PENDING] 시간 / 전체 ([PENDING]%)
  - 음수 구간: [PENDING] 시간 / 전체 ([PENDING]%)

자체 엔진 예상:
  - 평균: 0.00012 (2023-2026 실데이터)
  - 양수 구간: ~60% (추정)
  - 음수 구간: ~40% (추정)

근본 원인:
  [ ] A-1-a: 펀딩비가 예상보다 낮음 → 시장 약세
  [ ] A-1-b: 펀딩비 음수 구간이 예상보다 김 → 진입 신호 false positive
  [ ] A-1-c: 극단 스파이크 (±0.001) → 포지션 타이밍 불운
```

#### A-2. 펀딩비 수익 대 거래 수수료 비교

```
[PENDING: 수익 분석]

총 펀딩비 수익 (8시간 정산 누적):
  - 라이브 총액: [PENDING] USDT
  - 평균 거래당: [PENDING] USDT

거래당 수수료:
  - 진입 수수료: [PENDING] USDT (명목 × 0.055%)
  - 청산 수수료: [PENDING] USDT (명목 × 0.055%)
  - 합계: [PENDING] USDT (왕복)

수익성 판정:
  [ ] A-2-a: 펀딩비 수익 > 수수료 (흑자)
  [ ] A-2-b: 펀딩비 수익 ≈ 수수료 (손익분기)
  [ ] A-2-c: 펀딩비 수익 < 수수료 (적자) → 전략 재검토
```

### Track B: 거래 로직 및 진입/청산 정확도

**질문**: 진입 조건과 청산 조건이 예상대로 작동했는가?

#### B-1. 진입 신호 정확도

```
[PENDING: 거래 데이터 분석]

총 거래 수:
  - 라이브: [PENDING]회
  - 자체 엔진 예상 (6년): ~950회
  - 라이브 기간 예상: [PENDING]회
  - 편차: [PENDING]%

거짓 진입 (false positive):
  - 3회 연속 양수 펀딩비 후 즉시 음수로 전환: [PENDING]건
  - 기저 극단 확산 (+ 수익 실현): [PENDING]건
  - 원인 분석:
    [ ] B-1-a: 진입 임계값 (`min_funding_rate=0.0001`) 너무 낮음
    [ ] B-1-b: 연속 판독 수 (`consecutive_intervals=3`) 너무 적음
    [ ] B-1-c: 시장 레짐 필터 미적용
```

#### B-2. 청산 신호 정확도

```
[PENDING: 거래 데이터 분석]

평균 보유 기간:
  - 라이브: [PENDING] 시간
  - 자체 엔진 예상: ~48~120 시간
  - 편차: [PENDING]%

청산 사유 분포:
  - 펀딩비 반전 (3회): [PENDING]건 ([PENDING]%)
  - 최대보유 기간 (168h): [PENDING]건 ([PENDING]%)
  - 기저 극단 확산: [PENDING]건 ([PENDING]%)
  - Kill Switch: [PENDING]건 ([PENDING]%)
  - 기타: [PENDING]건 ([PENDING]%)

조기 청산 (기대 이하):
  [ ] B-2-a: 펀딩비 반전이 거짓 신호 → 필터 강화
  [ ] B-2-b: 최대보유 기간 도달 전 종료 → 기한 연장 검토
```

### Track C: 위험 관리 및 마진/청산 이벤트

**질문**: 마진 비율과 청산 위험은 관리 가능한 수준이었는가?

#### C-1. 마진 비율 히스토리

```
[PENDING: 마진 데이터 분석]

최소 마진 비율:
  - 라이브: [PENDING]x
  - 자체 엔진 예상 (최악): ~2.0x
  - 안전 기준: ≥2.0x (유지마진 0.5% 기준)

마진 경고 발생:
  - 1.5x 이하: [PENDING]회
  - 1.0x 이하 (위험): [PENDING]회
  - 청산 발생: [PENDING]회

근본 원인:
  [ ] C-1-a: 극단 변동 (2022년 5월 같은 시나리오) → 레버리지 과도
  [ ] C-1-b: 포지션 사이징 오류 → 자본비율 재검토
  [ ] C-1-c: 기저 극단 확산 → 청산 기준 강화
```

#### C-2. Kill Switch 발동 여부

```
[PENDING: Kill Switch 로그 분석]

일일 손실 임계값:
  - 설정: [PENDING]% (일일 최대 손실)
  - 발동 건수: [PENDING]회
  - 발동 시점 및 손실액: [PENDING]

최대낙폭 임계값:
  - 설정: [PENDING]%
  - 도달 여부: [PENDING]

Kill Switch 원인:
  [ ] C-2-a: 예정된 발동 (정상 안전장치)
  [ ] C-2-b: 거짓 발동 (임계값 재조정 필요)
  [ ] C-2-c: 미발동 (임계값 너무 높음, 위험)
```

### Track D: 시장 환경 및 회귀 분석

**질문**: 라이브 기간의 시장 환경이 백테스트 가정과 일치했는가?

#### D-1. 시장 레짐 분석

```
[PENDING: 시장 데이터 분석]

보유 기간 동안의 시장 상태:
  - 트렌딩 업: [PENDING]% (회귀 거래에 유리)
  - 트렌딩 다운: [PENDING]% (회귀 거래에 불리)
  - 레인징: [PENDING]% (안정적)
  - 변동성: [PENDING]% (극단)

BTC 가격 변동:
  - 시작: [PENDING] USD
  - 종료: [PENDING] USD
  - 총 변화: [PENDING]% ([PENDING] USD)
  - 최대상승: [PENDING]% (라이브 기간)
  - 최대하락: [PENDING]% (라이브 기간)

근본 원인:
  [ ] D-1-a: 강한 다운트렌드 → 펀딩비 음수 지속 → 진입 기회 감소
  [ ] D-1-b: 극단 변동성 → 마진 추가증거금 / 청산 위험
  [ ] D-1-c: 예상과 일치 → 전략 강건성 확인
```

#### D-2. 펀딩비와 기저 스프레드의 관계

```
[PENDING: 데이터 분석]

기저 스프레드 변동:
  - 평균: [PENDING]%
  - 최대: [PENDING]%
  - 최소: [PENDING]%
  - 99.9 퍼센타일: [PENDING]%

기저와 펀딩비의 상관도:
  - 상관계수: [PENDING]
  - 인과성: [PENDING]

원인 분석:
  [ ] D-2-a: 기저 극단 확산 → 미실현 손실 발생
  [ ] D-2-b: 펀딩비와 기저 괴리 → 차익 기회 손실
  [ ] D-2-c: 정상 관계 유지 → 전략 논리 유효
```

---

## 최종 판정 (Verdict)

### 시나리오 1: 라이브 성공 (CAGR > 15%)

```
모든 Track 통과:
  ✓ 펀딩비 수익 > 수수료 (A-2)
  ✓ 진입/청산 신호 정확 (B-1, B-2)
  ✓ 마진 비율 관리 양호 (C-1)
  ✓ 시장 환경 적응 (D-1, D-2)

결론:
  → fa80_lev5_r30 설정 검증됨
  → 메인넷 확대 배포 권고
  → 단, 마진 안전배율 2.0x 이상 유지 모니터링
```

### 시나리오 2: 부분 성공 (CAGR 5~15%)

```
일부 Track 실패:
  ✗ 펀딩비 수익 < 수수료 (A-2)
    또는
  ✗ 거짓 진입 과다 (B-1)

원인 및 개선:
  1. 진입 임계값 상향 (`min_funding_rate = 0.0002`)
  2. 연속 판독 수 증가 (`consecutive_intervals = 5`)
  3. 시장 레짐 필터 추가 (트렌딩/레인징 구분)
  
다음 단계:
  → 파라미터 조정 후 재검증
  → 테스트넷 추가 1주 운영
  → 메인넷 진입은 보류
```

### 시나리오 3: 실패 (CAGR < 5% 또는 마이너스)

```
대다수 Track 실패:
  ✗ 극단 청산 발생 (C-2)
  ✗ Kill Switch 연속 발동 (C-2)
  ✗ 시장 환경 악화 (D-1)

원인:
  → 2020-2022 합성 폴백 제거 후 성과 급락
  → 현재 파라미터 (fa80_lev5_r30) 부적절
  → 보수 설정 필요

개선안:
  1. fa80_lev4_r30 (레버리지 4x로 축소)
  2. 또는 fa50_lev5_r30 (자본비율 50% 축소)
  3. 펀딩비 최소 임계값 0.00015로 상향
  
다음 단계:
  → 재백테스트 (자체 엔진 또는 Jesse)
  → 개선 파라미터 검증
  → 메인넷 진입 재검토 (6개월 후)
```

---

## 데이터 제공 요청

**현재 상태**: 🔴 BLOCKED

RCA를 진행하려면 다음 데이터를 사용자가 제공해야 합니다:

### 필수 파일 목록

1. **live_trades.csv** — 모든 체결 기록
   ```
   entry_ts,exit_ts,entry_price,exit_price,side,size,pnl,fee_entry,fee_exit,funding_accumulated
   ```

2. **live_funding.csv** — 펀딩비 정산 기록
   ```
   timestamp,rate,position_value,funding_payment,cumulative
   ```

3. **live_positions.csv** — 포지션 히스토리
   ```
   open_ts,close_ts,entry_price,exit_price,size,notional,reason
   ```

4. **live_logs.jsonl** — 서비스 로그 (선택)
   ```
   {"ts": "...", "event": "...", "level": "...", "message": "..."}
   ```

### 추출 방법

```bash
# 컨테이너에서 직접 쿼리
docker compose exec postgres psql -U cryptoengine -d cryptoengine << 'EOF'
\COPY (SELECT * FROM trades ORDER BY entry_ts) TO '/tmp/live_trades.csv' WITH CSV HEADER;
\COPY (SELECT * FROM funding_payments ORDER BY timestamp) TO '/tmp/live_funding.csv' WITH CSV HEADER;
EOF

docker compose cp postgres:/tmp/live_trades.csv ./live_trades.csv
docker compose cp postgres:/tmp/live_funding.csv ./live_funding.csv
```

---

## 참고자료

- [FA 전략 명세서](./fa_strategy_specification.md)
- [Jesse vs 자체 엔진 비교](./fa_jesse_vs_self_engine.md)
- [자체 엔진 6년 백테스트](/.result/12.sta.html)
- [CLAUDE.md 프로젝트 가이드](/CLAUDE.md)

---

**작성일**: 2026-04-12  
**상태**: 대기 중 (라이브 데이터 대기)  
**다음 단계**: 사용자로부터 CSV 파일 수신 → RCA 분석 진행
