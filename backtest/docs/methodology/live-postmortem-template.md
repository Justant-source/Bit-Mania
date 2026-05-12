---
title: FA 라이브 거래 근본 원인 분석 템플릿
category: test
related_code:
  - cryptoengine/scripts/phase5_preflight.py
last_updated: 2026-05-01
---

# FA 라이브 거래 Postmortem 템플릿

라이브 거래 결과에 대한 A/B/C/D 근본 원인 분석(RCA) 구조.

**평가 기간**: [PENDING: 사용자 제공 라이브 거래 데이터]  
**환경**: Bybit 테스트넷 또는 메인넷  
**전략 설정**: fa80_lev5_r30 (자본비율 80%, 레버리지 5x, 재투자 30%)

---

## 상태

🔴 **BLOCKED** — 사용자의 라이브 거래 로그(CSV/JSON) 필요

---

## 필요한 데이터

| 항목 | 형식 | 출처 | 예시 |
|------|------|------|------|
| **체결 기록** | CSV | PostgreSQL `trades` | entry_ts, exit_ts, pnl, fee |
| **펀딩비 수익** | CSV | `funding_payments` | timestamp, amount_usd |
| **포지션 히스토리** | CSV | `positions` | open_ts, close_ts, size |
| **서비스 로그** | JSON Lines | `service_logs` | event, level, message, ts |

### 데이터 추출

```bash
docker compose exec postgres psql -U cryptoengine -d cryptoengine << 'EOF'
\COPY (SELECT * FROM trades ORDER BY entry_ts) TO STDOUT WITH CSV HEADER > /tmp/trades.csv
\COPY (SELECT * FROM funding_payments ORDER BY timestamp) TO STDOUT WITH CSV HEADER > /tmp/funding.csv
EOF

docker compose cp postgres:/tmp/trades.csv ./
docker compose cp postgres:/tmp/funding.csv ./
```

---

## A/B/C/D RCA 구조

### Track A: 펀딩비 수익 분석

**질문**: 라이브에서 예상한 펀딩비 수익을 얻었는가?

#### A-1. 예상 vs 실제 펀딩비

```
[PENDING: 데이터 분석]

라이브 펀딩비 분포:
  - 평균: [PENDING]
  - 중위: [PENDING]
  - 표준편차: [PENDING]
  - 양수 구간: [PENDING]% 
  - 음수 구간: [PENDING]%

근본 원인:
  [ ] A-1-a: 펀딩비가 예상보다 낮음 → 시장 약세
  [ ] A-1-b: 음수 구간이 길어짐 → 진입 신호 false positive
  [ ] A-1-c: 극단 스파이크 → 포지션 타이밍 불운
```

#### A-2. 펀딩비 수익 vs 거래 수수료

```
[PENDING: 분석]

총 펀딩비 수익: [PENDING] USDT
거래당 왕복 수수료: [PENDING] USDT

수익성 판정:
  [ ] A-2-a: 펀딩비 > 수수료 (흑자)
  [ ] A-2-b: 펀딩비 ≈ 수수료 (손익분기)
  [ ] A-2-c: 펀딩비 < 수수료 (적자)
```

### Track B: 거래 로직 및 신호 정확도

**질문**: 진입/청산 조건이 예상대로 작동했는가?

#### B-1. 진입 신호 정확도

```
[PENDING: 분석]

총 거래 수: [PENDING]
예상 (자체 엔진): ~950회
편차: [PENDING]%

거짓 진입 (false positive):
  [ ] B-1-a: 임계값 (`min_funding_rate=0.0001`) 너무 낮음
  [ ] B-1-b: 연속 판독 수 (`consecutive_intervals=3`) 너무 적음
  [ ] B-1-c: 시장 레짐 필터 미적용
```

#### B-2. 청산 신호 정확도

```
[PENDING: 분석]

평균 보유 기간: [PENDING] 시간
예상: ~48~120 시간

청산 사유 분포:
  - 펀딩비 반전 (3회): [PENDING]건
  - 최대보유 기간 (168h): [PENDING]건
  - 기저 극단 확산: [PENDING]건
  - Kill Switch: [PENDING]건
```

### Track C: 위험 관리 및 마진

**질문**: 마진 비율과 청산 위험은 관리되었는가?

#### C-1. 마진 비율 히스토리

```
[PENDING: 분석]

최소 마진 비율: [PENDING]x
안전 기준: ≥2.0x

마진 경고:
  [ ] C-1-a: 극단 변동 → 레버리지 과도
  [ ] C-1-b: 포지션 사이징 오류 → 재검토
  [ ] C-1-c: 기저 극단 확산 → 청산 강화
```

#### C-2. Kill Switch 발동

```
[PENDING: 분석]

일일 손실 임계값: [PENDING]%
발동 건수: [PENDING]회

원인:
  [ ] C-2-a: 예정된 발동 (정상)
  [ ] C-2-b: 거짓 발동 (임계값 조정)
  [ ] C-2-c: 미발동 (임계값 높음, 위험)
```

### Track D: 시장 환경

**질문**: 라이브 기간의 시장 환경이 백테스트 가정과 일치했는가?

#### D-1. 시장 레짐

```
[PENDING: 분석]

BTC 가격 변화: [PENDING]%
변동성: [PENDING]% (극단)

원인:
  [ ] D-1-a: 강한 다운트렌드 → 펀딩비 음수
  [ ] D-1-b: 극단 변동성 → 마진 추가증거금
  [ ] D-1-c: 예상과 일치 → 전략 강건성 확인
```

#### D-2. 기저와 펀딩비 관계

```
[PENDING: 분석]

기저 스프레드 변동: [PENDING]%
상관도: [PENDING]

원인:
  [ ] D-2-a: 기저 극단 확산 → 미실현 손실
  [ ] D-2-b: 펀딩비와 괴리 → 차익 손실
  [ ] D-2-c: 정상 관계 유지 → 전략 유효
```

---

## 최종 판정

### 시나리오 1: 라이브 성공 (CAGR > 15%)

```
모든 Track 통과:
  ✓ 펀딩비 > 수수료
  ✓ 신호 정확
  ✓ 마진 관리 양호
  ✓ 환경 적응

결론:
  → fa80_lev5_r30 검증됨
  → 메인넷 확대 배포 권고
```

### 시나리오 2: 부분 성공 (CAGR 5~15%)

```
일부 Track 실패:
  ✗ 펀딩비 < 수수료
  ✗ 거짓 진입 과다

개선:
  1. 진입 임계값 상향 (0.0002)
  2. 연속 판독 수 증가 (5)
  3. 레짐 필터 추가
```

### 시나리오 3: 실패 (CAGR < 5%)

```
대다수 Track 실패:
  ✗ 극단 청산
  ✗ Kill Switch 연속

개선:
  → fa80_lev4_r30 (레버리지 축소)
  → 펀딩비 임계값 상향 (0.00015)
```

---

**최종 수정**: 2026-05-01
