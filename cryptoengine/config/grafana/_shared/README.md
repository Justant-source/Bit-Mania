---
last_updated: 2026-05-02
---

# Grafana 공유 표준

이 폴더는 모든 대시보드가 참조하는 SSOT(Single Source of Truth) 문서 모음.
CryptoEngine Grafana 대시보드 재설계(2026-05-02)의 산출물.

## 파일
- `thresholds.md` — 색상/임계값 토큰 (A~E 5개 카테고리)
- `refresh_policy.md` — 새로고침 정책 (6개 대시보드)
- `sql_library.md` — 공통 SQL 쿼리 (Task 2에서 작성)
- `prometheus_metrics_audit.md` — Prometheus 메트릭 감사 결과 (Task 2에서 작성)

## 사용 규칙
1. 대시보드 JSON에서 색상은 직접 HEX 입력하되, 패널 description에 토큰명 주석 추가
   (예: "토큰: risk.danger < 5%, risk.safe ≥ 20%")
2. 임계값은 항상 `thresholds.md`에서 인용. 변경 시 이 문서를 먼저 수정
3. 새 대시보드 추가 시 Task 7 이후 이 README의 대시보드 맵 업데이트

## 대시보드 폴더 구조
```
config/grafana/dashboards/
├── trading/           # Overview, Performance & Risk, Strategies & Positions
├── operations/        # Operations
├── analysis/          # LLM Intelligence
└── mobile/            # Overview (Mobile)
```

---

## 최종 대시보드 맵 (2026-05-02 재설계 완료)

| 폴더 | 대시보드 | uid | 주기 | 용도 |
|---|---|---|---|---|
| Trading | Overview | `ce-overview` | 30s | 1순위 진입점, 청산 위험 모니터 |
| Trading | Performance & Risk | `ce-performance-risk` | 1m | 수익/리스크 시계열 분석 |
| Trading | Strategies & Positions | `ce-strategies-positions` | 30s | 전략 운영 + 오픈 포지션 |
| Operations | Operations | `ce-operations` | 30s | 인프라 + 로그 + 데이터 파이프라인 |
| Analysis | LLM Intelligence | `ce-llm-intelligence` | 5m | LLM 고유 통계 + 권장 vs 실행 일치율 |
| Mobile | Overview (Mobile) | `ce-overview-mobile` | 30s | 모바일 긴급 대응 (세로 풀폭) |

## 사용 시나리오

| 상황 | 우선 확인 대시보드 |
|---|---|
| 출근 첫 화면 | Overview (자동 홈 설정) |
| 청산 위험 대응 | Overview Row 2 → Strategies & Positions Section D |
| 전일 성과 리뷰 | Performance & Risk |
| 전략 진단 | Strategies & Positions |
| 장애 발생 | Operations |
| 외출 중 점검 | Mobile Overview |
| LLM 효용 검토 | LLM Intelligence |

## 변경 이력

### 2026-05-02: Grafana 재설계 완료 (Task 7)

**신규**:
- 청산 위험 시각화 (Overview Row 2: 4개 패널)
- 자산 곡선 + Kill Switch / 레짐 어노테이션
- 인프라 메트릭 시각화 (Operations Section B: CPU/메모리/디스크/Redis)
- LogWriter 큐 모니터링 (Operations Section D)
- 로그 검색 변수 ($log_search)
- Phase 5 일일 손실 한도 표기 ($-10)
- LLM 권장 vs 실행 일치율 패널 (id=3 교체)
- Mobile Overview (8개 패널, 세로 전체폭)

**구조**:
- 폴더: 4개 (Trading/Operations/Analysis/Mobile)
- 대시보드: 6개 (Trading 3개 + Operations 1개 + Analysis 1개 + Mobile 1개)
- Home Dashboard: Overview 자동 설정 (GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH)
- Phase 5 환경변수: PHASE5_MODE (기본값 false)

**삭제**:
- 레거시 대시보드 6개: main_cockpit, assets_overview, positions_orders, strategy_monitor, service-logs, system_health
- 구 provisioning YAML: dashboard.yaml
- json/ 폴더 내 llm_intelligence.json (analysis/ 로 이동)
