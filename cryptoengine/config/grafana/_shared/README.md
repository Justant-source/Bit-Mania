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

## Task 7에서 적용 예정
- Home Dashboard: `GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH` → overview.json
- PHASE5_MODE 환경변수 토글
- 기존 json/ 폴더 내 6개 파일 삭제
