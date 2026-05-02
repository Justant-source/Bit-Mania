---
last_updated: 2026-05-02
---

# Grafana 색상/임계값 표준 (SSOT)

모든 대시보드의 색상 설정은 이 파일의 토큰을 인용한다. 인라인 매직 넘버 금지.

## 색상 토큰

### A. 자산/PnL 계열 (값의 부호로 판단)
| 토큰 | 색상 | HEX | 사용처 |
|---|---|---|---|
| `pnl.positive` | 녹색 | #2E7D32 | PnL > 0, 미실현 수익 양수 |
| `pnl.neutral` | 회색 | #757575 | PnL ≈ 0 (±0.1% 이내) |
| `pnl.negative` | 적색 | #C62828 | PnL < 0 |

### B. 위험 게이지 계열 (단조 증가 위험도)
| 토큰 | 색상 | HEX | 의미 |
|---|---|---|---|
| `risk.safe` | 녹색 | #2E7D32 | 안전 영역 |
| `risk.caution` | 황색 | #F9A825 | 주의 |
| `risk.danger` | 적색 | #C62828 | 위험 |
| `risk.critical` | 진적색 + 깜빡임 | #B71C1C | 즉시 조치 |

### C. 신뢰도/품질 계열
| 토큰 | 색상 | HEX | 의미 |
|---|---|---|---|
| `confidence.low` | 회색 | #9E9E9E | <50% |
| `confidence.medium` | 청색 | #1976D2 | 50~75% |
| `confidence.high` | 진청색 | #0D47A1 | >75% |

### D. 레짐 색상 (분류)
| 레짐 | 색상 | HEX |
|---|---|---|
| RANGING | 회색 | #757575 |
| TRENDING_UP | 녹색 | #2E7D32 |
| TRENDING_DOWN | 적색 | #C62828 |
| VOLATILE | 보라 | #6A1B9A |

### E. LLM 레이팅 (5단계)
| 레이팅 | 색상 | HEX |
|---|---|---|
| STRONG_BUY | 진녹색 | #1B5E20 |
| BUY | 녹색 | #66BB6A |
| NEUTRAL | 회색 | #BDBDBD |
| SELL | 주황 | #FB8C00 |
| STRONG_SELL | 진적색 | #B71C1C |

## 임계값 표

### 청산 위험 (P0 핵심)
| 지표 | safe | caution | danger | critical |
|---|---|---|---|---|
| 청산까지 거리 (%) | ≥20 | 10~20 | 5~10 | <5 |
| 마진 사용률 (margin_ratio) | <0.5 | 0.5~0.7 | 0.7~0.85 | ≥0.85 |
| 일일 손실 한도 진행률 | <50% | 50~70% | 70~90% | ≥90% |
| 주간 손실 한도 진행률 | <50% | 50~70% | 70~90% | ≥90% |

### Kill Switch 트리거 (Phase 4 / Phase 5)
| Layer | Phase 4 | Phase 5 |
|---|---|---|
| 일일 손실 | -5% 상대값 | -$10 절대값 |
| 최대 낙폭(주간) | -10% 상대값 | -$20 절대값 |
| 마진 비율 | >1.5x | 동일 |
| 변동성 | 15분 ATR > 기준×3 | 동일 |

### 데이터 신선도 (Operations 패널용)
| 서비스 | green (sec) | yellow (sec) | red (sec) |
|---|---|---|---|
| market-data | <120 | 120~300 | >300 |
| execution-engine | <30 | 30~120 | >120 |
| funding-arb | <120 | 120~300 | >300 |
| llm-advisor | <21600 | 21600~43200 | >43200 |
| OHLCV 수집 | <120 | 120~600 | >600 |
| 펀딩비 수집 | <1800 | 1800~3600 | >3600 |

### 인프라 (Prometheus)
| 메트릭 | safe | caution | danger |
|---|---|---|---|
| CPU | <60% | 60~85% | >85% |
| 메모리 | <70% | 70~85% | >85% |
| 디스크 | <70% | 70~85% | >85% (>90% critical) |
| Redis 메모리 | <60% | 60~80% | >80% |

## Grafana JSON 색상 적용 패턴 (참고)

```json
{
  "fieldConfig": {
    "defaults": {
      "color": { "mode": "thresholds" },
      "thresholds": {
        "mode": "absolute",
        "steps": [
          { "color": "#B71C1C", "value": null },
          { "color": "#C62828", "value": 5 },
          { "color": "#F9A825", "value": 10 },
          { "color": "#2E7D32", "value": 20 }
        ]
      },
      "unit": "percent"
    }
  },
  "description": "청산까지 거리(%). 토큰: risk.critical<5%, risk.danger 5~10%, risk.caution 10~20%, risk.safe≥20%"
}
```
