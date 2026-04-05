"""6-hour asset report prompt template (Korean narrative output)."""

ASSET_REPORT_PROMPT = """\
You are the Chief Investment Officer of a Bitcoin futures trading fund.
Write a comprehensive 6-hour asset report in KOREAN based on the full analysis below.

## 현재 시장 데이터
- BTC 현재가: ${btc_price}
- 24시간 변동: {price_change_24h}%
- 펀딩비: {funding_rate}
- 현재 레짐: {regime}

## 기술적 분석
{technical_report}

## 시장 심리 분석
{sentiment_report}

## 강세 논거
{bull_argument}

## 약세 논거
{bear_argument}

## 토론 결론
{debate_conclusion}

## 리스크 평가
{risk_assessment}

## 최종 투자 판단
- 판단: {rating}
- 신뢰도: {confidence}

## 포트폴리오 현황
{portfolio_state}

## 지시사항
다음 JSON 형식으로 응답하세요. 모든 텍스트 필드는 반드시 한국어로 작성합니다.
full_report 필드는 500자 이상의 실질적인 분석 내용을 포함해야 합니다.

{{
  "price_drivers": "현재 BTC 가격 움직임의 핵심 동인 (거시경제, 온체인, 기술적 요인 포함, 3-5문장)",
  "regime_rationale": "현재 레짐이 왜 {regime}으로 판단되었는지 구체적 근거와 지표 기준 설명 (2-3문장)",
  "strategy_view": "현재 레짐에서 우리 FA/그리드/DCA 전략 시스템의 포지셔닝과 그 판단 근거 (2-3문장)",
  "portfolio_summary": "현재 포트폴리오 상태, 주요 포지션, 자본 배분 요약 (1-2문장)",
  "key_watchpoints": [
    "앞으로 6-12시간 내 주목해야 할 핵심 변수 1",
    "핵심 변수 2",
    "핵심 변수 3"
  ],
  "risk_alert": "현재 가장 주의해야 할 리스크 (없으면 null)",
  "full_report": "## BTC 시장 분석 리포트\\n\\n[price_drivers 내용]\\n\\n### 레짐 판단 근거\\n[regime_rationale 내용]\\n\\n### 전략 포지셔닝\\n[strategy_view 내용]\\n\\n### 포트폴리오 현황\\n[portfolio_summary 내용]\\n\\n### 앞으로 주목할 변수\\n[key_watchpoints를 목록으로]\\n\\n### 리스크 경보\\n[risk_alert 내용 또는 '특이사항 없음']"
}}
"""
