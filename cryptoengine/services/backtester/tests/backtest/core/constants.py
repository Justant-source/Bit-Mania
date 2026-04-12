"""
Backtest Constants — Single Source of Truth
===========================================
모든 백테스트 파일은 여기서 상수를 임포트해야 합니다.
하드코딩 금지: TAKER_FEE, MAKER_FEE, SLIPPAGE_PCT 는 이 파일에서만 정의합니다.

기준: Bybit VIP 0 (2026)
"""

# ──────────────────────────────────────────────
# 거래 비용 (Bybit VIP 0, 2026 기준)
# ──────────────────────────────────────────────
MAKER_FEE     = 0.0002    # 0.02%  — 지정가 메이커 수수료
TAKER_FEE     = 0.00055   # 0.055% — 시장가 테이커 수수료
SLIPPAGE_PCT  = 0.0003    # 0.03%  — 보수적 슬리피지 추정

# 복합 비용 (편의 상수)
ROUND_TRIP_FEE      = TAKER_FEE * 2         # 진입+청산 모두 taker 시
ROUND_TRIP_FEE_FULL = TAKER_FEE * 2 + SLIPPAGE_PCT * 2  # 슬리피지 포함
ONE_WAY_FEE         = TAKER_FEE + SLIPPAGE_PCT           # 편도

# ──────────────────────────────────────────────
# 펀딩비 관련
# ──────────────────────────────────────────────
FUNDING_INTERVAL_HOURS = 8   # Bybit 기준: 00:00 / 08:00 / 16:00 UTC
FUNDING_INTERVALS_PER_DAY = 3
FUNDING_INTERVALS_PER_YEAR = FUNDING_INTERVALS_PER_DAY * 365  # 1095

# ──────────────────────────────────────────────
# Bybit BTCUSDT 선물 최소 주문 단위
# ──────────────────────────────────────────────
BYBIT_BTCUSDT_MIN_QTY  = 0.001   # BTC — 최소 주문 수량
BYBIT_BTCUSDT_TICK_SIZE = 0.1    # USD — 호가 단위

# ──────────────────────────────────────────────
# 리스크 한도
# ──────────────────────────────────────────────
MAX_LEVERAGE                   = 10      # 최대 허용 레버리지
DAILY_LOSS_KILL_SWITCH_PCT     = 0.03    # 3% 일손실 → Kill Switch
MAX_DRAWDOWN_KILL_SWITCH_PCT   = 0.15    # 15% MDD → Kill Switch

# ──────────────────────────────────────────────
# HMM / 레짐 감지
# ──────────────────────────────────────────────
MIN_REGIME_CONFIDENCE = 0.70   # HMM 신뢰도 70% 미만 신호 무시
MIN_HOLD_BARS         = 4      # 최소 4시간 보유 (4× 1h bar)

# ──────────────────────────────────────────────
# 펀딩비 차익거래 (FA) 기본값
# ──────────────────────────────────────────────
FA_MIN_FUNDING_THRESHOLD = 0.0001   # 연 환산 최소 진입 펀딩비
FA_MIN_CONSECUTIVE       = 3        # 연속 3회 이상 양수 펀딩비 확인
FA_MAX_HOLD_BARS         = 168      # 최대 168시간(7일) 보유
