"""
Backtest Exceptions — 실데이터 누락 시 조용한 합성 폴백을 금지합니다.
"""


class MissingDataError(Exception):
    """
    필요한 실데이터가 없을 때 발생합니다.
    합성(synthetic) 데이터로 조용히 대체하지 마세요.

    Usage:
        if not data_available:
            raise MissingDataError(
                "BTC 1m OHLCV 데이터 없음 (2023-01 ~ 2026-04). "
                "실행: python scripts/data/fetch_binance_vision.py --symbol BTCUSDT"
            )
    """
    pass


class DataQualityError(Exception):
    """
    데이터가 있지만 품질 검증에 실패할 때 발생합니다.
    (연속성 깨짐, 이상치, 결측치 초과 등)
    """
    pass


class BacktestConfigError(Exception):
    """
    백테스트 설정값이 잘못되었을 때 발생합니다.
    (잘못된 레버리지, 음수 자본, 잘못된 날짜 범위 등)
    """
    pass
