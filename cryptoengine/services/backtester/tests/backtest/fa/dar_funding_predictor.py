"""dar_funding_predictor.py — Dynamic AutoRegressive 펀딩비 예측 모델.

Dynamic AutoRegressive (DAR) 모델을 사용하여 다음 8시간 펀딩비를 예측.
학습 윈도우: 60일(720개 8h 바) 롤링
재추정: 매일 한 번 (또는 매 8시간)
폴백: 학습 데이터 부족 시 7일 이동평균
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Optional


class DARFundingPredictor:
    """DAR 펀딩비 예측기.

    학습 윈도우를 기반으로 동적 회귀 모델 구성:
        funding_t+1 = β0 + β1·funding_t + β2·funding_t-1 + β3·funding_t-2 + ε

    Args:
        window: 학습 윈도우 (8h 바 수, 기본 180 = 60일)
    """

    def __init__(self, window: int = 180):
        self.window = window
        self._coefs: Optional[np.ndarray] = None

    def fit(self, funding_series: pd.Series) -> None:
        """최근 window 구간으로 DAR 모델 학습.

        Args:
            funding_series: 펀딩비 시리즈 (index는 무관, values만 사용)
        """
        if len(funding_series) < 10:
            self._coefs = None
            return

        data = funding_series.iloc[-self.window:].values
        if len(data) < 10:
            self._coefs = None
            return

        # X: [t-1, t-2, t-3, constant] → y: t+1
        n = len(data) - 3
        if n < 5:
            self._coefs = None
            return

        X = np.column_stack([
            data[2:-1],    # t-1
            data[1:-2],    # t-2
            data[:-3],     # t-3
            np.ones(n),    # intercept
        ])
        y = data[3:]       # t+1

        try:
            # Least squares 회귀
            self._coefs, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
        except Exception:
            self._coefs = None

    def predict(self, funding_series: pd.Series) -> float:
        """다음 8h 펀딩비 예측.

        Args:
            funding_series: 최근 펀딩비 시리즈

        Returns:
            float: 예측된 다음 8h 펀딩비 (±0.75% 범위 클리핑)
        """
        if self._coefs is None or len(funding_series) < 3:
            # 폴백: 7일(21개 8h) 이동평균
            window = min(21, len(funding_series))
            if window > 0:
                return float(funding_series.iloc[-window:].mean())
            return 0.0

        data = funding_series.values
        x = np.array([data[-1], data[-2], data[-3], 1.0])
        pred = float(np.dot(self._coefs, x))

        # 클리핑: ±0.75%/8h 범위
        return float(np.clip(pred, -0.0075, 0.0075))

    def validate_on_test_set(self, train_series: pd.Series, test_series: pd.Series) -> dict:
        """테스트 세트에서 DAR 모델 성능 검증.

        Args:
            train_series: 학습 데이터
            test_series: 테스트 데이터

        Returns:
            dict: {rmse, mae, mape, count}
        """
        self.fit(train_series)

        predictions = []
        actuals = []

        # 테스트 데이터를 한 스텝씩 진행
        combined = pd.concat([train_series, test_series])
        for i in range(len(train_series), len(combined) - 1):
            hist = combined.iloc[:i]
            actual = combined.iloc[i + 1]

            if len(hist) >= 3:
                pred = self.predict(hist)
                predictions.append(pred)
                actuals.append(actual)

        if not predictions:
            return {"rmse": 0.0, "mae": 0.0, "mape": 0.0, "count": 0}

        predictions = np.array(predictions)
        actuals = np.array(actuals)

        rmse = float(np.sqrt(np.mean((predictions - actuals) ** 2)))
        mae = float(np.mean(np.abs(predictions - actuals)))

        # MAPE: 0으로 나누기 방지
        abs_actuals = np.abs(actuals)
        mape_values = np.where(
            abs_actuals > 1e-6,
            np.abs(predictions - actuals) / abs_actuals,
            0.0
        )
        mape = float(np.mean(mape_values[abs_actuals > 1e-6])) if np.any(abs_actuals > 1e-6) else 0.0

        return {
            "rmse": rmse,
            "mae": mae,
            "mape": mape,
            "count": len(predictions),
        }
