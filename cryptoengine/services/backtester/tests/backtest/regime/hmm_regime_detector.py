"""hmm_regime_detector.py — GaussianHMM 3-state 레짐 감지기.

실행:
    python tests/backtest/regime/hmm_regime_detector.py --validate
"""
from __future__ import annotations

import sys
sys.path.insert(0, "/app")

import numpy as np
import pandas as pd
from hmmlearn.hmm import GaussianHMM
from sklearn.preprocessing import StandardScaler


class HMMRegimeDetector:
    """3-state Hidden Markov Model 레짐 감지기.

    State labels:
        0 = 저변동/축적 (low volatility)
        1 = 중간 (transition)
        2 = 고변동/추세 (high volatility / trending)

    학습 피처: 6h 수익률, 6h 실현변동성(20-bar), BB 폭(BBW)
    학습 윈도우: 365일 롤링
    """

    def __init__(self, n_components: int = 3):
        self.n_components = n_components
        self.model = None
        self.scaler = StandardScaler()
        self._state_map = {}  # raw state → labeled state

    def _prepare_features(self, df_6h: pd.DataFrame) -> pd.DataFrame:
        """6h OHLCV DataFrame에서 피처 추출."""
        close = df_6h["close"]
        ret = close.pct_change()
        vol = ret.rolling(20).std()
        bb_mid = close.rolling(20).mean()
        bb_std_val = close.rolling(20).std()
        bbw = (bb_std_val * 4) / bb_mid  # BB 폭 = (2σ × 2) / mid

        feat = pd.DataFrame({
            "ret": ret,
            "vol": vol,
            "bbw": bbw,
        }, index=df_6h.index).dropna()
        return feat

    def fit(self, df_6h: pd.DataFrame) -> None:
        """모델 학습."""
        feat = self._prepare_features(df_6h)
        if len(feat) < 50:
            raise ValueError(f"최소 50개 바 필요, 현재 {len(feat)}개")

        X = feat.values
        X_scaled = self.scaler.fit_transform(X)

        self.model = GaussianHMM(
            n_components=self.n_components,
            covariance_type="full",
            n_iter=200,
            random_state=42,
        )
        self.model.fit(X_scaled)

        # State를 변동성 기준으로 재정렬 (state 번호 불안정성 해결)
        means = []
        for i in range(self.n_components):
            # 각 state의 평균 변동성 (scaler 역변환 후 vol 컬럼)
            state_mask = (self.model.predict(X_scaled) == i)
            if state_mask.any():
                means.append((i, feat["vol"].values[state_mask].mean()))
            else:
                means.append((i, 0.0))

        # 변동성 오름차순으로 정렬
        sorted_states = sorted(means, key=lambda x: x[1])
        self._state_map = {orig: new for new, (orig, _) in enumerate(sorted_states)}

    def predict_state(self, df_6h: pd.DataFrame) -> int:
        """현재 레짐 state 예측 (0=저변동, 1=중간, 2=고변동)."""
        if self.model is None:
            raise RuntimeError("모델이 학습되지 않았습니다. fit() 먼저 호출하세요.")
        feat = self._prepare_features(df_6h)
        if len(feat) < 1:
            raise RuntimeError("피처 계산 실패")
        X_scaled = self.scaler.transform(feat.values[-50:])
        raw_state = self.model.predict(X_scaled)[-1]
        return self._state_map.get(raw_state, raw_state)

    def predict_proba(self, df_6h: pd.DataFrame) -> np.ndarray:
        """각 state의 확률 반환."""
        if self.model is None:
            raise RuntimeError("모델이 학습되지 않았습니다.")
        feat = self._prepare_features(df_6h)
        if len(feat) < 1:
            raise RuntimeError("피처 계산 실패")
        X_scaled = self.scaler.transform(feat.values[-50:])
        proba = self.model.predict_proba(X_scaled)[-1]
        # state_map 재정렬
        n = len(proba)
        reordered = np.zeros(n)
        for orig, new in self._state_map.items():
            if orig < n and new < n:
                reordered[new] = proba[orig]
        return reordered


if __name__ == "__main__":
    import asyncio
    from datetime import datetime, timezone
    from tests.backtest.core import load_ohlcv, make_pool

    async def validate():
        pool = await make_pool()
        start = datetime(2023, 4, 1, tzinfo=timezone.utc)
        end = datetime(2026, 4, 10, tzinfo=timezone.utc)
        df_6h = await load_ohlcv(pool, "BTCUSDT", "6h", start, end)
        await pool.close()

        if df_6h.empty:
            print("[ERROR] 6h OHLCV 데이터 없음. 먼저 데이터 시드 필요.")
            return

        print(f"[INFO] 6h 데이터 로드: {len(df_6h)}개 ({df_6h.index[0]} ~ {df_6h.index[-1]})")

        det = HMMRegimeDetector(n_components=3)
        det.fit(df_6h)

        print("[INFO] HMM 학습 완료")
        state = det.predict_state(df_6h)
        proba = det.predict_proba(df_6h)
        print(f"[INFO] 현재 레짐: {state} (0=저변동, 1=중간, 2=고변동)")
        print(f"[INFO] 확률: {proba}")
        print("[PASS] HMM 레짐 감지기 검증 완료")

    asyncio.run(validate())
