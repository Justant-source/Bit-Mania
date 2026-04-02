"""Exchange connector factory."""

from __future__ import annotations

from typing import Any

from shared.exchange.base import ExchangeConnector
from shared.exchange.binance import BinanceConnector
from shared.exchange.bybit import BybitConnector

_REGISTRY: dict[str, type[ExchangeConnector]] = {
    "bybit": BybitConnector,
    "binance": BinanceConnector,
}


def exchange_factory(
    exchange_id: str,
    **kwargs: Any,
) -> ExchangeConnector:
    """Instantiate a connector by name.

    >>> connector = exchange_factory("bybit", api_key="...", testnet=True)
    """
    cls = _REGISTRY.get(exchange_id.lower())
    if cls is None:
        raise ValueError(
            f"Unknown exchange '{exchange_id}'. "
            f"Available: {', '.join(sorted(_REGISTRY))}"
        )
    return cls(**kwargs)
