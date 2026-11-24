"""Recording Notifier for tests. Every call is appended to ``calls`` as (method, kwargs)."""

from __future__ import annotations

from typing import Any


class FakeNotifier:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def _rec(self, name: str, **kw: Any) -> None:
        self.calls.append((name, kw))

    def of(self, name: str) -> list[dict[str, Any]]:
        """kwargs of every call to ``name``."""
        return [kw for n, kw in self.calls if n == name]

    @property
    def names(self) -> list[str]:
        return [n for n, _ in self.calls]

    async def send(self, html: str, *, dedup_key: str | None = None) -> None:
        self._rec("send", html=html, dedup_key=dedup_key)

    async def entry(self, *, qty: float, price: float, stop_loss: float | None,
                    equity: float | None) -> None:
        self._rec("entry", qty=qty, price=price, stop_loss=stop_loss, equity=equity)

    async def exit(self, *, qty: float, entry_price: float | None, exit_price: float,
                   pnl: float | None, reason: str, hold: str | None) -> None:
        self._rec("exit", qty=qty, entry_price=entry_price, exit_price=exit_price,
                  pnl=pnl, reason=reason, hold=hold)

    async def order_rejected(self, *, side: str, reason: str, will_retry: bool) -> None:
        self._rec("order_rejected", side=side, reason=reason, will_retry=will_retry)

    async def kill_switch(self, *, level: str, reason: str, action: str,
                          daily_dd_pct: float, weekly_dd_pct: float) -> None:
        self._rec("kill_switch", level=level, reason=reason, action=action,
                  daily_dd_pct=daily_dd_pct, weekly_dd_pct=weekly_dd_pct)

    async def anomaly(self, *, kind: str, message: str) -> None:
        self._rec("anomaly", kind=kind, message=message)
