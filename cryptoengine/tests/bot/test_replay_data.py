"""bot.replay_data: Binance klines pagination/upsert logic with HTTP and DB mocked."""

from __future__ import annotations

import asyncio

from bot import replay_data as rd

H = rd.BAR_MS


def _k(ts: int, close_time: int | None = None) -> list:
    return [ts, "1.0", "2.0", "0.5", "1.5", "10.0", ts + H - 1 if close_time is None else close_time]


class FakeConn:
    def __init__(self, store: dict, last):
        self.store, self.last = store, last

    async def execute(self, *a):
        return None

    async def fetchval(self, q, *a):
        return max(self.store) if self.store else None

    async def executemany(self, q, rows):
        for r in rows:
            self.store[r[0]] = r[1:]


class _Acq:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *a):
        return False


class FakePool:
    def __init__(self, store=None):
        self.store = {} if store is None else store

    def acquire(self):
        return _Acq(FakeConn(self.store, None))


class FakeDB:
    def __init__(self, store=None):
        self.pool = FakePool(store)


class FakeResp:
    def __init__(self, data, status=200):
        self.data, self.status = data, status

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"http {self.status}")

    async def json(self):
        return self.data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class FakeSession:
    """Serves klines from a dict ts->row, honouring startTime/limit like Binance."""

    def __init__(self, bars: list[list], status=200):
        self.bars, self.status, self.calls = bars, status, []

    def get(self, url, params=None):
        self.calls.append(params["startTime"])
        out = [b for b in self.bars if b[0] >= params["startTime"]][: params["limit"]]
        return FakeResp(out, self.status)

    async def close(self):
        pass


def test_sync_starts_after_last_ts_and_skips_open_bar():
    base = 1_700_000_000_000 // H * H
    store = {base: (1, 1, 1, 1, 1)}
    bars = [_k(base + i * H) for i in range(1, 4)]
    now = base + 3 * H + 1000  # bar base+3H closes at base+4H-1 > now -> still open
    n = asyncio.run(rd.sync(FakeDB(store), now_ms=now, session=FakeSession(bars)))
    assert n == 2
    assert sorted(store) == [base, base + H, base + 2 * H]


def test_sync_paginates():
    base = 1_700_000_000_000 // H * H
    bars = [_k(base + i * H) for i in range(2500)]
    store: dict = {}
    sess = FakeSession(bars)
    # empty table starts at FIRST_TS; the fake serves everything >= startTime
    n = asyncio.run(rd.sync(FakeDB(store), now_ms=base + 3000 * H, session=sess))
    assert n == 2500 and len(store) == 2500
    assert len(sess.calls) == 3


def test_sync_noop_when_up_to_date():
    base = 1_700_000_000_000 // H * H
    store = {base: (1, 1, 1, 1, 1)}
    sess = FakeSession([_k(base)])
    n = asyncio.run(rd.sync(FakeDB(store), now_ms=base + H - 1, session=sess))
    assert n == 0 and sess.calls == []


def test_sync_failure_does_not_raise():
    base = 1_700_000_000_000 // H * H
    store: dict = {}
    n = asyncio.run(rd.sync(FakeDB(store), now_ms=base, session=FakeSession([_k(base)], status=429)))
    assert n == 0 and store == {}


def test_sync_survives_broken_db():
    class Broken:
        pool = None
    assert asyncio.run(rd.sync(Broken())) == 0


def test_ddl_matches_spec_tables():
    assert "replay_ohlcv_4h" in rd.DDL and "replay_reference_trades" in rd.DDL
    assert rd.DDL.count("CREATE TABLE IF NOT EXISTS") == 2
