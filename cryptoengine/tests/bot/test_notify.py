import pytest
import pytest_asyncio
from aiohttp import web

from bot.notify import TelegramNotifier, truncate_html


class Server:
    def __init__(self):
        self.requests = []
        self.responses = []  # queue of (status, json); default ok

    async def handler(self, request):
        body = await request.json()
        self.requests.append((request.match_info["method"], body))
        status, js = self.responses.pop(0) if self.responses else (200, {"ok": True})
        return web.json_response(js, status=status)


@pytest_asyncio.fixture
async def srv():
    s = Server()
    app = web.Application()
    app.router.add_post("/bot{token}/{method}", s.handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    s.url = f"http://127.0.0.1:{port}"
    yield s
    await runner.cleanup()


def make(srv):
    return TelegramNotifier("tok", "123", api_base=srv.url)


@pytest.mark.asyncio
async def test_send_payload(srv):
    n = make(srv)
    await n.send("<b>hi</b>")
    await n.close()
    method, body = srv.requests[0]
    assert method == "sendMessage"
    assert body["chat_id"] == "123" and body["parse_mode"] == "HTML" and body["text"] == "<b>hi</b>"


@pytest.mark.asyncio
async def test_dedup_window(srv):
    n = make(srv)
    t = [1000.0]
    n._clock = lambda: t[0]
    await n.send("a", dedup_key="k")
    await n.send("a", dedup_key="k")
    t[0] += 119
    await n.send("a", dedup_key="k")
    await n.send("b", dedup_key="other")
    assert len(srv.requests) == 2
    t[0] += 2  # 121s after first send
    await n.send("a", dedup_key="k")
    assert len(srv.requests) == 3
    await n.close()


@pytest.mark.asyncio
async def test_html_fallback_to_plain(srv):
    srv.responses = [(400, {"ok": False, "description": "Bad Request: can't parse entities: x"})]
    n = make(srv)
    await n.send("<b>a &amp; b</b>")
    assert len(srv.requests) == 2
    assert "parse_mode" not in srv.requests[1][1]
    assert srv.requests[1][1]["text"] == "a & b"
    await n.close()


@pytest.mark.asyncio
async def test_rate_limit_drops_but_exempt_passes(srv):
    n = make(srv)
    for i in range(25):
        await n.send(f"m{i}")
    assert len(srv.requests) == 20
    await n.kill_switch(level="L2", reason="r", action="a", daily_dd_pct=1, weekly_dd_pct=2)
    assert len(srv.requests) == 21
    await n.close()


@pytest.mark.asyncio
async def test_never_raises_on_http_failure():
    n = TelegramNotifier("tok", "123", api_base="http://127.0.0.1:1")  # connection refused
    await n.send("x")
    await n.anomaly(kind="k", message="m")
    await n.entry(qty=0.007, price=80000.0, stop_loss=61000.0, equity=150.0)
    await n.close()


@pytest.mark.asyncio
async def test_never_raises_on_server_error(srv):
    srv.responses = [(500, {"ok": False})]
    n = make(srv)
    await n.send("x")
    await n.close()


@pytest.mark.asyncio
async def test_empty_credentials_noop(srv):
    n = TelegramNotifier("", "", api_base=srv.url)
    await n.send("x")
    await n.exit(qty=1, entry_price=None, exit_price=1, pnl=None, reason="r", hold=None)
    assert srv.requests == []


@pytest.mark.asyncio
async def test_formats_and_silence(srv):
    n = make(srv)
    await n.entry(qty=0.007, price=80000.0, stop_loss=61333.33, equity=150.0)
    await n.exit(qty=0.007, entry_price=80000.0, exit_price=78000.0, pnl=-14.5, reason="ema_cross<x>", hold="1d 2h")
    await n.order_rejected(side="buy", reason="insufficient", will_retry=True)
    await n.kill_switch(level="PORTFOLIO", reason="dd", action="close", daily_dd_pct=5.1, weekly_dd_pct=6.2)
    await n.anomaly(kind="boom", message="a < b")
    await n.close()
    bodies = [b for _, b in srv.requests]
    assert "매수 체결" in bodies[0]["text"] and bodies[0]["disable_notification"] is True
    assert "-14.50" in bodies[1]["text"] and "ema_cross&lt;x&gt;" in bodies[1]["text"]
    assert "주문 거부" in bodies[2]["text"]
    assert "KILL SWITCH" in bodies[3]["text"] and bodies[3]["disable_notification"] is False
    assert "a &lt; b" in bodies[4]["text"]


def test_truncate_closes_tags():
    msg = "<b>" + "x" * 5000 + "</b>"
    out = truncate_html(msg)
    assert len(out) <= 4096 and out.endswith("</b>")
