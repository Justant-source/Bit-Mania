# Single-process bot image. Build context: cryptoengine/ (same as the old services; see
# docker-compose.yml `build.context: .`). Build from the compose file, not by hand.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN useradd --create-home --uid 1000 --shell /usr/sbin/nologin bot

WORKDIR /app

COPY bot/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && rm /tmp/requirements.txt

COPY bot /app/bot
COPY shared /app/shared
COPY config/bot.yaml /app/config/bot.yaml

USER bot

EXPOSE 3000

# The bot touches /tmp/heartbeat_ok on every scheduler tick (<= 60 s apart). While an order
# is being worked (re-peg up to ~200 s, order timeout 420 s) the tick is blocked and the
# file can go ~5-7 min without a touch, so the window is 25 min.
HEALTHCHECK --interval=60s --timeout=10s --start-period=120s --retries=3 \
    CMD test -n "$(find /tmp/heartbeat_ok -mmin -25 2>/dev/null)" || exit 1

CMD ["python", "-m", "bot.main"]
