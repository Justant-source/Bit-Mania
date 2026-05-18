FROM python:3.12-slim

WORKDIR /app

# System deps for asyncpg
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY services/telegram-bot/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/telegram-bot .

# Make shared package available
COPY shared /app/shared

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
