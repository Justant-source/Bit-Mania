FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev cron && \
    mkdir -p /var/spool/cron/crontabs && \
    rm -rf /var/lib/apt/lists/*

COPY services/backtester/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backtester service files
COPY services/backtester /app

# Copy top-level scripts (phase5_preflight, etc.)
COPY scripts /app/scripts

# Make shared package available
COPY shared /app/shared

# Results directory
RUN mkdir -p /app/results

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
