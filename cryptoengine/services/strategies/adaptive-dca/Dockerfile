FROM python:3.12-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY services/strategies/adaptive-dca/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Shared library
COPY shared /app/shared

# Base strategy
COPY services/strategies/base_strategy.py /app/

# Strategy code
COPY services/strategies/adaptive-dca /app/strategy

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "strategy.main"]
