FROM python:3.12-slim

WORKDIR /app

# System deps for asyncpg
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Make shared package available
COPY --from=shared /app/shared /app/shared

CMD ["python", "main.py"]
