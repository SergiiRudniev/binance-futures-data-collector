FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC

WORKDIR /app

RUN addgroup --gid 1000 collector \
    && adduser --uid 1000 --gid 1000 --disabled-password --gecos "" collector

COPY deploy/market_collector/requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/requirements.txt

COPY scripts/collect_binance_microstructure_5s.py /app/scripts/collect_binance_microstructure_5s.py
COPY scripts/run_monthly_market_collector.py /app/scripts/run_monthly_market_collector.py
COPY deploy/market_collector/backup.py /app/backup.py
COPY deploy/market_collector/healthcheck.py /app/healthcheck.py
COPY deploy/market_collector/collector.yaml /app/collector.yaml

USER collector

CMD ["python", "/app/scripts/run_monthly_market_collector.py", "--config", "/app/collector.yaml"]
