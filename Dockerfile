FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DRAIN_DB_PATH=/data/drain.db \
    DRAIN_WORKERS=2

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Run unprivileged; /data is the only writable location the app needs.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin drain \
    && mkdir -p /data \
    && chown -R drain:drain /data /srv
USER 10001

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=2)"

CMD ["python", "-m", "app.main"]
