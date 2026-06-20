FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN useradd -u 10001 -r -s /usr/sbin/nologin app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app.py .

USER app
EXPOSE 8930

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; r=urllib.request.urlopen('http://127.0.0.1:8930/healthz',timeout=3); sys.exit(0 if r.status==200 else 1)" || exit 1

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8930", "--workers", "2", "--access-log"]
