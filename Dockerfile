FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY edge_collector ./edge_collector

ENV EDGE_STATE_DIR=/data
VOLUME ["/data"]

RUN useradd -M -u 1000 edge && mkdir -p /data && chown -R edge /app /data
USER edge

EXPOSE 8000

# Dung /healthz co san (app.py) thay vi cai them curl/wget vao image - python
# da co san trong base image nay. Kiem tra dung port dang chay (doc lai
# EDGE_LISTEN_PORT, mac dinh 8000) vi Nam co the doi port qua /setup - xem
# docker-reviewer 2026-09-17.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import os,sys,urllib.request as u; \
    sys.exit(0 if u.urlopen('http://127.0.0.1:' + os.environ.get('EDGE_LISTEN_PORT', '8000') + '/healthz', timeout=3).status == 200 else 1)"

CMD ["python", "-m", "edge_collector"]
