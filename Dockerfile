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

CMD ["python", "-m", "edge_collector"]
