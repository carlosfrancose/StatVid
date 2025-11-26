FROM python:3.11-slim

WORKDIR /app

# System deps (optional for pyarrow/lightgbm optimizations)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY README.md ./

ENV PYTHONPATH=/app/src
ENV DATA_DIR=/app/data
ENV LOG_LEVEL=INFO

CMD ["python", "-m", "statvid", "--help"]

