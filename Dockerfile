FROM python:3.11.10-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17

ENV JAVA_HOME=/usr/lib/jvm/java-17 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    PYSPARK_PYTHON=python

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[dev]"
