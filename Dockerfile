# Multi-stage build — uv installs deps, bitnami/spark is the runtime
FROM python:3.11-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /build
COPY pyproject.toml .
RUN uv sync --python 3.11 --no-dev --no-install-project

FROM bitnami/spark:3.5.0
USER root
COPY --from=builder /build/.venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONPATH="/app:${PYTHONPATH}"
WORKDIR /app
COPY jobs/    ./jobs/
COPY config/  ./config/
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8090/health || exit 1
USER spark
