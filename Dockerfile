# Multi-stage build using uv for fast, reproducible installs
# Stage 1: build dependencies with uv
FROM python:3.11-slim AS builder

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /build
COPY pyproject.toml .

# uv sync with no project code — just installs deps into /build/.venv
RUN uv sync --python 3.11 --no-dev --no-install-project

# Stage 2: runtime image with Spark
FROM bitnami/spark:3.5.0
USER root

# Copy uv-installed Python packages from builder
COPY --from=builder /build/.venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONPATH="/app:${PYTHONPATH}"

WORKDIR /app
COPY jobs/    ./jobs/
COPY config/  ./config/

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8090/health || exit 1

USER spark
