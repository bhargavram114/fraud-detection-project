# Multi-stage build — uv installs deps, bitnamilegacy/spark is the runtime.
#
# WHY bitnamilegacy AND NOT bitnami:
#   Since Aug 2025 Bitnami moved versioned tags (e.g. spark:3.5.0) from
#   docker.io/bitnami to docker.io/bitnamilegacy. The legacy repo gets no
#   updates or security patches. It is the same image family docker-compose*.yml
#   uses, so local and container builds now match. For a real production image
#   consider the official apache/spark image instead.
FROM python:3.11-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /build
COPY pyproject.toml .
RUN uv sync --python 3.11 --no-dev --no-install-project

FROM bitnamilegacy/spark:3.5.0
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
