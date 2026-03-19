FROM python:3.10.14-slim

LABEL maintainer="Zipstack Inc."

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1
# Set to immediately flush stdout and stderr streams without first buffering
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH /backend

ENV BUILD_CONTEXT_PATH backend
ENV BUILD_PACKAGES_PATH visitran
ENV DJANGO_SETTINGS_MODULE "backend.server.settings.dev"
ENV UV_VERSION 0.5.11

# Disable all telemetry by default
ENV OTEL_TRACES_EXPORTER none
ENV OTEL_METRICS_EXPORTER none
ENV OTEL_LOGS_EXPORTER none
ENV OTEL_SERVICE_NAME visitran_backend

# Install system dependencies and uv in a single layer
RUN apt-get update && \
    apt-get --no-install-recommends install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/* && \
    pip install --no-cache-dir -U pip uv==${UV_VERSION}

WORKDIR /app

# Copy local dependency packages first (changes less frequently)
COPY ${BUILD_PACKAGES_PATH}/ /visitran

# Copy only dependency files first for better caching
COPY ${BUILD_CONTEXT_PATH}/pyproject.toml ${BUILD_CONTEXT_PATH}/uv.lock ./

# Create venv and install dependencies (this layer will be cached unless dependencies change)
# Using uv sync for dependency installation
RUN set -e && \
    rm -rf .venv .pdm* .python* requirements.txt 2>/dev/null || true && \
    uv venv && \
    . .venv/bin/activate && \
    uv sync --frozen --no-dev --no-editable --group deploy

# Install OpenTelemetry in a separate layer (can be cached independently)
RUN . .venv/bin/activate && \
    pip install --no-cache-dir opentelemetry-distro opentelemetry-exporter-otlp && \
    opentelemetry-bootstrap -a install

# Copy application code last (changes most frequently)
COPY ${BUILD_CONTEXT_PATH}/ .

EXPOSE 8000

ENTRYPOINT [ "./entrypoint.sh" ]
