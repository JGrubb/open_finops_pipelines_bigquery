# Multi-stage build for Cloud Run job
# Uses uv for fast dependency installation

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --no-dev --frozen

# Runtime stage
FROM python:3.13-slim-bookworm

WORKDIR /app

# Copy dependencies from builder
COPY --from=builder /app/.venv /app/.venv

# Install uv in runtime
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy application code
COPY pyproject.toml ./
COPY pipelines/ ./pipelines/
COPY main.py ./
COPY .dlt/ ./.dlt/

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Default runs all pipelines - override with CMD ["uv", "run", "python", "main.py", "aws"]
CMD ["uv", "run", "python", "main.py", "all"]
