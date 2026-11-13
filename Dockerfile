FROM python:3.12.13-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.11.19 /uv /usr/local/bin/uv
ENV PYTHONUNBUFFERED=1 UV_PYTHON_DOWNLOADS=never PATH="/app/.venv/bin:$PATH"
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable
RUN mkdir -p /var/lib/bridge /run/bridge-controls && chown -R 10001:10001 /var/lib/bridge /run/bridge-controls
USER 10001:10001
