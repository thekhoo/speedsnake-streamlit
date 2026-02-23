FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY app.py ./

ENTRYPOINT ["uv", "run", "streamlit", "run", "app.py", \
    "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
