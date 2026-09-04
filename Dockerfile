FROM python:3.11-slim

# Install system dependencies
RUN echo "deb http://deb.debian.org/debian bookworm main contrib non-free non-free-firmware" > /etc/apt/sources.list && \
    apt-get update && apt-get install -y --no-install-recommends \
    p7zip-full \
    unrar \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install --no-cache-dir uv

WORKDIR /app

# Copy dependency files first for better layer caching
COPY pyproject.toml uv.lock ./

# Install project dependencies (no dev extras). UV cache would otherwise be
# baked into the image (~100-300 MB of wheels); the cache is useless at
# runtime.
RUN uv sync --no-dev --frozen --no-cache

# Copy the rest of the project
COPY telecrime/ ./telecrime/
COPY alembic/ ./alembic/
COPY alembic.ini ./

# Create data directory
RUN mkdir -p /app/data

# Default command: run the dashboard
CMD ["uv", "run", "python", "-m", "telecrime", "dashboard", "--host", "0.0.0.0", "--port", "8000"]
