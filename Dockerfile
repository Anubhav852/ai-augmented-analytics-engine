# =====================================================================
# STAGE 1: Compilation & Dependency Assembly
# =====================================================================
FROM python:3.11-slim AS builder

WORKDIR /build

# Avoid writing python bytecode (.pyc) files to disk to optimize space
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install building essentials for compiled python dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies to a local user directory structure
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt


# =====================================================================
# STAGE 2: Optimized Enterprise Production Runtime
# =====================================================================
FROM python:3.11-slim AS runner

WORKDIR /workspace

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copy only the compiled python package wheels from the builder layer
COPY --from=builder /root/.local /root/.local
COPY app/ ./app/

# Bridge the compiled user-space libraries directly into the python system path
ENV PATH=/root/.local/bin:$PATH

# Expose the network ingestion layer port
EXPOSE 8000

# Default command launches our high-throughput FastAPI Ingestion Gateway
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]