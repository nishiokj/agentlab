# Base image for benchmark containers
# Pinned digest for reproducibility
FROM python:3.11.8-slim-bookworm

# Determinism defaults
ENV PYTHONHASHSEED=0 \
    TZ=UTC \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    SOURCE_DATE_EPOCH=1700000000 \
    PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    patch \
    ripgrep \
    && rm -rf /var/lib/apt/lists/*

# Create workspace and home directories
RUN mkdir -p /workspace /tmp/benchhome

WORKDIR /workspace
