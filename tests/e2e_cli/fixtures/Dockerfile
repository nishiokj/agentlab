FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        git \
        patch \
        zstd \
    && rm -rf /var/lib/apt/lists/*

COPY bench-requirements.txt /tmp/bench-requirements.txt

RUN pip install --no-cache-dir -r /tmp/bench-requirements.txt

WORKDIR /agentlab/workspace
