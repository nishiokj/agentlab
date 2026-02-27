#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Building benchmark Docker images..."

# Build base image
echo "==> Building bench-base:dev"
docker build \
    -f "$ROOT_DIR/bench/docker/base.Dockerfile" \
    -t bench-base:dev \
    "$ROOT_DIR"

# Build agent image
echo "==> Building bench-agent:dev"
docker build \
    -f "$ROOT_DIR/bench/docker/agent.Dockerfile" \
    -t bench-agent:dev \
    "$ROOT_DIR"

# Build grader image
echo "==> Building bench-grader:dev"
docker build \
    -f "$ROOT_DIR/bench/docker/grader.Dockerfile" \
    -t bench-grader:dev \
    "$ROOT_DIR"

echo "All images built successfully."
