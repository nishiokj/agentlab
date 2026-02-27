# Agent sandbox image
# Built on top of base image; includes tool server
FROM bench-base:dev

# Install benchmark package and tool server dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Copy benchmark code
COPY bench/ /opt/bench/bench/
COPY pyproject.toml /opt/bench/pyproject.toml
RUN pip install --no-cache-dir -e /opt/bench

# Agent container exposes tool server port
EXPOSE 8080

# Default: start tool server
CMD ["python", "-m", "bench.tools.server", "--workspace", "/workspace", "--port", "8080"]
