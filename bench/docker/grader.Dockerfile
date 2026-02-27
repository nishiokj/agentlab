# Grader sandbox image
# Built on top of base image; includes hidden runner and grading harness
FROM bench-base:dev

# Install benchmark package
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Install coverage for optional coverage collection
RUN pip install --no-cache-dir coverage>=7.3

# Copy benchmark code
COPY bench/ /opt/bench/bench/
COPY pyproject.toml /opt/bench/pyproject.toml
RUN pip install --no-cache-dir -e /opt/bench

# Copy schemas for validation
COPY schemas/ /opt/bench/schemas/

# Grader runs with no network; default command is grade
CMD ["python", "-m", "bench.cli", "grade"]
