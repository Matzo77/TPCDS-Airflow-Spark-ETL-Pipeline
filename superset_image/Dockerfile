# --------------------------------------------------------------------------------
# Custom superset image used in `docker-compose.superset.yml` file
# --------------------------------------------------------------------------------

FROM apache/superset:latest

# Environment variables
ENV SUPERSET_LOAD_EXAMPLES=no
ENV SUPERSET_SECRET_KEY=supersecretkey

# Switch to root user
USER root

# Install OS dependencies
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client && rm -rf /var/lib/apt/lists/*

# Ensure pip exists in Superset venv
RUN /app/.venv/bin/python -m ensurepip --upgrade && \
    /app/.venv/bin/python -m pip install --no-cache-dir --upgrade pip && \
    /app/.venv/bin/python -m pip install --no-cache-dir duckdb duckdb-engine boto3

# Copy custom start script
COPY superset_image/superset_start.sh /app/scripts/superset_start.sh
COPY superset_image/init_duckdb.py /app/scripts/init_duckdb.py
RUN chmod +x /app/scripts/superset_start.sh

# Switch back to the default user
USER superset

CMD ["bash", "/app/scripts/superset_start.sh"]