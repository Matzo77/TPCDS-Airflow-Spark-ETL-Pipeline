#!/bin/bash
set -e

echo "🔄 Waiting for Postgres to be ready..."
until pg_isready -h superset-db -p 5432 -U superset; do
  echo "⏳ Waiting for Postgres..."
  sleep 2
done
echo "✅ Postgres is ready!"

# --- Activate Superset Python venv ---
if [ -f /app/.venv/bin/activate ]; then
    echo "⚡ Activating Superset venv..."
    source /app/.venv/bin/activate   # Bash supports 'source'
else
    echo "❌ Superset venv not found at /app/.venv"
fi

# --- Initialize / upgrade Superset metadata DB ---
echo "🚀 Upgrading Superset metadata database..."
superset db upgrade

# --- Create admin user if not exists ---
echo "👤 Ensuring admin user exists..."
superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@superset.com \
    --password admin || true

# --- Initialize Superset ---
echo "⚙️  Initializing Superset..."
superset init

# --- Initialize DuckDB ---
echo "🦆 Initializing DuckDB..."
if [ -f /app/scripts/init_duckdb.py ]; then
    echo "📂 Running /app/scripts/init_duckdb.py ..."
    python /app/scripts/init_duckdb.py || echo "⚠️ DuckDB initialization script failed, continuing..."
else
    echo "❌ init_duckdb.py not found in /app/scripts!"
fi

# --- Start Superset server ---
echo "🚀 Starting Superset server on port 8088..."
superset run -h 0.0.0.0 -p 8088
