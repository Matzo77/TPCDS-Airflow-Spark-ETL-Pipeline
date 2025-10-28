#!/bin/sh
# Creates Superset, DuckDB, and Postgres folders and sets permissions
# DO NOT FORGET TO RUN "chmod +x init_mounts.sh" BEFORE BUILDING THE .yml FILE

HOST_PATH="/mnt/"

mkdir -p "$HOST_PATH/superset/home"
mkdir -p "$HOST_PATH/superset/postgres-data"
mkdir -p "$HOST_PATH/duckdb"

chown -R 1000:1000 "$HOST_PATH/superset/home"
chown -R 999:999 "$HOST_PATH/superset/postgres-data"
chown -R 1000:1000 "$HOST_PATH/duckdb"

echo "Superset and DuckDB folders created and permissions set."