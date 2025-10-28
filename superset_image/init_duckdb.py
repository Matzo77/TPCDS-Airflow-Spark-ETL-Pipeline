import duckdb
import os

# Path to store your DuckDB database
duckdb_file = "/duckdb_data/airflow_spark_portfolio.duckdb"

# Connect or create DuckDB
conn = duckdb.connect(duckdb_file)

# Load Config file
CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../include/")
with open(f"{CONFIG_DIR}config.yaml", "r") as f:
    cfg = yaml.safe_load(f)

MINIO_CONN_ID = "minio_default"

minio_cfg = cfg["minio"]

# Configure S3/MinIO
conn.execute("INSTALL httpfs;")
conn.execute("LOAD httpfs;")
conn.execute("SET s3_endpoint='shared-minio:9000';")
conn.execute("SET s3_access_key_id='*********';")  # 👈👈👈 CHANGE TO YOUR YOUR DATA LAKE ACCESS KEY AND SECRET KEY
conn.execute("SET s3_secret_access_key='**********';") # 👈👈👈 CHANGE TO YOUR YOUR DATA LAKE ACCESS KEY AND SECRET KEY
conn.execute("SET s3_use_ssl=false;")
conn.execute("SET s3_url_style='path';")

# Create tables of the data to be used in DuckDB

# Sales tables

folders = ['dim_customer_sales', 'dim_date', 'dim_item_sales','dim_warehouse_sales','fact_returns_with_sales','fact_sales_inv_weekly']
for f in folders:
    sql = f"""
    CREATE OR REPLACE TABLE {f}_table AS
    SELECT *
    FROM read_parquet('s3://portfolio-project/analytics/sales/{f}/*.parquet');
    """
    conn.execute(sql)

# Marketing tables

folders = ['dim_customer_marketing', 'rpt_clv_marketing', 'fact_sales_marketing_daily']
for f in folders:
    sql = f"""
    CREATE OR REPLACE TABLE {f}_table AS
    SELECT *
    FROM read_parquet('s3://portfolio-project/analytics/marketing/{f}/*.parquet');
    """
    conn.execute(sql)

# DuckDB is now ready to query MinIO files
print("DuckDB initialized and connected to MinIO.")