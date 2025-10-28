# TPCDS Airflow Spark ETL Pipeline

An end-to-end **data engineering pipeline** built using **Apache Airflow**, **Apache Spark**, **PostgreSQL**, **MinIO**, and **Apache Superset**.  
The project simulates a real-world data workflow using the **TPC-DS synthetic dataset**, showcasing orchestration, transformation, and visualization on a modern open-source stack.

---

## Overview

This project demonstrates a **fully automated ETL pipeline** that:
1. **Extracts** synthetic TPC-DS data from a PostgreSQL database.
2. **Stores** the extracted data in **MinIO** (S3-compatible object storage) under the `/RAW` directory.
3. **Transforms** the data using **Apache Spark**, converting it into optimized **Parquet** format.
4. **Loads** the transformed data back into **MinIO** under the `/ANALYTICS` directory.
5. **Visualizes** the results using **DuckDB + Apache Superset** dashboards.

All tasks are **orchestrated via Apache Airflow** (Astro CLI) to enable scheduling, logging, and reproducibility.

---

## Architecture

```text
         ┌─────────────┐
         │  PostgreSQL │
         └──────┬──────┘
                │
         (Extract - Airflow Task: SqlAlchemy)
                │
                ▼
          ┌──────────┐
          │   MinIO  │
          │  RAW Zone│
          └────┬─────┘
               │
       (Transform - Airflow Task: Spark)
               │
               ▼
          ┌──────────┐
          │   MinIO  │
          │Analytics │
          └────┬─────┘
               │
         (Query - DuckDB)
               │
               ▼
          ┌────────────┐
          │ Superset   │
          │ Dashboard  │
          └────────────┘
```

## Tech Stack

| Component                      | Purpose                                           |
| ------------------------------ | ------------------------------------------------- |
| **Apache Airflow (Astro CLI)** | Pipeline orchestration & scheduling               |
| **Apache Spark**               | Distributed data processing & transformation      |
| **PostgreSQL**                 | Source database for TPC-DS synthetic data         |
| **MinIO**                      | S3-compatible object storage for data lake layers |
| **DuckDB**                     | Query engine for analytical queries               |
| **Apache Superset**            | Business intelligence & data visualization        |
| **Docker / Docker Compose**    | Containerized environment for all services        |


## Project Structure
```text
tpcds-airflow-spark-etl-pipeline/
.
├── README.md
├── airflow
│   ├── dags
│   │   ├── load_daily_tables.py
│   │   └── tpcds_etl_dag.py
│   ├── include
│   │   └── config.yaml
│   ├── packages.txt
│   ├── plugins
│   │   └── load_raw_tables.py
│   ├── requirements.txt
│   ├── spark_jobs
│   │   ├── marketing_spark_job.py
│   │   └── sales_spark_job.py
├── data
│   ├── mounts
│   │   └── init_mounts.sh
├── docker-compose.spark.yml
├── docker-compose.superset.yml
├── spark_script
│   ├── init_spark_jars.sh
│   └── spark_jars
└── superset_image
    ├── Dockerfile
    ├── init_duckdb.py
    └── superset_start.sh
```

## Data Flow

| Step | Task                    | Tool               | Output                                |
| ---- | ----------------------- | ------------------ | ------------------------------------- |
| 1    | Extract TPC-DS tables   | Airflow + Postgres | CSV files in `minio/RAW`              |
| 2    | Transform CSV → Parquet | Airflow + Spark    | Parquet files in `minio/ANALYTICS`    |
| 3    | Register dataset        | DuckDB             | Analytical dataset ready for querying |
| 4    | Visualize results       | Superset           | Interactive dashboards                |

## Running the Project

### Prerequisites
- Docker & Docker Compose
- minIO server (or your Data Lake service of choice)
- Astro CLI (follow Astro CLI [docs](https://www.astronomer.io/docs/astro/cli/install-cli))
- Python 3.9+
- Access to the TPC-DS dataset loaded into PostgreSQL

### 1. Clone the Repository
```bash
git clone https://github.com/Matzo77/TPCDS-Airflow-Spark-ETL-Pipeline.git
cd TPCDS-Airflow-Spark-ETL-Pipeline
```
### 2. Start Airflow with Astro CLI and Configture the Network
- first start Airflow by running this command:
```bash
cd airflow
astro dev start
```
- Then run:
```bash
docker network ls
```
and find the name of the network that Airflow is using. You should then go to `docker-compose.superset.yml` and `docker-compose.spark.yml` files, and update the network name.

### 3. Create the required connections in Airflow UI

```text
| ID | Connection ID           | Type     | Host / Endpoint          | Schema | Login / User   | Password     | Port | Extra / Notes                                                                                                                          |
|----|-------------------------|----------|---------------------------|--------|----------------|------------|-------|----------------------------------------------------------------------------------------------------------------------------------------|
| 1  | project_postgres_conn   | Postgres | <Host Address>            | None   | <Username>     | ********   | <Port>| Standard Postgres connection                                                                                                           |
| 2  | minio_default           | AWS      | None (use endpoint_url)   | None   | None           | ********   | None  | Object storage; extra JSON: `{'aws_access_key_id': '***', 'aws_secret_access_key': '***', 'endpoint_url': '<Storage Service Endpoint>` |
| 3  | spark_default           | Spark    | spark://spark-master:7077 | None   | None           | ********   | None  | Spark cluster; extra JSON: `{'spark_home': '/opt/spark', 'spark_binary': '/opt/spark/bin/spark-submit', 'deploy-mode': 'client'}`      |
| 4  | smtp_default            | SMTP     | smtp.gmail.com            | None   | <Email>        | ********   | 587   | Email service; extra JSON: `{'disable_ssl': 'True', 'from_email': '<Email>'}`                                                          |
```
> **Note:**  
> - Replace `<Host Address>`, `<Username>`, `<Password>`, `<Storage Service Endpoint>` and `<Email>` with your actual credentials.  

### 4. Run the Spark Container
```bash
docker-compose -f docker-compose.spark.yml up -d
```

### 5. Run the Data Loding and ETL Dags
In the Airflow UI:
- Trigger load_daily_tables_dag to load raw data into CSV format and dump into /RAW directory
- Trigger tpcds_etl_dag to read the CSV files, transform and save them in Parquet format under /ANALYTICS directory

### 6. Create Visualizations
- First you would have to run Superset with the following command:
```bash
docker-compose -f docker-compose.superset.yml up -d
```
- Then go to `localhost:8088` and create a database connection, as pe below:
  - Database Name: Any name you want
  - SQLAlchemyURL: `duckdb:////duckdb_data/airflow_spark_portfolio.duckdb`

Once this step is done, you can proceed with querying your database and creating visualizations on Superset.

## References
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)  
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)  
- [MinIO Documentation](https://docs.min.io/)  
- [Apache Superset Documentation](https://superset.apache.org/docs/)  
- [TPC-DS Benchmark Info](http://www.tpc.org/tpcds/)  


