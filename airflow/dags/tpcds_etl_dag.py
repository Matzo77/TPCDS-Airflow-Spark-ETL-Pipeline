from airflow.sdk import dag, task, task_group
from datetime import date, datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
import time
import os
import pendulum
import yaml

# --- config ---
CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../include/")
with open(f"{CONFIG_DIR}config.yaml", "r") as f:
    cfg = yaml.safe_load(f)

MINIO_CONN_ID = "minio_default"

minio_cfg = cfg["minio"]

minio_endpoint = minio_cfg["endpoint"]
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
bucket_name = minio_cfg["bucket"]

REQUIRED_FILES = [
    "catalog_returns_{date}.csv",
    "catalog_sales_{date}.csv",
    "customer_address_{date}.csv",
    "customer_{date}.csv",
    "customer_demographics_{date}.csv",
    "date_dim_{date}.csv",
    "household_demographics_{date}.csv",
    "income_band_{date}.csv",
    "inventory_{date}.csv",
    "item_{date}.csv",
    "store_returns_{date}.csv",
    "store_sales_{date}.csv",
    "warehouse_{date}.csv",
    "web_returns_{date}.csv",
    "web_sales_{date}.csv"
]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SPARK_JOBS_DIR = os.path.join(BASE_DIR, "../spark_jobs/")

SPARK_ARGS = [
    minio_endpoint,
    minio_access_key,
    minio_secret_key
]
JARS = "/opt/spark/jars/hadoop-aws-3.4.0.jar"
PACKAGES = "org.apache.hadoop:hadoop-aws:3.4.0"

# --- DAG ---
@dag(
    dag_id="tpcds_etl_dag",
    start_date=datetime(2025, 10, 22),
    schedule='0 10 * * *',  # daily at 10 AM UTC / 6 AM EDT
    catchup=False
)
def tpcds_etl_dag():

    @task
    def check_files_task():
        hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

        prefix = f"raw/"

        local_tz = pendulum.timezone("America/New_York")
        datestr = datetime.now(tz=local_tz).strftime("%Y-%m-%d")

        expected_files = [prefix + f.format(date=datestr) for f in REQUIRED_FILES]

        existing_files = hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        if not existing_files:
            raise ValueError(f"No files found in bucket path {prefix}")
        
        missing = [f for f in expected_files if f not in existing_files]
        if missing:
            raise ValueError(f"Missing required files: {missing}")
        
        file_urls = [f"s3a://{bucket_name}/{f}" for f in expected_files]
        return file_urls

    @task_group(group_id="spark_jobs_group")
    def spark_jobs_group():
        """Run a sequence of Spark ETL jobs"""

        spark_job_1 = SparkSubmitOperator(
            task_id="sales_spark_job",
            application=os.path.join(SPARK_JOBS_DIR,"sales_spark_job.py"),
            conn_id="spark_default",
            packages=PACKAGES,
            application_args = [
                "{{ ti.xcom_pull(task_ids='check_files_task') }}",
                *SPARK_ARGS,
            ],
            verbose=True,
            jars=JARS
        )

        spark_job_2 = SparkSubmitOperator(
            task_id="marketing_spark_job",
            application=os.path.join(SPARK_JOBS_DIR,"marketing_spark_job.py"),
            conn_id="spark_default",
            packages=PACKAGES,
            application_args = [
                "{{ ti.xcom_pull(task_ids='check_files_task') }}",
                *SPARK_ARGS,
            ],
            verbose=True,
            jars=JARS
        )

        spark_job_1 >> spark_job_2

    # --- Success Email Notification ---
    email_success = EmailOperator(
        task_id="email_success",
        to=Variable.get("support_email"),
        subject="AIRFLOW NOTIFICATION: Portfolio DAG Success",
        html_content="<p>All Spark jobs completed successfully ✅</p>",
    )

    # --- Orchestartion ---
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> check_files_task() >> spark_jobs_group() >> email_success >> end


tpcds_etl_dag()

    