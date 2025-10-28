from airflow.sdk import dag, task
from datetime import datetime, timedelta
from load_raw_tables import export_all_tables_to_minio

default_args = {
    'owner': 'matin',
    'start_date': datetime(2025, 10, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    schedule='0 6 * * *',  # daily at 6 AM UTC (~2 AM EST/EDT)
    catchup=False,
    tags=['tpcds', 'minio'],
)
def load_daily_tables_dag():

    @task
    def load_tables_task():
        """
        Calls the standalone Python script to export tables to MinIO.
        """
        export_all_tables_to_minio()

    load_tables_task()

load_daily_tables_dag()
