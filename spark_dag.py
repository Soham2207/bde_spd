from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from spark_job import run_spark

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,5,13),
    'email': ['soham.rane2207@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spark_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='complete_spark',
    python_callable=run_spark,
    dag=dag, 
)

run_etl