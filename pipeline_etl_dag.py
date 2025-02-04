from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Adicionar caminho do pipeline ao PYTHONPATH
sys.path.append(str(Path(__file__).parent.parent))
from pipeline_airflow import NorthwindETL

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_postgres(**context):
    execution_date = context['ds']
    etl = NorthwindETL(
        postgres_conn_string="postgresql://user:password@localhost:5432/northwind",
        csv_path="order_details.csv",
        execution_date=execution_date
    )
    if not etl.extract_from_postgres():
        raise Exception("Falha na extração do Postgres")

def extract_csv(**context):
    execution_date = context['ds']
    etl = NorthwindETL(
        postgres_conn_string="postgresql://user:password@localhost:5432/northwind",
        csv_path="order_details.csv",
        execution_date=execution_date
    )
    if not etl.extract_from_csv():
        raise Exception("Falha na extração do CSV")

def load_data(**context):
    execution_date = context['ds']
    etl = NorthwindETL(
        postgres_conn_string="postgresql://user:password@localhost:5432/northwind",
        csv_path="order_details.csv",
        execution_date=execution_date
    )
    if not etl.load_to_postgres():
        raise Exception("Falha no carregamento dos dados")
    etl.export_results()

with DAG(
    'northwind_etl',
    default_args=default_args,
    description='Pipeline ETL Northwind',
    schedule_interval='0 0 * * *',  # Executa diariamente à meia-noite
    start_date=datetime(2024, 1, 1),
    catchup=True  # Permite execução de datas anteriores
) as dag:

    extract_postgres_task = PythonOperator(
        task_id='extract_postgres',
        python_callable=extract_postgres
    )

    extract_csv_task = PythonOperator(
        task_id='extract_csv',
        python_callable=extract_csv
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    [extract_postgres_task, extract_csv_task] >> load_task