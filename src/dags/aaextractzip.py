from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

def extract_zip_temperature():
    from zipfile import ZipFile
    logger = logging.getLogger("airflow.task")
    with ZipFile('/tmp/airflow/p2/temperature.csv.zip', 'r') as zip:
        zip.extractall('/tmp/airflow/p2/')
        print('/tmp/airflow/p2/temperature.csv.zip now is extracted')

def extract_zip_humidity():
    from zipfile import ZipFile
    logger = logging.getLogger("airflow.task")
    with ZipFile('/tmp/airflow/p2/humidity.csv.zip', 'r') as zip:
        zip.extractall('/tmp/airflow/p2/')
        print('/tmp/airflow/p2/humidity.csv.zip now is extracted')


def extract_zip():
    extract_zip_temperature()
    extract_zip_humidity()


default_args = {
    'owner': 'airflow', # Lo ejecuta el usuario airflow
    'depends_on_past': False,
    'start_date': days_ago(2), # Comienzo inmediato
    'email': ['joseinn@correo.ugr.es'], # Email al que enviar el informe si hay error.
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}


dag = DAG(
    dag_id='aaextractzip',
    default_args = default_args,
    description = 'comprobaciondepaquetes',
    dagrun_timeout=timedelta(minutes=2),
    schedule_interval=timedelta(days=1),
)


ExtractZip = PythonOperator(
                    task_id='extract_zip',
                    python_callable=extract_zip,
                    op_kwargs={},
                    provide_context=True,
                    dag=dag)
                    
ExtractZip