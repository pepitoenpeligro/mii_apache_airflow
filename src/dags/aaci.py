from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta



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
    dag_id='aaci',
    default_args = default_args,
    description = 'testearservicio1',
    dagrun_timeout=timedelta(minutes=2),
    schedule_interval=timedelta(days=1),
)



TestServiceV1 = BashOperator(
    task_id='testservicev1',
    depends_on_past=False,
    bash_command='cd /tmp/airflow/p2/cc2_weatherpredictor_v1-master/ && python -m pytest tests.py',
    dag=dag
)

TestServiceV1