from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

def check_packages():
    import sys
    packages= ['pymongo', 'pytest', 'pandas', 'urllib']
    logger = logging.getLogger("airflow.task")
    for package in packages:
        try:
            exec("from {module} import *".format(module=package))
            logging.info(package, "OK, is installed ")
        except Exception as e:
            logging.info(package, "is not installed")
            logging.error(e)


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
    dag_id='aatestpackages',
    default_args = default_args,
    description = 'comprobaciondepaquetes',
    dagrun_timeout=timedelta(minutes=2),
    schedule_interval=timedelta(days=1),
)


CheckPackages = PythonOperator(
                    task_id='install_packages',
                    python_callable=check_packages,
                    op_kwargs={},
                    provide_context=True,
                    dag=dag)
                    
CheckPackages