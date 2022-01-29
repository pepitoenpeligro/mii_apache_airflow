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

def extract_zip_service_v1():
    from zipfile import ZipFile
    file='/tmp/airflow/p2/cc2_weatherpredictor_v1.zip'
    with ZipFile(file, 'r') as zip:
        zip.extractall('/tmp/airflow/p2/')
        print(file,' now is extracted')

def extract_zip_service_v2():
    from zipfile import ZipFile
    file='/tmp/airflow/p2/cc2_weatherpredictor_v2.zip'
    with ZipFile(file, 'r') as zip:
        zip.extractall('/tmp/airflow/p2/')
        print(file,' now is extracted')

def extract_zip_service_v3():
    from zipfile import ZipFile
    file='/tmp/airflow/p2/cc2_weatherpredictor_v3.zip'
    with ZipFile(file, 'r') as zip:
        zip.extractall('/tmp/airflow/p2/')
        print(file,' now is extracted')


dag = DAG(
    dag_id='aadownloadservices',
    default_args = default_args,
    description = 'descargaservicios',
    dagrun_timeout=timedelta(minutes=2),
    schedule_interval=timedelta(days=1),
)


DownloadService1 = BashOperator(
    task_id='download_service_v1',
    depends_on_past=False,
    bash_command='rm -f /tmp/airflow/p2/cc2_weatherpredictor_v1.zip; curl -o  /tmp/airflow/p2/cc2_weatherpredictor_v1.zip -LJ https://github.com/pepitoenpeligro/cc2_weatherpredictor_v1/archive/refs/heads/master.zip',
    dag=dag
)


DownloadService2 = BashOperator(
    task_id='download_service_v2',
    depends_on_past=False,
    bash_command='rm -f /tmp/airflow/p2/cc2_weatherpredictor_v2.zip; curl -o  /tmp/airflow/p2/cc2_weatherpredictor_v2.zip -LJ https://github.com/pepitoenpeligro/cc2_weatherpredictor_v2/archive/refs/heads/master.zip',
    dag=dag
)

DownloadService3 = BashOperator(
    task_id='download_service_v3',
    depends_on_past=False,
    bash_command='rm -f /tmp/airflow/p2/cc2_weatherpredictor_v3.zip; curl -o  /tmp/airflow/p2/cc2_weatherpredictor_v3.zip -LJ https://github.com/pepitoenpeligro/cc2_weatherpredictor_v3/archive/refs/heads/master.zip',
    dag=dag
)

# https://github.com/pepitoenpeligro/cc2_weatherpredictor_v1/blob/master/modelos/model__temp.p\?raw\=true
# https://github.com/pepitoenpeligro/cc2_weatherpredictor_v1/blob/master/modelos/model__hum.p\?raw\=true


ExtractService1 = PythonOperator(
    task_id='extract_zip_v1',
    python_callable=extract_zip_service_v1,
    op_kwargs={},
    provide_context=True,
    dag=dag
)


ExtractService2 = PythonOperator(
    task_id='extract_zip_v2',
    python_callable=extract_zip_service_v2,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

ExtractService3 = PythonOperator(
    task_id='extract_zip_v3',
    python_callable=extract_zip_service_v3,
    op_kwargs={},
    provide_context=True,
    dag=dag
)



DownloadService1 >> ExtractService1 >> DownloadService2 >> ExtractService2 >> DownloadService3 >> ExtractService3