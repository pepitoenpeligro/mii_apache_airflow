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
    dag_id='aadownloaddata',
    default_args = default_args,
    description = 'descargadedatos',
    dagrun_timeout=timedelta(minutes=2),
    schedule_interval=timedelta(days=1),
)

CreateDir = BashOperator(
    task_id='create_dir',
    depends_on_past=False,
    bash_command='mkdir -p /tmp/airflow/p2/',
    dag=dag
)


DownloadTemperatureData = BashOperator(
    task_id='download_temperature_data',
    depends_on_past=False,
    bash_command='curl -o /tmp/airflow/p2/temperature.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/temperature.csv.zip',
    dag=dag
)


DownloadHumidityData = BashOperator(
    task_id='download_humidity_data',
    depends_on_past=False,
    bash_command='curl -o /tmp/airflow/p2/humidity.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/humidity.csv.zip',
    dag=dag
)

CreateDir >> [DownloadTemperatureData, DownloadHumidityData]