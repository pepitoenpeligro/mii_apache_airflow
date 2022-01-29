from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import pandas

def preprocessing():
    
    temperature_frame   = pandas.read_csv('/tmp/airflow/p2/temperature.csv')
    humidity_frame      = pandas.read_csv('/tmp/airflow/p2/humidity.csv')
    

    humedad_sf          = humidity_frame['San Francisco']
    temperatura_sf      = temperature_frame['San Francisco']

    datetime            = humidity_frame['datetime']

    columns_names = {'DATE':datetime, 'TEMP':temperatura_sf, 'HUM':humedad_sf}
    dataframe = pandas.DataFrame(data=columns_names)
    dataframe.fillna(dataframe.mean())
    dataframe.to_csv('/tmp/airflow/p2/san_francisco.csv', sep='\t', encoding='utf-8', index=False)

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
    dag_id='aapreprocessing',
    default_args = default_args,
    description = 'preprocesadodelcsv',
    dagrun_timeout=timedelta(minutes=2),
    schedule_interval=timedelta(days=1),
)


Preprocessing = PythonOperator(
    task_id='extract_zip',
    python_callable=preprocessing,
    op_kwargs={},
    provide_context=True,
    dag=dag
)
                    
Preprocessing