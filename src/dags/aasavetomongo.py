from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import pandas
import pymongo

def save_to_mongo():
    # Read from /tmp/airflow/p2/san_francisco.csv
    logger = logging.getLogger("airflow.task")
    dataframe = pandas.read_csv('/tmp/airflow/p2/san_francisco.csv', sep='\t')#,header=None)
    #dataframe.columns = ['DATE','TEMP','HUM']
    #dataframe = dataframe.head()
    print(dataframe)
    logging.info("dataframe from csv readed")
    dataframe_as_dict = dataframe.to_dict('records')
    print(dataframe_as_dict)
    logging.info("dataframe conveted to dictionary")
    client = pymongo.MongoClient(
            "mongodb+srv://"+"pepitoenpeligro"+":"+"QDGzSuG1Wv9QtQ6Z"+"@cluster0.xoro9.mongodb.net/test?retryWrites=true&w=majority")
    logging.info("mongo connected")
    #code = client.p2Airflow['sanfrancisco'].insert_one({'index':'SF', 'datos':dataframe_as_dict}).inserted_id
    code = client.p2Airflow['sanfrancisco'].insert_many(dataframe.to_dict('records'))
    #logging.info("mongo inserted", code)




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
    dag_id='aasavetomongo',
    default_args = default_args,
    description = 'preprocesadodelcsv',
    dagrun_timeout=timedelta(minutes=2),
    schedule_interval=timedelta(days=1),
)


SaveToMongo = PythonOperator(
    task_id='guardadoamongo',
    python_callable=save_to_mongo,
    op_kwargs={},
    provide_context=True,
    dag=dag
)
                    
SaveToMongo