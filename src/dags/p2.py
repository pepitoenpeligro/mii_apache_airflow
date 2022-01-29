from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import pandas
import pymongo

default_args = {
    'owner': 'airflow', # dag owner
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['joseinn@correo.ugr.es'], # Email al que enviar el informe si hay error.
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 7,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='00_practica2',
    default_args = default_args,
    description = 'Flujo de tares de la p2',
    dagrun_timeout=timedelta(minutes=55), # specify how long a DagRun should be up before timing out / failing, so that new DagRuns can be created. The timeout is only enforced for scheduled DagRuns
    schedule_interval=timedelta(days=1), # Defines how often that DAG runs, this timedelta object gets added to your latest task instance's execution_date to figure out the next schedule
)


## Fase 0
# Check if all packages needed are installed
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

# Tarea 0 
CheckPackages = PythonOperator(
    task_id='checkPackages',
    python_callable=check_packages,
    op_kwargs={},
    provide_context=True,
    dag=dag
)


# Tarea 1
# Crear directorio donde guardar los datos
# Descargar los datos de temperatura
# Descargar los datos de humedad
CreateDir = BashOperator(
    task_id='CreateTMPDir',
    depends_on_past=False,
    bash_command='mkdir -p /tmp/airflow/p2/',
    dag=dag
)


DownloadTemperatureData = BashOperator(
    task_id='getTempCSV',
    depends_on_past=False,
    bash_command='curl -o /tmp/airflow/p2/temperature.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/temperature.csv.zip',
    dag=dag
)


DownloadHumidityData = BashOperator(
    task_id='getHumCSV',
    depends_on_past=False,
    bash_command='curl -o /tmp/airflow/p2/humidity.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/humidity.csv.zip',
    dag=dag
)


# Tarea 2
# Descomprimir los ficheros zip descargados en Tarea 1
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
    
ExtractZip = PythonOperator(
    task_id='unzipCSV',
    python_callable=extract_zip,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# Tarea 3
# Preprocesar los ficheros de temperatura y humedad uniéndolos en un único fichero con el formato
# DATE; TEMP; HUM
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

Preprocessing = PythonOperator(
    task_id='EncodeCSV',
    python_callable=preprocessing,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# Tarea 4
# Guardar el contenido del fichero preprocesado en mongodb dentro de la db p2Airflow
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

SaveToMongo = PythonOperator(
    task_id='saveToMongoAtlas',
    python_callable=save_to_mongo,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# Tarea 5
# Descargar los microservicios desarrollados para el consumo de los datos anteriores

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

DownloadService1 = BashOperator(
    task_id='getServiceV1',
    depends_on_past=False,
    bash_command='rm -f /tmp/airflow/p2/cc2_weatherpredictor_v1.zip; curl -o  /tmp/airflow/p2/cc2_weatherpredictor_v1.zip -LJ https://github.com/pepitoenpeligro/cc2_weatherpredictor_v1/archive/refs/heads/master.zip',
    dag=dag
)


DownloadService2 = BashOperator(
    task_id='getServiceV2',
    depends_on_past=False,
    bash_command='rm -f /tmp/airflow/p2/cc2_weatherpredictor_v2.zip; curl -o  /tmp/airflow/p2/cc2_weatherpredictor_v2.zip -LJ https://github.com/pepitoenpeligro/cc2_weatherpredictor_v2/archive/refs/heads/master.zip',
    dag=dag
)

DownloadService3 = BashOperator(
    task_id='getServiceV3',
    depends_on_past=False,
    bash_command='rm -f /tmp/airflow/p2/cc2_weatherpredictor_v3.zip; curl -o  /tmp/airflow/p2/cc2_weatherpredictor_v3.zip -LJ https://github.com/pepitoenpeligro/cc2_weatherpredictor_v3/archive/refs/heads/master.zip',
    dag=dag
)
ExtractService1 = PythonOperator(
    task_id='unzipServiceV1',
    python_callable=extract_zip_service_v1,
    op_kwargs={},
    provide_context=True,
    dag=dag
)


ExtractService2 = PythonOperator(
    task_id='unzipServiceV2',
    python_callable=extract_zip_service_v2,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

ExtractService3 = PythonOperator(
    task_id='unzipServiceV3',
    python_callable=extract_zip_service_v3,
    op_kwargs={},
    provide_context=True,
    dag=dag
)


# Tarea 6
# Testear con preubas de unidad 

TestServiceV1 = BashOperator(
    task_id='testServiceV1',
    depends_on_past=False,
    bash_command='cd /tmp/airflow/p2/cc2_weatherpredictor_v1-master/ && python -m pytest tests.py',
    dag=dag
)

TestServiceV2 = BashOperator(
    task_id='testServiceV2',
    depends_on_past=False,
    bash_command='cd /tmp/airflow/p2/cc2_weatherpredictor_v2-master/ && python -m pytest tests.py',
    dag=dag
)

TestServiceV3 = BashOperator(
    task_id='testServiceV3',
    depends_on_past=False,
    bash_command='cd /tmp/airflow/p2/cc2_weatherpredictor_v3-master/ && python -m pytest tests.py',
    dag=dag
)


# Tarea 7
# Enviar los microservicios a la máquina de despligue y lanzarlos a ejecución permanente con docker

CopiarFuentesV1 = BashOperator(
    task_id='sendServiceV1',
    depends_on_past=False,
    bash_command='scp  -r /tmp/airflow/p2/cc2_weatherpredictor_v1-master pepe@157.90.224.208:~',
    dag=dag
)

GenerarContenedorV1 = BashOperator(
    task_id='generateContainerV1',
    depends_on_past=False,
    bash_command='ssh pepe@157.90.224.208 "cd cc2_weatherpredictor_v1-master && docker build --no-cache -t pepitoenpeligro/cc2_weatherpredictor_v1 -f Dockerfile . "',
    dag=dag
)

DesplegarContenedorV1 = BashOperator(
    task_id='deployServiceV1',
    depends_on_past=False,
    bash_command='ssh pepe@157.90.224.208 "docker run -p 3005:3005 --name service1 -d pepitoenpeligro/cc2_weatherpredictor_v1" ',
    dag=dag
)



CopiarFuentesV2 = BashOperator(
    task_id='sendServiceV2',
    depends_on_past=False,
    bash_command='scp  -r /tmp/airflow/p2/cc2_weatherpredictor_v2-master pepe@157.90.224.208:~',
    dag=dag
)

GenerarContenedorV2 = BashOperator(
    task_id='generateContainerV2',
    depends_on_past=False,
    bash_command='ssh pepe@157.90.224.208 "cd cc2_weatherpredictor_v2-master && docker build --no-cache -t pepitoenpeligro/cc2_weatherpredictor_v2 -f Dockerfile . "',
    dag=dag
)

DesplegarContenedorV2 = BashOperator(
    task_id='deployServiceV2',
    depends_on_past=False,
    bash_command='ssh pepe@157.90.224.208 "docker run -p 3006:3006 --name service2 -d pepitoenpeligro/cc2_weatherpredictor_v2" ',
    dag=dag
)



CopiarFuentesV3 = BashOperator(
    task_id='sendServiceV3',
    depends_on_past=False,
    bash_command='scp  -r /tmp/airflow/p2/cc2_weatherpredictor_v3-master pepe@157.90.224.208:~',
    dag=dag
)

GenerarContenedorV3 = BashOperator(
    task_id='generateContainerV3',
    depends_on_past=False,
    bash_command='ssh pepe@157.90.224.208 "cd cc2_weatherpredictor_v3-master && docker build --no-cache -t pepitoenpeligro/cc2_weatherpredictor_v3 -f Dockerfile . " ',
    dag=dag
)

DesplegarContenedorV3 = BashOperator(
    task_id='deployServiceV3',
    depends_on_past=False,
    bash_command='ssh pepe@157.90.224.208 "docker run -p 3007:3007 --name service3 -d pepitoenpeligro/cc2_weatherpredictor_v3" ',
    dag=dag
)


CheckPackages >> CreateDir >> [DownloadTemperatureData, DownloadHumidityData]
[DownloadTemperatureData, DownloadHumidityData] >> ExtractZip >> Preprocessing
Preprocessing >> SaveToMongo >> [DownloadService1, DownloadService2, DownloadService3] 
DownloadService1 >> ExtractService1 >> TestServiceV1
DownloadService2  >> ExtractService2 >> TestServiceV2
DownloadService3  >> ExtractService3 >> TestServiceV3
TestServiceV1 >> CopiarFuentesV1 >> GenerarContenedorV1 >> DesplegarContenedorV1
TestServiceV2 >> CopiarFuentesV2 >> GenerarContenedorV2 >> DesplegarContenedorV2
TestServiceV3 >> CopiarFuentesV3 >> GenerarContenedorV3 >> DesplegarContenedorV3
