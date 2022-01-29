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
    'retry_delay': timedelta(minutes=10)#,
    #'run_as_user':'airflow'
}




dag = DAG(
    dag_id='aadespliegue',
    default_args = default_args,
    description = 'desplegarservicios',
    dagrun_timeout=timedelta(minutes=2),
    schedule_interval=timedelta(days=1)
    
)

# QuienSoy = BashOperator(
#     task_id='quiensoy',
#     depends_on_past=False,
#     bash_command='whoami',
#     dag=dag
# )

CopiarFuentes = BashOperator(
    task_id='desplieguev1',
    depends_on_past=False,
    bash_command='scp  -r /tmp/airflow/p2/cc2_weatherpredictor_v1-master pepe@157.90.224.208:~',
    dag=dag
)

GenerarContenedorV1 = BashOperator(
    task_id='generarcontenedorv1',
    depends_on_past=False,
    bash_command='ssh pepe@157.90.224.208 "cd cc2_weatherpredictor_v1-master && docker build --no-cache -t pepitoenpeligro/cc2_weatherpredictor_v1 -f Dockerfile . " && exit',
    dag=dag
)

DesplegarContenedorV1 = BashOperator(
    task_id='desplegarcontenedorv1',
    depends_on_past=False,
    bash_command='ssh pepe@157.90.224.208 "docker run -p 3005:3005 -d pepitoenpeligro/cc2_weatherpredictor_v1" && exit',
    dag=dag
)

CopiarFuentes >> GenerarContenedorV1 >> DesplegarContenedorV1