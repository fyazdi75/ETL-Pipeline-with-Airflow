from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from ETL import test, Authorization, Extract, transform, loadToxlsx


#define start and end date
#Yesterday midnight
StartDate = (datetime.now()- pd.offsets.BusinessDay(n=1)).strftime('%Y-%m-%d')
#today midnight
EndDate =  (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)).strftime('%Y-%m-%d')


default_args= {
    'owner': 'faezeh',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id = 'ETL-Inbound',
    default_args = default_args,
    description = ' this is our first python airflow DAG',
    start_date = datetime(2024,1,25,2),
    schedule_interval = '@daily'


) as dag:
    taskAuth = PythonOperator(
        task_id ='Auth_task',
        python_callable = Authorization
    )

    taskExtract= PythonOperator(
        task_id = 'Extract_task',
        python_callable = Extract,
        op_kwargs = {'StartDate':StartDate, 'EndDate':EndDate}
    )
    tasktransform = PythonOperator(
        task_id = 'transform_task',
        python_callable = transform
    )
    taskLoad = PythonOperator(
        task_id = 'load_task',
        python_callable = loadToxlsx 
    )

taskAuth >> taskExtract >> tasktransform >> taskLoad