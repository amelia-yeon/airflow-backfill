
from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator 


import pendulum
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from app.controller.get_data import GetPublicData



# local timezone
local_tz = pendulum.timezone("Asia/Seoul")

def _complete():
    print("complete")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023,9,11, tzinfo=local_tz),
    'retries': 0,
    'catchup':False
}

with DAG(
    dag_id='bigquery_pipeline_test',
    description='public_api_pipeline',
    default_args=default_args,
    start_date=datetime(2023,9,11),
    schedule_interval=None,
    tags=['bigquery','batch','test']
) as dag:
    
    
    start = EmptyOperator(task_id="start", dag=dag)
    
    delete_table = PythonOperator(
        task_id='delete_bigquery_table',
        python_callable=GetPublicData().delete_table,
        dag=dag
    )
    
    url_test = PythonOperator(
        task_id='test_for_url',
        python_callable=GetPublicData().status_test,
        dag=dag
    )
    
    store_public_data = PythonOperator(
        task_id = 'get_data_from_public',
        python_callable=GetPublicData().property,
        dag=dag
    )
    
    
    end = PythonOperator(
        task_id='end',
        python_callable=_complete,
        dag=dag
    )
    
    
    start >> delete_table >> url_test >> store_public_data >> end 
    
    