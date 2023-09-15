from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator 
import pendulum


# local timezone
local_tz = pendulum.timezone("Asia/Seoul")

venv_dir='/home/sychung/airflow/.venv'
main_dir='/home/sychung/airflow/app/main.py'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023,9,11, tzinfo=local_tz),
    'retries': 0,
    'catchup':False
}

with DAG(
    dag_id='bigquery_test_for_searching',
    description='search_bigquery_data',
    default_args=default_args,
    start_date=datetime(2023,9,11),
    schedule_interval=None,
    tags=['query','test','connect']
) as dag:
    
    task1 = BashOperator(
    task_id ="task2", 
    bash_command="echo task1", 
    dag=dag
    )
    
    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)
    
    t1 = BashOperator(
        task_id='bigquery_search_test',
        bash_command='bash /home/sychung/airflow/start.sh '
    )
    
    start >> task1 >> t1 >> end 











