
# bigquery dag 예시 파일 -> 현재 사용하는 operator가 아님 수정 필요 

import os 
from datetime import datetime, timedelta
from airflow import models, DAG

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator
    # BigQueryInsertJobOperator
)


from  airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


DATASET = "airflow_test2" 
TABLE = "forestfires"
IMPERSONATION_CHAIN = str(os.environ.get('IMPERSONATION_CHAIN'))
GCS_PROJECT_ID = str(os.environ.get('GCS_PROJECT_ID'))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'project_id': GCS_PROJECT_ID
}

dag = DAG(
    dag_id="bigquery_dataset_create_test_v2",
    description="create bigquery for test",
    default_args=default_args,
    start_date= datetime(2023, 9, 5),
    tags=["bigquery","test","real","final"]
    ) 

task1 = BashOperator(
    task_id ="task2", 
    bash_command="echo task1", 
    dag=dag
)

create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id="create_dataset",
    gcp_conn_id="google_cloud_default",
    dataset_id=DATASET,
    location="asia-northeast3",
    impersonation_chain=IMPERSONATION_CHAIN
)


start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)


# start >> task1 >> create_dataset >> end
start >> task1 >> end





