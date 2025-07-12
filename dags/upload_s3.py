from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import boto3

default_args = {
    'owner': 'mission_control',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='uploads3',
    default_args=default_args,
    start_date=datetime(2025, 7, 12),
    schedule="*/2 * * * *",
    catchup=False,
    tags=["s3", "upload"]
)

def upload_to_s3():
    s3 = boto3.client('s3')
    bucket_name = 'sat-telemetry-bucket'
    processed_dir = '/home/ec2-user/airflow/data/processed'

    for file in os.listdir(processed_dir):
        if file.endswith('.json'):
            filepath = os.path.join(processed_dir, file)
            s3.upload_file(filepath, bucket_name, f'processed/{file}')

upload_task = PythonOperator(
    task_id='uploadtos3',
    python_callable=upload_to_s3,
    dag=dag
)
