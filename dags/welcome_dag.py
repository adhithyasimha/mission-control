from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print(" Hello from Mission Control!")

with DAG(
    dag_id="hello_mission_control",
    start_date=datetime(2023, 7, 12), 
    schedule=None,                     
    catchup=False,                     
    tags=["example"],
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )

    hello_task
