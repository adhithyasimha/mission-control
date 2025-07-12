from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import random
import os

default_args = {
    'owner': 'mission_control',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='moon_satellite_telemetry_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 7, 12),
    schedule='*/2 * * * *',  
    catchup=False,
    tags=["telemetry", "moon", "satellite"]
) as dag:

    def simulate_telemetry():
        angle = random.randint(0, 359)  
        data = {
            "timestamp": str(datetime.utcnow()),
            "satellite_id": "Luna-1",
            "orbit_angle": angle,
            "position": {
                "x": round(1737 * random.uniform(-1, 1), 2),  
                "y": round(1737 * random.uniform(-1, 1), 2),
                "z": round(1737 * random.uniform(-1, 1), 2),
            },
            "velocity_kms": round(random.uniform(1.5, 2.5), 2),
            "temperature": round(random.uniform(-170, 120), 2),
            "battery_voltage": round(random.uniform(3.0, 4.2), 2),
            "signal_strength": random.randint(50, 100)
        }
        os.makedirs('/home/ec2-user/airflow/data/raw', exist_ok=True)
        with open(f'/home/ec2-user/airflow/data/raw/data_{int(datetime.utcnow().timestamp())}.json', 'w') as f:
            json.dump(data, f)

    def process_telemetry():
        raw_dir = '/home/ec2-user/airflow/data/raw'
        processed_dir = '/home/ec2-user/airflow/data/processed'
        os.makedirs(processed_dir, exist_ok=True)

        for file in os.listdir(raw_dir):
            if file.endswith('.json'):
                with open(os.path.join(raw_dir, file)) as f:
                    data = json.load(f)
                    data['status'] = 'OK' if data['battery_voltage'] > 3.5 else 'LOW'
                    with open(os.path.join(processed_dir, file), 'w') as pf:
                        json.dump(data, pf)

    t1 = PythonOperator(
        task_id='generate_moon_telemetry',
        python_callable=simulate_telemetry,
    )

    t2 = PythonOperator(
        task_id='process_moon_telemetry',
        python_callable=process_telemetry,
    )

    t1 >> t2
