# Mission Control 🚀

A satellite telemetry data pipeline built with Apache Airflow that simulates, processes, and stores lunar satellite data.

## What it does

This system automatically:
1. **Generates** realistic satellite telemetry data (position, temperature, battery, etc.)
2. **Processes** the data and adds health status indicators
3. **Uploads** processed data to AWS S3 for storage

Runs every 2 minutes to simulate continuous satellite monitoring.

## Sample Data Generated
```json
{
  "timestamp": "2025-07-18T10:30:00.000000",
  "satellite_id": "Luna-1",
  "orbit_angle": 245,
  "position": {"x": 1234.56, "y": -987.32, "z": 456.78},
  "velocity_kms": 1.87,
  "temperature": -45.23,
  "battery_voltage": 3.8,
  "signal_strength": 85,
  "status": "OK"
}
```

## Quick Start

1. **Install dependencies**
   ```bash
   pip install apache-airflow[amazon] boto3
   ```

2. **Set up AWS credentials**
   ```bash
   aws configure
   aws s3 mb s3://sat-telemetry-bucket
   ```

3. **Start Airflow**
   ```bash
   airflow db init
   airflow scheduler &
   airflow webserver --port 8080
   ```

4. **Enable DAGs** at `http://localhost:8080`

## Files

- `sat_telemetry_pipe.py` - Main pipeline that generates and processes telemetry
- `upload_s3.py` - Uploads processed files to S3
- `welcome_dag.py` - Simple hello world DAG for testing

Built for learning Airflow with a space mission theme! 🌙
