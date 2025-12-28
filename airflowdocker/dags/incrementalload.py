from pathlib import Path
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

dag = DAG(
    dag_id="incremental",
    start_date=datetime(2025,11,15),
    catchup=False
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /opt/airflow/data/events && "
        "curl -s -o /opt/airflow/data/events/events_{{ds}}.json "
        "\"http://host.docker.internal:5001/events?start_date=2025-11-15&end_date=2025-11-15\""
    ),
    dag=dag,
)

def calculate_stats(**context):
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    events = pd.read_json(input_path)
    # If 'time' column exists, extract date, otherwise use 'date'
    if 'time' in events.columns:
        events['date'] = pd.to_datetime(events['time']).dt.date
    elif 'date' in events.columns:
        events['date'] = pd.to_datetime(events['date']).dt.date
    else:
        raise ValueError("No 'time' or 'date' column found in JSON")
    stats = events.groupby(['date','user']).size().reset_index()

    stats.to_csv(output_path, index=False)

calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=calculate_stats,
    templates_dict={
        "input_path": "/opt/airflow/data/events/events_{{ds}}.json",
        "output_path": "/opt/airflow/data/events/output_{{ds}}.csv"
    },
    dag=dag,
)

fetch_events >> calculate_stats
