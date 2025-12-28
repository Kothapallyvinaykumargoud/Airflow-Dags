from pathlib import Path
from datetime import datetime
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.operators.bash import BashOperator

dag=DAG(
    dag_id="everyhour",
    start_date=datetime(2025,10,16),
    #end_date=datetime(2025,10,17),
    schedule='@hourly'
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /opt/airflow/data/events && "
        "curl -o /opt/airflow/data/events/events.json http://host.docker.internal:5001/events"
    ),
    dag=dag,
)


def calculate_stats(input_path,output_path):
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)


    events=pd.read_json(input_path)
    stats=events.groupby(['date','user']).size().reset_index()

    stats.to_csv(output_path,index=False)

calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=calculate_stats,
    op_kwargs={
        "input_path": "/opt/airflow/data/events/events.json",
        "output_path": "/opt/airflow/data/events/output.csv"
    },
    dag=dag
)


fetch_events>>calculate_stats



