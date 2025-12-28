from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello MWAA!")

dag = DAG('test_dag', start_date=datetime(2025, 11, 25), schedule_interval='@daily')

task = PythonOperator(
    task_id='hello_task',
    python_callable=hello_world,
    dag=dag
)
