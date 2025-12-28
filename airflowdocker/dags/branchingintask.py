from airflow import DAG
import airflow
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

erp_new=datetime.now() - timedelta(days=1)
def fetch_sales_old(**context):
    print("fetching old sales data")

def fetch_sales_new(**context):
    print("fetching new sales data")

def fetch_sales(**context):
    if  context['execuation_date']<erp_new:
        fetch_sales_old(**context)
    else:
        fetch_sales_new(**context)
def fetch_weather_old(**context):
    print("fetching old weather data")

def fetch_weather_new(**context):
    print("fetching new weather data")

def fetch_weather(**context):
    if  context['execuation_date']<erp_new:
        fetch_weather_old(**context)
    else:
        fetch_weather_new(**context)
with DAG(
    dag_id="fan_in_out",
    start_date=datetime.now() - timedelta(days=3),
    schedule="@daily",
) as dag :
    
    start= EmptyOperator(task_id="start")
    fetch_sales = EmptyOperator(task_id="fetch_sales",python_callable=fetch_sales)
    clean_sales = EmptyOperator(task_id="clean_sales")
    fetch_weather =EmptyOperator(task_id="fetch_weather",python_callable=fetch_weather)
    clean_weather =EmptyOperator(task_id="clean_weather")
    join_dataset = EmptyOperator(task_id="join_dataset")
    train_model = EmptyOperator(task_id="train_model")
    deploy_model = EmptyOperator(task_id="deploy_model")

#start>>fetch_sales>>clean_sales>>fetch_weather>>clean_weather>>join_dataset>>train_model>>deploy_model

    start >>[fetch_sales,fetch_weather]
    fetch_sales>>clean_sales 
    fetch_weather>>clean_weather
    [clean_sales,clean_weather]>>join_dataset>>train_model>>deploy_model