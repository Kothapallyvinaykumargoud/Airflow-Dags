from airflow import DAG
import airflow
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta ,timezone
from airflow.operators.python import PythonOperator,BranchPythonOperator

erp_new = datetime.now(timezone.utc) - timedelta(days=1)

def pick_erp_system(**context):
    logical_date = context['logical_date']  # timezone-aware
    print("Logical date:", logical_date)
    print("ERP new cutoff:", erp_new)

    # Branching must return task_id
    if logical_date < erp_new:
        return "fetch_sales_old"
    else:
        return "fetch_sales_new"

def fetch_sales_old(**context):
    print("fetching sales old data")

def fetch_sales_new(**context):
    print("fetching sales old data")

def clean_sales_old(**context):
    print("fetching sales old data")

def clean_sales_new(**context):
    print("fetching sales old data")


with DAG(
    dag_id="branch_in_dag",
    start_date=datetime.now() - timedelta(days=3),
    schedule="@daily",
) as dag :
    
    start= EmptyOperator(task_id="start")

    pick_erp_system=BranchPythonOperator(
        task_id="pick_erp_system",
        python_callable=pick_erp_system
    )
    fetch_sales_old=PythonOperator(
        task_id='fetch_sales_old',
        python_callable=fetch_sales_old
    )
    
    clean_sales_old = PythonOperator(
        task_id="clean_sales_old", 
        python_callable=clean_sales_old
    )

    fetch_sales_new = PythonOperator(
        task_id="fetch_sales_new", 
        python_callable=fetch_sales_new
    )
    clean_sales_new = PythonOperator(
        task_id="clean_sales_new", 
        python_callable=clean_sales_new
    )

    
    fetch_weather =EmptyOperator(task_id="fetch_weather")
    clean_weather =EmptyOperator(task_id="clean_weather")
    join_dataset = EmptyOperator(task_id="join_dataset",trigger_rule="none_failed")
    train_model = EmptyOperator(task_id="train_model")
    deploy_model = EmptyOperator(task_id="deploy_model")

#start>>fetch_sales>>clean_sales>>fetch_weather>>clean_weather>>join_dataset>>train_model>>deploy_model

    start >>[pick_erp_system,fetch_weather]
    pick_erp_system>>[fetch_sales_old,fetch_sales_new]
    fetch_sales_old>>clean_sales_old
    fetch_sales_new>>clean_sales_new
    fetch_weather>>clean_weather
    [clean_weather,clean_sales_old,clean_sales_new]>>join_dataset>>train_model>>deploy_model