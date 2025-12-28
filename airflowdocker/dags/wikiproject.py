import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from urllib import request
from airflow.operators.python import PythonOperator

dag=DAG(
    dag_id="wikiproject",
    start_date = datetime.now() - timedelta(days=3),
    schedule="@hourly"
)

get_data = BashOperator(
    task_id='get_data',
    bash_command=(
        "mkdir -p /opt/airflow/data/wikipage && "
        "curl -o /opt/airflow/data/wikipage/wikipageview.gz"
        "https://dumps.wikimedia.org/other/pageviews/"
        "{{ data_interval_start.strftime('%Y') }}/"
        "{{ data_interval_start.strftime('%Y-%m') }}/"
        "pageviews-{{ data_interval_start.strftime('%Y%m%d') }}-"
        "{{ '%02d' % data_interval_start.hour }}0000.gz"
    ),
    dag=dag,
)

get_data

# same as above but using python
def get_data():

    url=("https://dumps.wikimedia.org/other/pageviews/"
    f"{year}/{year}-{month:0>2}/projectviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    output_path= "/opt/airflow/data/wikipage/wikipageview.gz"
    request.urlretrieve(url, output_path)

get_data=PythonOperator(
    task_id='get_data',
    python_collable=get_data,
    dag=dag,
)
