from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.dummy import DummyOperator

DEFAULT_ARGS = {"start_date": datetime(2022, 1, 1)}


def _my_func(ds):
    print(ds)


with DAG(
    "my_dag", schedule_interval="@daily", default_args=DEFAULT_ARGS, catchup=False
) as dag:
    start = DummyOperator(task_id="start")
    my_task = PythonOperator(task_id="my_task", python_callable=_my_func)
    start >> my_task
