from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.helpers import cross_downstream
from airflow.exceptions import AirflowTaskTimeout

from datetime import datetime, timedelta

default_args = {
    "email": ["marc-airflow@yopmail.com"],
    "email_on_retry": True,
    "email_on_failure": False,
}


def _my_func(execution_date):
    if execution_date.day == 5:
        raise ValueError("Error")


def _extract_a_failure_callback(context):
    if isinstance(context["exception"], AirflowTaskTimeout):
        print("The task timed out")
    else:
        print("Other error")


def _extract_b_failure_callback(context):
    if isinstance(context["exception"], AirflowTaskTimeout):
        print("The task timed out")
    else:
        print("Other error")


with DAG(
    "my_dag_v_1_0_0",
    default_args=default_args,
    start_date=datetime(2022, 9, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract_a = BashOperator(
        owner="john",
        task_id="extract_a",
        bash_command="echo 'task_a' && sleep 10",
        wait_for_downstream=True,
        execution_timeout=timedelta(seconds=5),
        on_failure_callback=_extract_a_failure_callback,
    )

    extract_b = BashOperator(
        owner="john",
        task_id="extract_b",
        bash_command="echo 'task_b' && sleep 10",
        wait_for_downstream=True,
        execution_timeout=timedelta(seconds=5),
        on_failure_callback=_extract_a_failure_callback,
    )
