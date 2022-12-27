from multiprocessing.connection import wait
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

DEFAULT_ARGS = {"owner": "airflow"}


def _my_func(execution_date):
    if execution_date.day == 24:
        raise ValueError("ERROR")


with DAG(
    "ver_dag_v_1_0_0",
    start_date=datetime(2022, 12, 12),
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    dagrun_timeout=40,
    catchup=True,
) as dag:
    task_a = BashOperator(
        task_id="task_a",
        bash_command="echo 'task_a' && sleep 10",
        # wait_for_downstream=True,
    )
    task_b = BashOperator(
        task_id="task_b", bash_command="echo 'task_b' && exit 0", task_concurrency=1
    )
    task_c = PythonOperator(
        task_id="task_c", python_callable=_my_func, depends_on_past=True
    )

    task_a >> task_b >> task_c
