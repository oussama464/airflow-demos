from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.utils.helpers import cross_downstream


DEFAULT_ARGS = {
    "owner": "oussama",
    "start_date": datetime(2022, 1, 1),
    # "email": ["oussama.missaoui201@gmail.com"],
    # "email_on_retry": True,
    # "email_on_failure": True,
}


with DAG(
    "ver_dag_v_1_0_1",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    extract_a = BashOperator(
        task_id="extract_a", bash_command="echo 'task_a' && sleep 10"
    )
    extract_b = BashOperator(
        task_id="extract_b", bash_command="echo 'task_a' && sleep 10"
    )
    process_a = BashOperator(
        task_id="process_a",
        bash_command="echo 'task_a' && sleep 15",
        pool="process_tasks",
        execution_timeout=timedelta(seconds=12),
    )
    process_b = BashOperator(
        task_id="process_b",
        bash_command="echo 'task_c' && sleep 15",
        pool="process_tasks",
        execution_timeout=timedelta(seconds=20),
    )
    process_c = BashOperator(
        task_id="process_c",
        retries=3,
        retry_exponential_backoff=True,
        retry_delay=timedelta(seconds=5),
        pool="process_tasks",
        bash_command="echo '{{ ti.try_number}}' && sleep 15",
        execution_timeout=timedelta(seconds=10),
    )
    # end_process = EmptyOperator(task_id="end_process")
    # end_extract = EmptyOperator(task_id="end_extract")

    cross_downstream([extract_a, extract_b], [process_a, process_b, process_c])
