from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

DEFAULT_ARGS = {"start_date": datetime(2022, 9, 1)}
with DAG(
    dag_id="my_bash_dag",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    execute_command = BashOperator(
        task_id="execute_command",
        bash_command="./scripts/command.sh",
        do_xcom_push=False,
    )
