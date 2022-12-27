from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime

with DAG(
    "group_dag",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    def downloads():
        with TaskGroup("downloads", tooltip="Download tasks") as group:

            download_a = BashOperator(task_id="download_a", bash_command="sleep 10")

            download_b = BashOperator(task_id="download_b", bash_command="sleep 10")

            download_c = BashOperator(task_id="download_c", bash_command="sleep 10")
        return group

    check_files = BashOperator(task_id="check_files", bash_command="sleep 10")

    def transform():

        with TaskGroup("transform", tooltip="transform tasks") as group:

            transform_a = BashOperator(task_id="transform_a", bash_command="sleep 10")

            transform_b = BashOperator(task_id="transform_b", bash_command="sleep 10")

            transform_c = BashOperator(task_id="transform_c", bash_command="sleep 10")
        return group

    d = downloads()
    t = transform()

    (d >> check_files >> t)
