from airflow.models import DAG
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator,
    get_current_context,
)
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from operators import PrintMeOperator

DEFAULT_ARGS = {"start_date": datetime(2022, 9, 1)}


def _check_accuracy():
    accuracy = 1
    if accuracy > 0.9:
        return ["accurate", "top_accurate"]
    return "innacurate"


with DAG(
    dag_id="ml_dag",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    start_message = PrintMeOperator(
        task_id="start_message", my_message="running ml model '{{ ds }}'"
    )
    training_ml = EmptyOperator(task_id="training_ml")
    check_accuracy = BranchPythonOperator(
        task_id="check_accuracy", python_callable=_check_accuracy
    )
    accurate = EmptyOperator(task_id="accurate")
    top_accurate = EmptyOperator(task_id="top_accurate")
    inaccurate = EmptyOperator(task_id="inaccurate")
    publish_ml = EmptyOperator(
        task_id="publish_ml", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    (
        start_message
        >> training_ml
        >> check_accuracy
        >> [accurate, inaccurate, top_accurate]
        >> publish_ml
    )
