from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.decorators import task
from airflow.operators.python import get_current_context

DEFAULT_ARGS = {"start_date": datetime(2022, 9, 1)}


# def _process(path, filename, **context):

#     print(f"{path}/{filename} - {context['ds']}")
@task
def process(my_settings):
    context = get_current_context()
    print(f"{my_settings['path']} / {my_settings['filename']} - {context['ds']} ")


with DAG(
    "my_python_dag",
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    catchup=False,
) as dag:
    # task_a = PythonOperator(
    #     task_id="task_a",
    #     python_callable=_process,
    #     # op_args=["/user/local/airflow", "data.csv"],
    #     # op_kwargs={
    #     #     "path": "{{ var.value.path }}",
    #     #     "filename": "{{ var.value.filename }}",
    #     # },
    #     op_kwargs=Variable.get("my_settings", deserialize_json=True),
    # )
    store = BashOperator(task_id="store", bash_command="echo 'hi' ")
    process(Variable.get("my_settings", deserialize_json=True)) >> store
