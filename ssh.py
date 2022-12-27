from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime
from airflow.operators.bash import BashOperator

EXECUTE_SPARK_JOB_CMD = "echo 'hello from airflow' > ./airflow.txt"
with DAG(
    dag_id="ssh_operator_example",
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    start = BashOperator(task_id="start", bash_command="echo 'hi'")
    end = BashOperator(task_id="end", bash_command="echo 'bye'")
    ssh_task_azure_vm = SSHOperator(
        task_id="ssh_task_azure_vm",
        ssh_conn_id="ssh_new",
        command=EXECUTE_SPARK_JOB_CMD,
    )
    ssh_task_digital_droplet = SSHOperator(
        task_id="ssh_task_digital_droplet",
        ssh_conn_id="ssh_new_digit",
        command=EXECUTE_SPARK_JOB_CMD,
    )

    start >> [ssh_task_azure_vm, ssh_task_digital_droplet] >> end
