from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Following are defaults which can be overridden later on
args = {"owner": "test", "start_date": datetime(2018, 11, 1), "provide_context": True}

with DAG(
    "Hello-world",
    description="Hello-world",
    schedule_interval="*/30 * * * *",
    catchup=False,
    default_args=args,
) as dag:  # 0 * * * *   */1 * * * *
    t1 = BashOperator(task_id="task_1", bash_command='echo "Hello World from Task 1"')

    t2 = BashOperator(task_id="task_2", bash_command='echo "Hello World from Task 2"')

    t3 = BashOperator(task_id="task_3", bash_command='echo "Hello World from Task 3"')

    t4 = BashOperator(task_id="task_4", bash_command='echo "Hello World from Task 4"')

    t1 >> t2
    t1 >> t3
    t2 >> t4
    t3 >> t4
