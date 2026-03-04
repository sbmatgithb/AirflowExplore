from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def bash_dag():
    @task.python
    def first_task():
        print("Hello from first task!")

    @task.python
    def second_task():
        print("Hello from second task!")

    @task.python
    def third_task():
        print("Hello from third task!")

    @task.python
    def fourth_task():
        print("Hello from fourth task!")

    @task.bash
    def bast_task_modern() -> str:
        return "echo https://airflow.apache.org/"

    bash_task_oldSchool = BashOperator(
        task_id="bash_task_oldSchool",
        bash_command="echo https://airflow.apache.org/",
    )

    #Define task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    fourth = fourth_task()
    bash_modern = bast_task_modern()
    bash_oldSchool = bash_task_oldSchool

    _ = first >> second >> third >> fourth >> bash_modern >> bash_oldSchool



bash_dag_instance = bash_dag()