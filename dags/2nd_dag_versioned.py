from datetime import datetime

from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def versioned_dag():
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

    #Define task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    fourth = fourth_task()

    _ = first >> second >> third >> fourth


versioned_dag_instance = versioned_dag()