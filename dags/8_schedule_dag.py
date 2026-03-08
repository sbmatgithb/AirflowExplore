from pendulum import datetime

from airflow.sdk import dag, task


@dag(
    schedule="@daily",
    start_date=datetime(year=2026, month=3, day=8, tz="Asia/Kolkata"),
    catchup=False,
    is_paused_upon_creation=True,
    dag_id="first_schedule_dag"
)
def first_schedule_dag():
    @task.python
    def first_task():
        print("Hello from first task!")

    @task.python
    def second_task():
        print("Hello from second task!")

    @task.python
    def third_task():
        print("Hello from third task!")

    #Define task dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    _ = first >> second >> third

first_schedule_dag()