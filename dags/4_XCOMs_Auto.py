from datetime import datetime
from typing import Any

from airflow.sdk import dag, task


@dag(
    dag_id="xcoms_auto_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def xcoms_auto_dag():
    @task.python
    def first_task() -> dict[str, list[int]]:
        print("Hello from first task!")
        fetched_data = {"data": [1, 2, 3, 4, 5]}
        return fetched_data

    @task.python
    def second_task(data: Any) -> dict[str, list[int]]:
        print("Hello from second task!")
        transformed_data = [x * 2 for x in data["data"]]
        transformed_data_dict = {"data": transformed_data}
        return transformed_data_dict


    @task.python
    def third_task(data: Any) -> dict[str, list[int]]:
        print("Hello from third task!")
        loaded_data = data
        return loaded_data

    #Define task dependencies
    first = first_task()
    second = second_task(first)
    third = third_task(second)

    _ = first >> second >> third


xcoms_auto_dag_instance = xcoms_auto_dag()