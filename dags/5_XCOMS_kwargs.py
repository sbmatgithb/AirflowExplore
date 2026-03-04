from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="xcoms_kwargs_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def xcoms_kwargs_dag():
    @task.python
    def first_task(**kwargs) -> None:
        # Extracting 'ti' from kwargs to push to XComs manually
        ti = kwargs["ti"]
        fetched_data = {"data": [1, 2, 3, 4, 5]}
        ti.xcom_push(key="first_task_data", value=fetched_data)
        print("Hello from first task!")

    @task.python
    def second_task(**kwargs) -> None:
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids="first_task", key="first_task_data")
        transformed_data = [x * 2 for x in data["data"]]
        transformed_data_dict = {"data": transformed_data}
        ti.xcom_push(key="second_task_data", value=transformed_data_dict)
        print("Hello from second task!")

    @task.python
    def third_task(**kwargs) -> dict[str, list[int]]:
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids="second_task", key="second_task_data")
        loaded_data = data
        return loaded_data

    #Define task dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    _ = first >> second >> third


xcoms_kwargs_dag_instance = xcoms_kwargs_dag()