from datetime import datetime
from typing import Any
from airflow.sdk import dag, task


@dag(
    dag_id="parallel_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def parallel_dag():
    @task.python
    def extract_task(**kwargs) -> None:
        print("Extracting data...")
        ti = kwargs["ti"]
        extracted_data_dict = {
            "api_extracted_data": [1, 2, 3],
            "db_extracted_data": [4, 5, 6],
            "file_extracted_data": [7, 8, 9],
            "weekend_flag": False,
        }
        ti.xcom_push(key="extract_task_data", value=extracted_data_dict)
        print("Data extracted successfully")

    @task.python
    def transform_task_api(**kwargs) -> None:
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids="extract_task", key="extract_task_data")
        api_data = data["api_extracted_data"]
        transformed_api_data = [x * 2 for x in api_data]
        transformed_api_data_dict = {"api_transformed_data": transformed_api_data}
        ti.xcom_push(key="transform_task_api_data", value=transformed_api_data_dict)
        print("API data transformed successfully")

    @task.python
    def transform_task_db(**kwargs) -> None:
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids="extract_task", key="extract_task_data")
        db_data = data["db_extracted_data"]
        transformed_db_data = [x * 2 for x in db_data]
        transformed_db_data_dict = {"db_transformed_data": transformed_db_data}
        ti.xcom_push(key="transform_task_db_data", value=transformed_db_data_dict)
        print("DB data transformed successfully")

    @task.python
    def transform_task_file(**kwargs) -> None:
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids="extract_task", key="extract_task_data")
        file_data = data["file_extracted_data"]
        transformed_file_data = [x * 2 for x in file_data]
        transformed_file_data_dict = {"file_transformed_data": transformed_file_data}
        ti.xcom_push(key="transform_task_file_data", value=transformed_file_data_dict)
        print("File data transformed successfully")

    # Creating the conditional node
    @task.branch
    def decide_load(**kwargs) -> str:
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids="extract_task", key="extract_task_data")
        weekend_flag = data["weekend_flag"]
        if weekend_flag:
            return "no_load_task"
        else:
            return "load_task"

    @task.python
    def load_task(**kwargs) -> dict[str, Any]:
        ti = kwargs["ti"]
        api_data = ti.xcom_pull(task_ids="transform_task_api", key="transform_task_api_data")
        db_data = ti.xcom_pull(task_ids="transform_task_db", key="transform_task_db_data")
        file_data = ti.xcom_pull(task_ids="transform_task_file", key="transform_task_file_data")
        loaded_data = {
            "api_loaded_data": api_data,
            "db_loaded_data": db_data,
            "file_loaded_data": file_data,
        }
        print(f"Data loaded successfully: {loaded_data}")
        return {"loaded_data": loaded_data}

    @task.python
    def no_load_task(**kwargs) -> None:
        print("No loading on weekend")

    #Define task dependencies
    extract = extract_task()
    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_file = transform_task_file()
    load = load_task()
    no_load = no_load_task()
    decide = decide_load()

    extract >> [transform_api, transform_db, transform_file] >> decide
    decide >> [no_load, load]

parallel_dag()