from pendulum import datetime

from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id="orchestrated_serial_dag",
    start_date=datetime(year=2026, month=3, day=14, tz="Asia/Kolkata"),
    end_date=datetime(year=2026, month=3, day=31, tz="Asia/Kolkata"),
    catchup=True,
    schedule=None,  # Only triggered manually or by external means
)
def orchestrated_serial_dag():
    trigger_dag_9 = TriggerDagRunOperator(
        task_id="trigger_cron_dag",
        trigger_dag_id="first_schedule_cronn_dag",
        wait_for_completion=True,
    )
    trigger_dag_10 = TriggerDagRunOperator(
        task_id="trigger_delta_dag",
        trigger_dag_id="first_schedule_delta_dag",
        wait_for_completion=True,
    )
    trigger_dag_11 = TriggerDagRunOperator(
        task_id="trigger_incremental_dag",
        trigger_dag_id="incremental_load_dag",
        wait_for_completion=True,
    )
    trigger_dag_12 = TriggerDagRunOperator(
        task_id="trigger_special_dates_dag",
        trigger_dag_id="special_dates_dag",
        wait_for_completion=True,
    )

    _ = trigger_dag_9 >> trigger_dag_10 >> trigger_dag_11 >> trigger_dag_12


orchestrated_serial_dag()
