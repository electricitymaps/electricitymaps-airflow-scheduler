from datetime import datetime, timedelta, timezone

PATIENCE = timedelta(hours=24)
EXPECTED_DURATION = timedelta(hours=2)
LOCATION = (50.851748, 4.3286263)  # Brussels (lat, lon)

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag

from electricitymaps_airflow_scheduler.scheduler import ElectricityMapsSchedulerOperator


@dag(
    start_date=datetime.now(timezone.utc),
    schedule=None,
    catchup=False,
    tags=["test"],
)
def test_workflow_with_scheduler():
    setup_dummy_task = BashOperator(
        task_id="setup_dummy_task",
        bash_command="echo 'Setup dummy task'",
    )

    scheduler_task = ElectricityMapsSchedulerOperator(
        task_id="scheduler_task",
        patience=PATIENCE,
        expected_duration=EXPECTED_DURATION,
        location=LOCATION,
    )

    def dummy_python_operation():
        print("Dummy task ran at")
        print(datetime.now(timezone.utc))
        return "Done"

    python_operation_task = PythonOperator(
        task_id="python_operation_task",
        python_callable=dummy_python_operation,
    )

    other_task = BashOperator(
        task_id="other_task",
        bash_command="echo 'Other task'",
    )

    setup_dummy_task >> scheduler_task >> [python_operation_task, other_task]


test_workflow_with_scheduler()
