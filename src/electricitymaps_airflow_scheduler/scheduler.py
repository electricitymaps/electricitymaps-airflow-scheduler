from datetime import datetime, timedelta, timezone

from airflow.models import BaseOperator
from airflow.providers.standard.triggers.temporal import DateTimeTrigger
from airflow.utils.context import Context

from electricitymaps_airflow_scheduler.lib.electricitymaps import (
    DEFAULT_OPTIMIZATION_SIGNAL,
    schedule_execution,
)


class ElectricityMapsSchedulerOperator(BaseOperator):
    def __init__(
        self,
        patience: timedelta,
        expected_duration: timedelta,
        location: tuple[float, float],
        *args,
        **kwargs,
    ):
        self.patience = patience
        self.expected_duration = expected_duration
        self.location = location
        super().__init__(*args, **kwargs)

    def execute(self, context: Context) -> None:
        optimal_execution = schedule_execution(
            expected_duration_hours=int(self.expected_duration.total_seconds() / 3600),
            end_datetime=context["execution_date"] + self.expected_duration,
            optimization_signal=DEFAULT_OPTIMIZATION_SIGNAL,
            locations=[self.location],
        )

        now = datetime.now(timezone.utc)
        if optimal_execution.optimal_start_time < now:
            return None

        self.defer(
            trigger=DateTimeTrigger(
                moment=optimal_execution.optimal_start_time, end_from_trigger=True
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event_list: list) -> None:
        return None
