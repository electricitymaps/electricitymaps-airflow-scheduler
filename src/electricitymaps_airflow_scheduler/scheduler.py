from datetime import datetime, timedelta, timezone

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.providers.standard.triggers.temporal import DateTimeTrigger
from airflow.utils.context import Context

from electricitymaps_airflow_scheduler.lib.electricitymaps import (
    DEFAULT_OPTIMIZATION_SIGNAL,
    schedule_execution,
)


class ElectricityMapsSchedulerOperatorExtraLink(BaseOperatorLink):
    """Extra link for ElectricityMaps Scheduler Operator that points to the ElectricityMaps website."""

    name = "ElectricityMaps"

    def get_link(self, operator: BaseOperator, *, ti_key=None):
        return "https://www.electricitymaps.com/"


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
        now = datetime.now(timezone.utc)
        end_datetime = (now + self.patience).replace(
            minute=0, second=0, microsecond=0
        ) + timedelta(hours=1)
        self.log.info(
            f"""
            requesting optimal execution from the Electricity Maps API between {now} and {end_datetime},
            with expected duration {self.expected_duration} for location {self.location} and optimization signal {DEFAULT_OPTIMIZATION_SIGNAL}
            """
        )
        optimal_execution = schedule_execution(
            expected_duration_hours=int(self.expected_duration.total_seconds() / 3600),
            end_datetime=end_datetime,
            optimization_signal=DEFAULT_OPTIMIZATION_SIGNAL,
            locations=[self.location],
        )

        now = datetime.now(timezone.utc)
        if optimal_execution.optimal_start_time < now:
            self.log.info("proceeding with execution")
            return None

        self.log.info(f"deferring to {optimal_execution.optimal_start_time}")
        self.defer(
            trigger=DateTimeTrigger(
                moment=optimal_execution.optimal_start_time, end_from_trigger=True
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event_list: list) -> None:
        self.log.info("execute_complete")
        return None
