from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import TaskDeferred
from airflow.triggers.temporal import DateTimeTrigger

from electricitymaps_airflow_scheduler.lib.electricitymaps import (
    OptimizationOutput,
    OptimizationSignal,
    OptimizerResponse,
)
from electricitymaps_airflow_scheduler.scheduler import (
    ElectricityMapsSchedulerOperator,
)


class TestElectricityMapsSchedulerOperator:
    @pytest.fixture
    def operator(self):
        return ElectricityMapsSchedulerOperator(
            task_id="test_task",
            patience=timedelta(hours=4),
            expected_duration=timedelta(hours=1),
            location=(48.8566, 2.3522),  # Paris coordinates
        )

    @pytest.fixture
    def mock_context(self):
        return MagicMock()

    def _make_optimizer_response(
        self, optimal_start_time: datetime, zone_key: str = "FR"
    ) -> OptimizerResponse:
        """Helper to create OptimizerResponse with sensible defaults."""
        return OptimizerResponse(
            optimal_start_time=optimal_start_time,
            optimal_location=(2.3522, 48.8566),
            optimization_output=OptimizationOutput(
                metric_value_immediate_execution=100.0,
                metric_value_optimal_execution=80.0,
                metric_value_start_window_execution=90.0,
                metric_unit="gCO2eq/kWh",
                optimization_metric=OptimizationSignal.FLOW_TRACED_CARBON_INTENSITY,
                zone_key=zone_key,
            ),
        )

    @patch("electricitymaps_airflow_scheduler.scheduler.schedule_execution")
    @patch("electricitymaps_airflow_scheduler.scheduler.datetime")
    def test_execute_proceeds_immediately_when_optimal_time_in_past(
        self, mock_datetime, mock_schedule_execution, operator, mock_context
    ):
        """When API returns an optimal time that's already passed, execution proceeds immediately."""
        now = datetime(2024, 1, 1, 14, 30, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = now
        mock_schedule_execution.return_value = self._make_optimizer_response(
            optimal_start_time=datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)
        )

        result = operator.execute(mock_context)

        assert result is None
        mock_schedule_execution.assert_called_once()

    @patch("electricitymaps_airflow_scheduler.scheduler.schedule_execution")
    @patch("electricitymaps_airflow_scheduler.scheduler.datetime")
    def test_execute_defers_when_optimal_time_in_future(
        self, mock_datetime, mock_schedule_execution, operator, mock_context
    ):
        """When API returns a future optimal time, task is deferred with DateTimeTrigger."""
        now = datetime(2024, 1, 1, 10, 30, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = now
        future_optimal_time = datetime(2024, 1, 1, 14, 0, 0, tzinfo=timezone.utc)
        mock_schedule_execution.return_value = self._make_optimizer_response(
            optimal_start_time=future_optimal_time
        )

        with pytest.raises(TaskDeferred) as exc_info:
            operator.execute(mock_context)

        assert isinstance(exc_info.value.trigger, DateTimeTrigger)
        assert exc_info.value.trigger.moment == future_optimal_time
        assert exc_info.value.method_name == "execute_complete"

    @patch("electricitymaps_airflow_scheduler.scheduler.schedule_execution")
    @patch("electricitymaps_airflow_scheduler.scheduler.datetime")
    def test_execute_calculates_end_datetime_from_patience(
        self, mock_datetime, mock_schedule_execution, operator, mock_context
    ):
        """end_datetime is calculated as now + patience, rounded up to the next hour."""
        # At 10:45:30 with 4h patience → end_datetime should be 15:00:00
        now = datetime(2024, 1, 1, 10, 45, 30, tzinfo=timezone.utc)
        mock_datetime.now.return_value = now
        mock_schedule_execution.return_value = self._make_optimizer_response(
            optimal_start_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
        )

        operator.execute(mock_context)

        call_kwargs = mock_schedule_execution.call_args.kwargs
        expected_end = datetime(2024, 1, 1, 15, 0, 0, tzinfo=timezone.utc)
        assert call_kwargs["end_datetime"] == expected_end
        assert call_kwargs["locations"] == [(48.8566, 2.3522)]
