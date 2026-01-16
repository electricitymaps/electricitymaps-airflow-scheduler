# Electricity Maps Airflow scheduler

An Apache Airflow provider that schedules pipeline operations to run when carbon intensity is lowest. The operator defers task execution until the optimal time within a configurable patience window, using the [Electricity Maps](https://www.electricitymaps.com/) carbon-aware optimizer API.

## Installation

```bash
pip install electricitymaps-airflow-scheduler
```

## Configuration

Set your Electricity Maps API token as an environment variable:

```bash
export ELECTRICITYMAPS_API_TOKEN=your_token_here
```

## Usage

The package provides `ElectricityMapsSchedulerOperator`, a deferrable operator that queries the Electricity Maps API to find the optimal execution time based on carbon intensity forecasts.

### Basic example

```python
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator

from electricitymaps_airflow_scheduler.scheduler import ElectricityMapsSchedulerOperator


@dag(
    start_date=datetime.now(timezone.utc),
    schedule=None,
    catchup=False,
)
def my_carbon_aware_workflow():
    # This operator will defer until the optimal low-carbon time
    scheduler = ElectricityMapsSchedulerOperator(
        task_id="wait_for_low_carbon",
        patience=timedelta(hours=24),      # How long to wait for optimal conditions
        expected_duration=timedelta(hours=2),  # Expected runtime of downstream tasks
        location=(50.851748, 4.3286263),   # Brussels (lat, lon)
    )

    def my_task():
        print("Running at optimal carbon intensity!")

    run_task = PythonOperator(
        task_id="run_task",
        python_callable=my_task,
    )

    scheduler >> run_task


my_carbon_aware_workflow()
```

### Operator parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `patience` | `timedelta` | Maximum time the operator will wait for optimal conditions |
| `expected_duration` | `timedelta` | Expected runtime of the tasks that follow this operator |
| `location` | `tuple[float, float]` | Geographic coordinates as `(latitude, longitude)` |

The operator uses the `flow-traced_carbon_intensity` optimization signal by default, which minimizes the carbon intensity of electricity consumption at the specified location.

### Location requirements

The operator supports a single location at a time. The provided coordinates must be within a zone supported by your Electricity Maps API subscription with **forecast access**. The API resolves coordinates to a `zone_key` (e.g., `BE` for Belgium, `DE` for Germany).

The optimizer uses forecast data to determine the optimal execution time within your patience window, so standard API access with only real-time data is not sufficient.

You can check available zones in the [Electricity Maps API documentation](https://docs.electricitymaps.com/).

## How it works

The operator uses the Electricity Maps [Carbon-Aware Optimizer API](https://docs.electricitymaps.com/) endpoint:

```
POST https://api.electricitymaps.com/beta/carbon-aware-optimizer
```

When the operator executes, it:

1. Calculates the optimization window from now until `now + patience` (rounded to the next hour)
2. Sends a request to the API with:
   - `duration`: The expected task duration (ceiled to whole hours)
   - `startWindow`: The next full hour from the current time
   - `endWindow`: The end of the patience window
   - `locations`: The coordinates as `[longitude, latitude]`
   - `optimizationMetric`: The signal to optimize for (default: `flow-traced_carbon_intensity`)
3. Receives the optimal start time from the API response
4. If the optimal time is in the future, the operator **defers** using Airflow's `DateTimeTrigger` and releases the worker
5. When the trigger fires at the optimal time, the operator completes and downstream tasks begin

This deferrable pattern means no worker slot is occupied while waiting for optimal conditions.

## Development

```bash
# Install dependencies
poetry install

# Run linting
poetry run ruff check .
poetry run ruff format .

# Run tests
poetry run pytest

# Run Airflow locally (credentials shown in logs)
poetry run airflow standalone
```

## License

Apache License 2.0
