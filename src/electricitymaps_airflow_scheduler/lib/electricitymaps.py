import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum

import requests

"""
Here we will have the logic to call Electricity Maps' API to schedule operations within an Airflow pipeline
"""

BASE_URL = "https://api.electricitymaps.com/"
API_VERSION = "beta"
CONSUMPTION_OPTIMIZER_URL = f"{BASE_URL}{API_VERSION}/carbon-aware-optimizer"

_API_TOKEN = os.getenv("ELECTRICITYMAPS_API_TOKEN")


class OptimizationSignal(Enum):
    FLOW_TRACED_CARBON_INTENSITY = "flow-traced_carbon_intensity"
    FLOW_TRACED_RENEWABLE_SHARE = "flow-traced_renewable_share"
    NET_LOAD = "net_load"


DEFAULT_OPTIMIZATION_SIGNAL = OptimizationSignal.FLOW_TRACED_CARBON_INTENSITY


@dataclass
class OptimizationOutput:
    metric_value_immediate_execution: float
    metric_value_optimal_execution: float
    metric_value_start_window_execution: float
    metric_unit: str
    optimization_metric: OptimizationSignal
    zone_key: str


@dataclass
class OptimizerResponse:
    optimal_start_time: datetime
    optimal_location: tuple[float, float]  # lon, lat
    optimization_output: OptimizationOutput


def parse_optimizer_response(response: dict) -> OptimizerResponse:
    return OptimizerResponse(
        optimal_start_time=datetime.fromisoformat(
            response["optimalStartTime"].strip("Z")
        ).replace(tzinfo=timezone.utc),
        optimal_location=response["optimalLocation"],
        optimization_output=OptimizationOutput(
            **{
                k
                if "_" in k
                else "".join(["_" + c.lower() if c.isupper() else c for c in k]).lstrip(
                    "_"
                ): v
                for k, v in response["optimizationOutput"].items()
            }
        ),
    )


def schedule_execution(
    expected_duration_hours: float,
    end_datetime: datetime,
    optimization_signal: OptimizationSignal,
    locations: list[tuple[float, float]],
    power_output: float | None = None,
) -> OptimizerResponse:
    if not locations:
        raise ValueError("No locations provided")

    ceiled_expected_duration_hours = math.ceil(expected_duration_hours)
    body = {
        "duration": f"PT{ceiled_expected_duration_hours}H",
        "startWindow": (
            (
                datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
                + timedelta(hours=1)
            ).isoformat()
        ),
        "endWindow": end_datetime.replace(
            minute=0, second=0, microsecond=0
        ).isoformat(),
        "locations": [[lon, lat] for lat, lon in locations],
        "optimizationMetric": optimization_signal.value,
    }
    if power_output:
        body["powerConsumption"] = power_output

    headers = {
        "auth-token": _API_TOKEN,
        "Content-Type": "application/json",
    }
    try:
        response = requests.post(CONSUMPTION_OPTIMIZER_URL, headers=headers, json=body)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise e
    return parse_optimizer_response(response.json())
