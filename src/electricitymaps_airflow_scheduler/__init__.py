__version__ = "0.1.0"


def get_provider_info():
    return {
        "package-name": "electricitymaps-airflow-scheduler",
        "name": "ElectricityMaps",
        "description": "Electricity Maps' Airflow provider to schedule operations within an Airflow pipeline",
        "connection-types": [],
        "extra-links": [
            "src.electricitymaps_airflow_scheduler.scheduler.ElectricityMapsSchedulerOperatorExtraLink"
        ],
        "versions": [__version__],
    }
