# Electricity Maps' Airflow Scheduler

Electricity Maps' Airflow provider to schedule operations within an Airflow pipeline

## Usage

```bash
export ELECTRICITYMAPS_API_TOKEN=`YOUR TOKEN`
```

And then use it in your Airflow pipeline, see `examples/scheduled_worflow.py`

## Development

```bash
poetry install
```


## Experiment

```bash
poetry run airflow standalone
```

credentials given at the beginning of the logs
