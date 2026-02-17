[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Checked with mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)

# APScheduler Exporter
A Prometheus exporter for APScheduler metrics

# Usage
## Basic Example
```python
from apscheduler_metrics import APSchedulerExporter

scheduler_exporter = APSchedulerExporter(scheduler=your_scheduler)
scheduler_exporter.start_http_server()
```

## Configuration
- port (default value: 8888)
- listen address (default value: '0.0.0.0')


# Metrics

Name     | Description                                       | Type
---------|---------------------------------------------------|----
apscheduler_job_done_total | Sent when a job is done.                          | Counter
apscheduler_job_errors_total | Sent when a job is failed.                        | Counter
apscheduler_job_missed_total | Sent when a job is missed.                        | Counter
apscheduler_job_last_duration_seconds | The runtime (seconds) for the last job execution. | Gauge
