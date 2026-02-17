import logging
import time
from typing import Optional

from apscheduler.events import (
    EVENT_JOB_ADDED,
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    EVENT_JOB_SUBMITTED,
    JobExecutionEvent,
    JobSubmissionEvent,
)
from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    start_http_server,
)

logger = logging.getLogger(__name__)


class APSchedulerExporter:
    def __init__(self, scheduler, registry: Optional[CollectorRegistry] = None):
        self.scheduler = scheduler
        self.registry = registry or CollectorRegistry()
        self._http_server_started = False
        self._jobs_cache: dict = {}

        self.event_job_metrics = {
            EVENT_JOB_EXECUTED: Counter(
                "apscheduler_job_done_total",
                "Total number of job done",
                ["job_name"],
                registry=self.registry,
            ),
            EVENT_JOB_ERROR: Counter(
                "apscheduler_job_errors_total",
                "Total number of job errors",
                ["job_name"],
                registry=self.registry,
            ),
            EVENT_JOB_MISSED: Counter(
                "apscheduler_job_missed_total",
                "Total number of missed jobs",
                ["job_name"],
                registry=self.registry,
            ),
        }

        self.last_job_duration_metrics = Gauge(
            "apscheduler_job_last_duration_seconds",
            "Duration of the last job execution in seconds",
            ["job_name"],
            registry=self.registry,
        )

        self.scheduler.add_listener(
            self._on_job_started, EVENT_JOB_ADDED | EVENT_JOB_SUBMITTED
        )
        self.scheduler.add_listener(
            self._on_job_terminated,
            EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED,
        )

    def _on_job_started(self, event: JobSubmissionEvent):
        if event.job_id not in self._jobs_cache and (
            job := self.scheduler.get_job(event.job_id)
        ):
            self._jobs_cache[event.job_id] = {
                "job_name": job.name,
                "start_time": time.time(),
            }

    def _on_job_terminated(self, event: JobExecutionEvent):
        if job_data := self._jobs_cache.get(event.job_id):
            self.event_job_metrics[event.code].labels(
                job_name=job_data["job_name"]
            ).inc()
            self.last_job_duration_metrics.labels(job_name=job_data["job_name"]).set(
                time.time() - job_data["start_time"]
            )

            del self._jobs_cache[event.job_id]

    def start_http_server(self, port: int = 8888, addr: str = "0.0.0.0"):
        if self._http_server_started:
            logger.warning("HTTP server already started")
            return

        try:
            start_http_server(port=port, addr=addr, registry=self.registry)
            self._http_server_started = True
            logger.info(f"Prometheus metrics server started on {addr}:{port}")

        except OSError as e:
            if "Address already in use" in str(e):
                logger.error(f"Port {port} is already in use")
                raise
            raise
