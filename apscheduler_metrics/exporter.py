import logging
import time
from typing import Callable, Optional

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
    Summary,
    start_http_server,
)

logger = logging.getLogger(__name__)


class APSchedulerExporter:
    """Attributes:
    scheduler: The APScheduler instance to monitor.
    registry (CollectorRegistry): Prometheus registry used to expose metrics.
    """

    def __init__(
        self,
        scheduler,
        registry: Optional[CollectorRegistry] = None,
        job_name_extractor: Optional[Callable] = None,
        excluded_jobs_by_id: Optional[list] = None,
    ):
        """Args:
        scheduler: The APScheduler instance to monitor.
        registry (Optional[CollectorRegistry]): Prometheus registry to
            register metrics into.
        job_name_extractor (Optional[Callable]): A callable that takes a
            job ID (str) and returns a human-readable metric label (str).
        excluded_jobs_by_id (Optional[list]): A list of job ids to exclude
            from metrics collection.
        """
        self.scheduler = scheduler
        self.registry = registry or CollectorRegistry()
        self.excluded_jobs_by_id = excluded_jobs_by_id or []
        self._http_server_started = False
        self._jobs_cache: dict = {}
        self._job_name_extractor: Callable = job_name_extractor or (
            lambda job_id: job_id
        )

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

        self.job_duration_summary = Summary(
            "apscheduler_job_duration_seconds",
            "Job execution duration in seconds",
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
        if self._ignored_job(event.job_id):
            logger.debug(f"Job metrics skiped for {event.job_id}")
            return
        if event.job_id not in self._jobs_cache:
            job = self.scheduler.get_job(event.job_id)
            self._jobs_cache[event.job_id] = {
                "job_name": job.name if job else self._job_name_extractor(event.job_id),
                "start_time": time.time(),
            }

    def _on_job_terminated(self, event: JobExecutionEvent):
        if self._ignored_job(event.job_id):
            logger.debug(f"Job metrics skiped for {event.job_id}")
            return
        if job_data := self._jobs_cache.get(event.job_id):
            self.event_job_metrics[event.code].labels(
                job_name=job_data["job_name"]
            ).inc()
            duration = time.time() - job_data["start_time"]
            self.last_job_duration_metrics.labels(job_name=job_data["job_name"]).set(
                duration
            )
            self.job_duration_summary.labels(job_name=job_data["job_name"]).observe(
                duration
            )

            del self._jobs_cache[event.job_id]

    def _ignored_job(self, job_id: str) -> bool:
        return job_id.startswith("_") or job_id in self.excluded_jobs_by_id

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
