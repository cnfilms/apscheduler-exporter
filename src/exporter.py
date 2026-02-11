import logging
from typing import Optional

from apscheduler.events import (
    EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED, JobExecutionEvent, JobSubmissionEvent,
    EVENT_JOB_ADDED
)
from prometheus_client import Counter, CollectorRegistry, start_http_server

logger = logging.getLogger(__name__)



class APSchedulerExporter:
    def __init__(self, scheduler, registry: Optional[CollectorRegistry] = None):
        self.scheduler = scheduler
        self.registry = registry or CollectorRegistry()
        self.job_metrics = {}
        self._http_server_started = False
        self._update_thread = None
        self._stop_update_thread = False
        self._jobs_cache = {}

        self._init_metrics()
        self._register_listeners()

    def _init_metrics(self):
        # self.jobs_total = Gauge(
        #     'apscheduler_jobs_total',
        #     'Total number of scheduled jobs',
        #     registry=self.registry
        # )

        # self.jobs_by_state = Gauge(
        #     'apscheduler_jobs_by_state',
        #     'Number of jobs by state',
        #     ['state'],
        #     registry=self.registry
        # )

        # self.next_run_time = Gauge(
        #     'apscheduler_job_next_run_timestamp',
        #     'Next run time as Unix timestamp',
        #     ['job_name'],
        #     registry=self.registry
        # )

        self.job_metrics = {
            EVENT_JOB_EXECUTED: Counter(
                'apscheduler_job_executions_total',
                'Total number of job executions',
                ['job_name'],
                registry=self.registry
            ),
            EVENT_JOB_ERROR: Counter(
                'apscheduler_job_errors_total',
                'Total number of job errors',
                ['job_name'],
                registry=self.registry
            ),
            EVENT_JOB_MISSED: Counter(
                'apscheduler_job_missed_total',
                'Total number of missed jobs',
                ['job_name'],
                registry=self.registry
            )
        }



    def _register_listeners(self):
        self.scheduler.add_listener(self._on_job_submitted, EVENT_JOB_ADDED)
        self.scheduler.add_listener(self._on_job_terminated, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED)

    def _on_job_submitted(self, event: JobSubmissionEvent):
        if event.job_id not in self._jobs_cache:
            if job := self.scheduler.get_job(event.job_id):
                # start_time = datetime.now()
                self._jobs_cache[event.job_id] = {"job_name": job.name}
                # self.job_last_start.labels(name=job.name).set(start_time.timestamp())

    def _on_job_terminated(self, event: JobExecutionEvent):
        if job_data := self._jobs_cache.get(event.job_id):
            # end_time = datetime.now()

            self.job_metrics[event.code].labels(job_name=job_data["job_name"]).inc()

            # self.job_last_end.labels(name=job_data["name"]).set(end_time.timestamp())

            # duration = (end_time - job_data["start_time"]).total_seconds()
            # self.job_duration.labels(name=job_data["name"]).observe(duration)

            del self._jobs_cache[event.job_id]

    # def update_job_info(self):
    #     jobs = self.scheduler.get_jobs()
    #     self.jobs_total.set(len(jobs))
    #
    #     states = {'active': 0, 'paused': 0}
    #
    #     for job in jobs:
    #         job_name = job.name
    #
    #         if job.next_run_time:
    #             states['active'] += 1
    #             self.next_run_time.labels(
    #                 job_name=job_name
    #             ).set(job.next_run_time.timestamp())
    #         else:
    #             states['paused'] += 1
    #
    #     for state, count in states.items():
    #         self.jobs_by_state.labels(state=state).set(count)

    # def _update_metrics_loop(self, interval: int):
    #     while not self._stop_update_thread:
    #         try:
    #             self.update_job_info()
    #         except Exception as e:
    #             logger.error(f"Error updating metrics: {e}")
    #         time.sleep(interval)

    def start_http_server(self, port: int = 8000, addr: str = '0.0.0.0',
                          update_interval: int = 15):
        if self._http_server_started:
            logger.warning("HTTP server already started")
            return

        try:
            start_http_server(port=port, addr=addr, registry=self.registry)
            self._http_server_started = True
            logger.info(f"Prometheus metrics server started on {addr}:{port}")

            self._stop_update_thread = False
            # self._update_thread = threading.Thread(
            #     target=self._update_metrics_loop,
            #     args=(update_interval,),
            #     daemon=True,
            #     name="APSchedulerMetricsUpdater"
            # )
            # self._update_thread.start()
            logger.info(f"Metrics update thread started (interval: {update_interval}s)")

        except OSError as e:
            if "Address already in use" in str(e):
                logger.error(f"Port {port} is already in use")
                raise
            raise

    def stop_http_server(self):
        if self._update_thread and self._update_thread.is_alive():
            self._stop_update_thread = True
            self._update_thread.join(timeout=5)
            logger.info("Metrics update thread stopped")
