"""
logger.py
Structured JSON logger for AWS Glue ETL jobs.
All log entries are formatted as JSON so they are queryable
in CloudWatch Logs Insights with zero additional configuration.
"""
 
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any
 
 
class ETLLogger:
    """
    Wraps Python's standard logger with structured JSON output.
 
    Usage:
        logger = ETLLogger(job_name="customer_staging", run_id="jr_abc123")
        logger.info("transform_complete", rows_in=1_200_000, rows_out=1_198_340)
        logger.error("job_failed", error="NullPointerException", stage="cleanse")
 
    CloudWatch Logs Insights query example:
        fields @timestamp, job_name, event, rows_in, rows_out
        | filter level = "ERROR"
        | sort @timestamp desc
    """
 
    def __init__(self, job_name: str, run_id: str, level: int = logging.INFO):
        self.job_name = job_name
        self.run_id = run_id
        self._start_time = time.time()
 
        self._logger = logging.getLogger(job_name)
        self._logger.setLevel(level)
 
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(message)s"))
            self._logger.addHandler(handler)
 
    def _build_entry(self, level: str, event: str, **kwargs: Any) -> str:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "job_name": self.job_name,
            "run_id": self.run_id,
            "elapsed_sec": round(time.time() - self._start_time, 2),
            "event": event,
            **kwargs
        }
        return json.dumps(entry)
 
    def info(self, event: str, **kwargs: Any) -> None:
        self._logger.info(self._build_entry("INFO", event, **kwargs))
 
    def warning(self, event: str, **kwargs: Any) -> None:
        self._logger.warning(self._build_entry("WARNING", event, **kwargs))
 
    def error(self, event: str, **kwargs: Any) -> None:
        self._logger.error(self._build_entry("ERROR", event, **kwargs))
 
    def debug(self, event: str, **kwargs: Any) -> None:
        self._logger.debug(self._build_entry("DEBUG", event, **kwargs))
