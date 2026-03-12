"""
job_tracker.py
DynamoDB-backed job state tracking for idempotent pipeline reruns.
Stores job status, row counts, and timestamps per run.
"""

import boto3
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

JOB_STATUS_RUNNING = "RUNNING"
JOB_STATUS_SUCCESS = "SUCCESS"
JOB_STATUS_FAILED  = "FAILED"


class JobTracker:
    """
    Tracks ETL job runs in a DynamoDB table.

    Table schema:
        PK: job_name (String)
        SK: run_id   (String)
        Attributes: status, start_time, end_time, rows_read,
                    rows_written, error_message
    """

    def __init__(
        self,
        table_name: str,
        job_name: str,
        run_id: str,
        region: str = "us-east-1"
    ):
        self.table_name = table_name
        self.job_name = job_name
        self.run_id = run_id
        self._dynamodb = boto3.resource("dynamodb", region_name=region)
        self._table = self._dynamodb.Table(table_name)

    def start(self) -> None:
        """Mark the job as RUNNING in DynamoDB."""
        self._put_item(status=JOB_STATUS_RUNNING)
        logger.info(f"Job tracker: {self.job_name}/{self.run_id} marked as RUNNING")

    def succeed(self, rows_read: int = 0, rows_written: int = 0) -> None:
        """Mark the job as SUCCESS with row counts."""
        self._put_item(
            status=JOB_STATUS_SUCCESS,
            end_time=self._now(),
            rows_read=rows_read,
            rows_written=rows_written
        )
        logger.info(
            f"Job tracker: {self.job_name}/{self.run_id} marked as SUCCESS "
            f"({rows_read:,} read, {rows_written:,} written)"
        )

    def fail(self, error_message: str) -> None:
        """Mark the job as FAILED with an error message."""
        self._put_item(
            status=JOB_STATUS_FAILED,
            end_time=self._now(),
            error_message=str(error_message)[:1000]   # DynamoDB item size limit guard
        )
        logger.error(
            f"Job tracker: {self.job_name}/{self.run_id} marked as FAILED: {error_message}"
        )

    def get_last_successful_run(self) -> Optional[dict]:
        """
        Returns the most recent SUCCESS record for this job, or None.
        Useful for incremental load watermarking.
        """
        try:
            response = self._table.query(
                KeyConditionExpression="job_name = :jn",
                FilterExpression="#s = :status",
                ExpressionAttributeValues={
                    ":jn": self.job_name,
                    ":status": JOB_STATUS_SUCCESS
                },
                ExpressionAttributeNames={"#s": "status"},
                ScanIndexForward=False,
                Limit=1
            )
            items = response.get("Items", [])
            return items[0] if items else None
        except Exception as e:
            logger.warning(f"Could not fetch last successful run: {e}")
            return None

    def _put_item(self, **kwargs) -> None:
        item = {
            "job_name": self.job_name,
            "run_id": self.run_id,
            "start_time": self._now(),
            **kwargs
        }
        self._table.put_item(Item=item)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()
