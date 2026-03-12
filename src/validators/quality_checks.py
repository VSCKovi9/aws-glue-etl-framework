"""
quality_checks.py
Data quality validation checks for PySpark DataFrames.
Runs null rate, uniqueness, and row count checks before loading to target.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from dataclasses import dataclass, field
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class QualityCheckResult:
    passed: bool
    failures: List[str] = field(default_factory=list)

    def summary(self) -> str:
        if self.passed:
            return "All quality checks passed."
        return "Quality checks FAILED:\n" + "\n".join(f"  - {f}" for f in self.failures)


class QualityChecker:
    """
    Runs configurable data quality checks against a DataFrame.
    Intended to be run after cleansing, before loading to the target layer.
    """

    def __init__(self, config: dict):
        """
        Args:
            config: quality_checks block from the job config JSON.
                    Keys: null_threshold, uniqueness_keys, row_count_min
        """
        self.null_threshold = config.get("null_threshold", 0.05)
        self.uniqueness_keys = config.get("uniqueness_keys", [])
        self.row_count_min = config.get("row_count_min", 0)

    def run(self, df: DataFrame) -> QualityCheckResult:
        """
        Run all configured checks and return a QualityCheckResult.
        """
        failures = []

        failures += self._check_null_rates(df)
        failures += self._check_uniqueness(df)
        failures += self._check_row_count(df)

        passed = len(failures) == 0
        result = QualityCheckResult(passed=passed, failures=failures)

        if passed:
            logger.info(result.summary())
        else:
            logger.error(result.summary())

        return result

    def _check_null_rates(self, df: DataFrame) -> List[str]:
        """Fail if any column exceeds the configured null rate threshold."""
        failures = []
        total = df.count()
        if total == 0:
            return ["Row count is 0 — cannot compute null rates"]

        for col_name in df.columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_rate = null_count / total

            if null_rate > self.null_threshold:
                failures.append(
                    f"Column '{col_name}' has null rate {null_rate:.2%} "
                    f"(threshold: {self.null_threshold:.2%})"
                )

        return failures

    def _check_uniqueness(self, df: DataFrame) -> List[str]:
        """Fail if uniqueness key columns contain duplicate values."""
        failures = []
        if not self.uniqueness_keys:
            return failures

        total = df.count()
        distinct = df.select(*self.uniqueness_keys).distinct().count()

        if distinct < total:
            failures.append(
                f"Uniqueness check failed on {self.uniqueness_keys}: "
                f"{total - distinct} duplicate rows detected"
            )

        return failures

    def _check_row_count(self, df: DataFrame) -> List[str]:
        """Fail if the DataFrame has fewer rows than the configured minimum."""
        failures = []
        if self.row_count_min <= 0:
            return failures

        count = df.count()
        if count < self.row_count_min:
            failures.append(
                f"Row count {count:,} is below minimum threshold {self.row_count_min:,}"
            )

        return failures
