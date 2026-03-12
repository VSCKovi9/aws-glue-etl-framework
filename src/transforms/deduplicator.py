"""
deduplicator.py
Window-function based deduplication for PySpark DataFrames.
Handles SCD-style dedup where the most recent record per key is kept.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List
import logging

logger = logging.getLogger(__name__)


class Deduplicator:
    """Deduplication strategies for PySpark DataFrames."""

    @staticmethod
    def window_dedup(
        df: DataFrame,
        partition_by: List[str],
        order_by: str,
        descending: bool = True
    ) -> DataFrame:
        """
        Keep only the latest record per partition key using a window function.
        Commonly used for CDC (Change Data Capture) deduplication.

        Args:
            df: Input DataFrame
            partition_by: Columns that define a unique entity (e.g. ['customer_id'])
            order_by: Column to determine which record is "latest" (e.g. 'updated_at')
            descending: If True, highest value of order_by is kept (default True)

        Returns:
            DataFrame with one row per partition key
        """
        order_col = F.col(order_by).desc() if descending else F.col(order_by).asc()
        window = Window.partitionBy(*partition_by).orderBy(order_col)

        before = df.count()
        df = (
            df.withColumn("_row_num", F.row_number().over(window))
              .filter(F.col("_row_num") == 1)
              .drop("_row_num")
        )
        after = df.count()

        logger.info(
            f"Window dedup on {partition_by} by {order_by}: "
            f"{before} -> {after} rows ({before - after} duplicates removed)"
        )
        return df

    @staticmethod
    def hash_dedup(df: DataFrame, subset: List[str]) -> DataFrame:
        """
        Deduplicate rows by generating an MD5 hash of the specified columns.
        Keeps the first occurrence of each unique hash.

        Args:
            df: Input DataFrame
            subset: Columns to include in the hash

        Returns:
            Deduplicated DataFrame with a _row_hash column added
        """
        hash_expr = F.md5(
            F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in subset])
        )

        before = df.count()
        df = (
            df.withColumn("_row_hash", hash_expr)
              .dropDuplicates(["_row_hash"])
        )
        after = df.count()

        logger.info(
            f"Hash dedup on {subset}: {before} -> {after} rows"
        )
        return df
