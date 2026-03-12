"""
s3_connector.py
Read and write PySpark DataFrames to/from Amazon S3.
Supports Parquet, JSON, CSV, and Delta formats.
Handles partition pruning on read and partition-aware writes.
"""
 
from pyspark.sql import DataFrame, SparkSession
from typing import List, Optional
import logging
 
logger = logging.getLogger(__name__)
 
SUPPORTED_FORMATS = {"parquet", "json", "csv", "delta"}
 
 
class S3Connector:
    """Handles S3 reads and writes for Glue ETL jobs."""
 
    def __init__(self, spark: SparkSession):
        self.spark = spark
 
    def read(
        self,
        path: str,
        fmt: str = "parquet",
        schema=None,
        options: Optional[dict] = None
    ) -> DataFrame:
        """
        Read data from S3 into a DataFrame.
 
        Args:
            path:    S3 URI (e.g. s3://bucket/prefix/)
            fmt:     File format — parquet, json, csv, delta
            schema:  Optional StructType schema for schema enforcement
            options: Additional Spark reader options (e.g. {"header": "true"})
 
        Returns:
            PySpark DataFrame
        """
        self._validate_format(fmt)
        reader = self.spark.read.format(fmt)
 
        if schema:
            reader = reader.schema(schema)
        if options:
            reader = reader.options(**options)
 
        df = reader.load(path)
        logger.info(f"Read from S3: {path} | format={fmt} | rows={df.count():,}")
        return df
 
    def write(
        self,
        df: DataFrame,
        path: str,
        fmt: str = "parquet",
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        options: Optional[dict] = None
    ) -> None:
        """
        Write a DataFrame to S3.
 
        Args:
            df:           DataFrame to write
            path:         S3 destination URI
            fmt:          Output format — parquet, json, csv, delta
            mode:         Write mode — overwrite, append, ignore, error
            partition_by: Columns to partition the output by (e.g. ['year', 'month'])
            options:      Additional Spark writer options
        """
        self._validate_format(fmt)
        writer = df.write.format(fmt).mode(mode)
 
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if options:
            writer = writer.options(**options)
 
        writer.save(path)
        logger.info(
            f"Written to S3: {path} | format={fmt} | mode={mode} "
            f"| partitioned_by={partition_by}"
        )
 
    def _validate_format(self, fmt: str) -> None:
        if fmt not in SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format '{fmt}'. Must be one of: {SUPPORTED_FORMATS}"
            )
