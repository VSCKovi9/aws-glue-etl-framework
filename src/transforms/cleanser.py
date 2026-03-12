"""
cleanser.py
PySpark transformation functions for data cleansing.
Each function takes a DataFrame and returns a cleaned DataFrame.
Designed to be chained using .transform().
"""
 
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from typing import List, Optional
import logging
 
logger = logging.getLogger(__name__)
 
 
class Cleanser:
    """Collection of reusable PySpark cleansing transforms."""
 
    @staticmethod
    def trim_whitespace(df: DataFrame, columns: Optional[List[str]] = None) -> DataFrame:
        """
        Trim leading and trailing whitespace from string columns.
        If columns is None, applies to all StringType columns.
        """
        target_cols = columns or [
            f.name for f in df.schema.fields if isinstance(f.dataType, StringType)
        ]
        for col in target_cols:
            df = df.withColumn(col, F.trim(F.col(col)))
 
        logger.info(f"Trimmed whitespace from {len(target_cols)} columns")
        return df
 
    @staticmethod
    def standardize_nulls(
        df: DataFrame,
        null_values: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Replace common null-like string values with actual nulls.
        Default null strings: 'N/A', 'NA', 'null', 'NULL', 'none', 'None', '', '-'
        """
        null_strings = null_values or ["N/A", "NA", "null", "NULL", "none", "None", "", "-"]
        string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
 
        for col in string_cols:
            df = df.withColumn(
                col,
                F.when(F.col(col).isin(null_strings), None).otherwise(F.col(col))
            )
 
        logger.info(f"Standardized null values across {len(string_cols)} string columns")
        return df
 
    @staticmethod
    def remove_duplicates(df: DataFrame, subset: Optional[List[str]] = None) -> DataFrame:
        """
        Drop exact duplicate rows. If subset is provided, deduplicates
        based on those columns only (keeps first occurrence).
        """
        before = df.count()
        df = df.dropDuplicates(subset)
        after = df.count()
        dropped = before - after
 
        logger.info(f"Removed {dropped} duplicate rows ({before} -> {after})")
        return df
 
    @staticmethod
    def normalize_column_names(df: DataFrame) -> DataFrame:
        """
        Lowercase all column names and replace spaces and special
        characters with underscores. Ensures consistent naming.
        """
        import re
        new_cols = {
            col: re.sub(r"[^a-z0-9_]", "_", col.lower().strip())
            for col in df.columns
        }
        for old, new in new_cols.items():
            if old != new:
                df = df.withColumnRenamed(old, new)
 
        logger.info("Normalized column names")
        return df
 
    @staticmethod
    def drop_empty_rows(df: DataFrame, required_columns: List[str]) -> DataFrame:
        """
        Drop rows where all of the specified required_columns are null.
        Useful for filtering out completely empty records from source files.
        """
        before = df.count()
        condition = F.lit(True)
        for col in required_columns:
            condition = condition & F.col(col).isNull()
 
        df = df.filter(~condition)
        after = df.count()
 
        logger.info(f"Dropped {before - after} empty rows based on required columns: {required_columns}")
        return df
 
    @staticmethod
    def standardize_boolean(df: DataFrame, columns: List[str]) -> DataFrame:
        """
        Normalize boolean-like string values to True/False.
        Handles: 'yes'/'no', '1'/'0', 'true'/'false', 'y'/'n'
        """
        true_values = ["true", "yes", "y", "1"]
        false_values = ["false", "no", "n", "0"]
 
        for col in columns:
            df = df.withColumn(
                col,
                F.when(F.lower(F.col(col)).isin(true_values), True)
                 .when(F.lower(F.col(col)).isin(false_values), False)
                 .otherwise(None)
            )
 
        logger.info(f"Standardized boolean columns: {columns}")
        return df
 
