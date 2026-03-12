"""
test_quality_checks.py
Unit tests for src/validators/quality_checks.py
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.validators.quality_checks import QualityChecker


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_quality_checks")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def test_null_rate_passes(spark):
    df = spark.createDataFrame(
        [("A", 1), ("B", 2), ("C", 3), ("D", 4), ("E", 5)],
        schema=StructType([
            StructField("key", StringType()),
            StructField("val", IntegerType())
        ])
    )
    checker = QualityChecker({"null_threshold": 0.05})
    result = checker.run(df)
    assert result.passed


def test_null_rate_fails(spark):
    df = spark.createDataFrame(
        [("A", 1), (None, 2), (None, 3), (None, 4), ("E", 5)],
        schema=StructType([
            StructField("key", StringType()),
            StructField("val", IntegerType())
        ])
    )
    checker = QualityChecker({"null_threshold": 0.05})
    result = checker.run(df)
    assert not result.passed
    assert any("key" in f for f in result.failures)


def test_uniqueness_passes(spark):
    df = spark.createDataFrame(
        [("id1", "Alice"), ("id2", "Bob"), ("id3", "Carol")],
        schema=StructType([
            StructField("customer_id", StringType()),
            StructField("name", StringType())
        ])
    )
    checker = QualityChecker({"uniqueness_keys": ["customer_id"]})
    result = checker.run(df)
    assert result.passed


def test_uniqueness_fails(spark):
    df = spark.createDataFrame(
        [("id1", "Alice"), ("id1", "Alice Duplicate"), ("id3", "Carol")],
        schema=StructType([
            StructField("customer_id", StringType()),
            StructField("name", StringType())
        ])
    )
    checker = QualityChecker({"uniqueness_keys": ["customer_id"]})
    result = checker.run(df)
    assert not result.passed


def test_row_count_passes(spark):
    df = spark.createDataFrame(
        [("A",), ("B",), ("C",)],
        schema=StructType([StructField("val", StringType())])
    )
    checker = QualityChecker({"row_count_min": 3})
    result = checker.run(df)
    assert result.passed


def test_row_count_fails(spark):
    df = spark.createDataFrame(
        [("A",), ("B",)],
        schema=StructType([StructField("val", StringType())])
    )
    checker = QualityChecker({"row_count_min": 100})
    result = checker.run(df)
    assert not result.passed
    assert any("Row count" in f for f in result.failures)
