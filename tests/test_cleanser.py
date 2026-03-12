"""
test_cleanser.py
Unit tests for src/transforms/cleanser.py
Uses a local SparkSession — no AWS credentials needed.
"""
 
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
 
from src.transforms.cleanser import Cleanser
 
 
@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_cleanser")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
 
 
def test_trim_whitespace(spark):
    df = spark.createDataFrame(
        [("  Alice  ", "  Engineering  "), ("Bob", "  Sales")],
        schema=StructType([
            StructField("name", StringType()),
            StructField("dept", StringType())
        ])
    )
    result = Cleanser.trim_whitespace(df)
    rows = result.collect()
 
    assert rows[0]["name"] == "Alice"
    assert rows[0]["dept"] == "Engineering"
    assert rows[1]["dept"] == "Sales"
 
 
def test_standardize_nulls(spark):
    df = spark.createDataFrame(
        [("N/A", "active"), ("null", "inactive"), ("valid", "NA")],
        schema=StructType([
            StructField("code", StringType()),
            StructField("status", StringType())
        ])
    )
    result = Cleanser.standardize_nulls(df)
    rows = result.collect()
 
    assert rows[0]["code"] is None
    assert rows[1]["code"] is None
    assert rows[2]["status"] is None
    assert rows[2]["code"] == "valid"
 
 
def test_remove_duplicates(spark):
    df = spark.createDataFrame(
        [("A", 1), ("A", 1), ("B", 2), ("B", 2), ("C", 3)],
        schema=StructType([
            StructField("key", StringType()),
            StructField("val", IntegerType())
        ])
    )
    result = Cleanser.remove_duplicates(df)
    assert result.count() == 3
 
 
def test_normalize_column_names(spark):
    df = spark.createDataFrame(
        [("test",)],
        schema=StructType([StructField("First Name", StringType())])
    )
    result = Cleanser.normalize_column_names(df)
    assert "first_name" in result.columns
    assert "First Name" not in result.columns
 
 
def test_standardize_boolean(spark):
    df = spark.createDataFrame(
        [("yes",), ("no",), ("true",), ("0",), ("invalid",)],
        schema=StructType([StructField("flag", StringType())])
    )
    result = Cleanser.standardize_boolean(df, columns=["flag"])
    rows = result.collect()
 
    assert rows[0]["flag"] is True
    assert rows[1]["flag"] is False
    assert rows[2]["flag"] is True
    assert rows[3]["flag"] is False
    assert rows[4]["flag"] is None
