"""
conftest.py
Shared pytest fixtures for the test suite.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Single SparkSession shared across the entire test session.
    Uses local mode — no cluster or AWS connection required.
    """
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("etl-framework-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
