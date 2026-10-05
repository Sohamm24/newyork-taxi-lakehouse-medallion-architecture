import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Provides a lightweight, in-memory PySpark session for fast unit testing.
    Uses local[1] and 1 shuffle partition to minimize CPU and RAM footprint during tests.
    """
    spark_session = (
        SparkSession.builder
        .appName("unit-tests")
        .master("local[1]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()
