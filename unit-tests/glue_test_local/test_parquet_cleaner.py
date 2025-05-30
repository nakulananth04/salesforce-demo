import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

import parquet_cleaner as pc
import os

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import DecimalType
import parquet_cleaner as pc


@pytest.fixture(scope="session")
# def spark():
#     from pyspark.sql import SparkSession
#     return SparkSession.builder.master("local[*]").appName("test").getOrCreate()

def spark():
    return (
       SparkSession.builder
        .master("local[*]")
        .appName("test")
        .config("spark.sql.parquet.int64AsTimestamp.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )


def test_parse_timestamp_valid():
    ts = "2025-05-31_123456"
    dt = pc.parse_timestamp(ts)
    assert dt == datetime(2025, 5, 31, 12, 34, 56)


def test_parse_timestamp_invalid():
    assert pc.parse_timestamp("bad_format") is None


def test_validate_dataframe_logs_missing_columns(spark, caplog):
    schema = {"COL1": "string", "COL2": "int"}
    df = spark.createDataFrame([Row(COL1="value")])
    with caplog.at_level("INFO"):
        result = pc.validate_dataframe(df, schema, "test_table")
        assert not result
        assert "Missing Columns: ['COL2']" in caplog.text


def test_safe_cast_column_adds_missing(spark):
    df = spark.createDataFrame([{"col1": "123"}])
    result_df = pc.safe_cast_column(df, "col2", "int")
    assert "col2" in result_df.columns
    assert result_df.schema["col2"].dataType.simpleString() == "int"


def test_safe_cast_column_casts_existing(spark):
    df = spark.createDataFrame([{"col1": "123"}])
    result_df = pc.safe_cast_column(df, "col1", "int")
    assert result_df.schema["col1"].dataType.simpleString() == "int"


def test_chunked_function():
    l = list(range(10))
    chunks = list(pc.chunked(l, 3))
    assert len(chunks) == 4
    assert chunks[0] == [0, 1, 2]
    assert chunks[-1] == [9]

