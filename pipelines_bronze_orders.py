# Databricks notebook source
"""Lakeflow Declarative Pipeline: sample orders to Bronze.

Attach this Python source file to a Databricks Lakeflow Declarative Pipeline.
The pipeline creates a managed Bronze streaming table named
``bronze_sample_sales_orders`` by incrementally ingesting a Databricks sample
CSV dataset with Auto Loader.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE_PATH = "/databricks-datasets/retail-org/sales_orders/"


@dp.table(
    name="bronze_sample_sales_orders",
    comment="Bronze ingestion of Databricks sample retail sales order CSV files.",
    table_properties={"quality": "bronze"},
)
def bronze_sample_sales_orders():
    """Read Databricks sample sales-order files into a Bronze streaming table."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(SOURCE_PATH)
        .withColumn("_bronze_ingested_at", F.current_timestamp())
        .withColumn("_bronze_source_file", F.col("_metadata.file_path"))
    )
