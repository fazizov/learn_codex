# Databricks notebook source
"""Lakeflow Declarative Pipeline: sample retail data to Bronze.

Attach this Python source file to a Databricks Lakeflow Declarative Pipeline.
The pipeline creates managed Bronze streaming tables for Databricks sample
retail CSV datasets by incrementally ingesting them with Auto Loader.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ORDERS_SOURCE_PATH = "/databricks-datasets/retail-org/sales_orders/"
CUSTOMERS_SOURCE_PATH = "/databricks-datasets/retail-org/customers/"


def read_bronze_csv(source_path):
    """Read CSV files from a source path into a Bronze streaming DataFrame."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(source_path)
        .withColumn("_bronze_ingested_at", F.current_timestamp())
        .withColumn("_bronze_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="bronze_sample_sales_orders",
    comment="Bronze ingestion of Databricks sample retail sales order CSV files.",
    table_properties={"quality": "bronze"},
)
def bronze_sample_sales_orders():
    """Read Databricks sample sales-order files into a Bronze streaming table."""
    return read_bronze_csv(ORDERS_SOURCE_PATH)


@dp.table(
    name="bronze_sample_customers",
    comment="Bronze ingestion of Databricks sample retail customer CSV files.",
    table_properties={"quality": "bronze"},
)
def bronze_sample_customers():
    """Read Databricks sample customer files into a Bronze streaming table."""
    return read_bronze_csv(CUSTOMERS_SOURCE_PATH)
