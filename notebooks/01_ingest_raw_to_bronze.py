"""Databricks notebook for converting raw Parquet files into Bronze Delta tables."""

# Databricks notebook source

# COMMAND ----------
# Title: 01 Ingest Raw to Bronze
# Purpose:
# Load each Parquet file from the selected raw dataset folder and write it to a
# corresponding Delta table in the Bronze schema.

from pyspark.sql import functions as F

# MAGIC %run ./_shared_pipeline_config

# COMMAND ----------
register_pipeline_widgets(dbutils)
config = load_pipeline_config(dbutils)

CATALOG = config["catalog"]
RAW_SCHEMA = config["raw_schema"]
BRONZE_SCHEMA = config["bronze_schema"]
RAW_VOLUME = config["raw_volume"]
DATASET_NAME = config["dataset_name"]
source_path = config["resolved_source_path"]

print(f"Catalog: {CATALOG}")
print(f"Raw schema: {RAW_SCHEMA}")
print(f"Bronze schema: {BRONZE_SCHEMA}")
print(f"Raw volume: {RAW_VOLUME}")
print(f"Dataset name: {DATASET_NAME}")
print(f"Source path: {source_path}")

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

try:
    source_entries = dbutils.fs.ls(source_path)
except Exception as exc:
    raise FileNotFoundError(
        f"Could not access source path '{source_path}'. "
        "Confirm the volume, dataset folder, and permissions."
    ) from exc

parquet_entries = [entry for entry in source_entries if entry.path.endswith(".parquet")]

if not parquet_entries:
    raise FileNotFoundError(
        f"No Parquet files were found under '{source_path}'. "
        "Confirm the dataset folder contains delivered source files."
    )

print(f"Found {len(parquet_entries)} Parquet files to ingest.")

# COMMAND ----------
for entry in sorted(parquet_entries, key=lambda item: item.name):
    source_file = entry.name
    table_name = source_file.removesuffix(".parquet")
    target_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    table_existed = spark.catalog.tableExists(target_table)

    print(f"Reading source file: {entry.path}")
    dataframe = spark.read.parquet(entry.path).withColumn(
        "_source_file", F.lit(source_file)
    ).withColumn("_ingested_at", F.current_timestamp())

    row_count = dataframe.count()

    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    if table_existed:
        print(f"Overwrote existing Delta table: {target_table} ({row_count} rows)")
    else:
        print(f"Created new Delta table: {target_table} ({row_count} rows)")

# COMMAND ----------
print("Bronze ingestion complete.")
spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}").show(truncate=False)
