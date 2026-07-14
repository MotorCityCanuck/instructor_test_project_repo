"""Databricks notebook for converting raw Parquet files into Bronze Delta tables."""

# Databricks notebook source

# COMMAND ----------
# Title: 01 Ingest Raw to Bronze
# Purpose:
# Load each Parquet file from the selected raw dataset folder and write it to a
# corresponding Delta table in the Bronze schema.

from pathlib import Path
import uuid

from pyspark.sql import functions as F
from pyspark.sql import types as T


def _load_shared_pipeline_config() -> None:
    """Load shared notebook config helpers without relying on notebook magic."""
    search_roots = []

    if "__file__" in globals():
        search_roots.append(Path(__file__).resolve().parent)

    current_dir = Path.cwd().resolve()
    search_roots.extend([current_dir, *current_dir.parents])

    for root in search_roots:
        candidate = root / "_shared_pipeline_config.py"
        if candidate.exists():
            exec(candidate.read_text(), globals())
            return

        candidate = root / "notebooks" / "_shared_pipeline_config.py"
        if candidate.exists():
            exec(candidate.read_text(), globals())
            return

    raise FileNotFoundError(
        "Could not locate '_shared_pipeline_config.py'. "
        "Run this file from the repository workspace or keep the shared "
        "config helper under notebooks/."
    )


_load_shared_pipeline_config()

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
def _read_parquet_with_uuid_fallback(parquet_path: str, source_file: str):
    """Read a Parquet file, falling back to PyArrow for native UUID columns."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as import_exc:
        raise ModuleNotFoundError(
            "Spark could not read a Parquet UUID column and PyArrow is not "
            "available for the fallback path. Install PyArrow on the cluster "
            "or rewrite the source file with UUID columns cast to strings."
        ) from import_exc

    print(
        f"Spark could not read native UUID columns in {source_file}; "
        "using PyArrow fallback and converting UUID values to strings."
    )

    if parquet_path.startswith("dbfs:/Volumes/"):
        pyarrow_path = parquet_path.replace("dbfs:/Volumes/", "/Volumes/", 1)
    elif parquet_path.startswith("dbfs:/"):
        pyarrow_path = parquet_path.replace("dbfs:/", "/dbfs/", 1)
    else:
        pyarrow_path = parquet_path

    arrow_table = pq.read_table(pyarrow_path)
    records = arrow_table.to_pylist()

    def _arrow_type_to_spark_type(arrow_type):
        type_text = str(arrow_type).lower()

        if "uuid" in type_text:
            return T.StringType()
        if pa.types.is_null(arrow_type):
            return T.StringType()
        if pa.types.is_boolean(arrow_type):
            return T.BooleanType()
        if pa.types.is_int8(arrow_type):
            return T.ByteType()
        if pa.types.is_int16(arrow_type):
            return T.ShortType()
        if pa.types.is_integer(arrow_type):
            return T.LongType()
        if pa.types.is_float32(arrow_type):
            return T.FloatType()
        if pa.types.is_floating(arrow_type):
            return T.DoubleType()
        if pa.types.is_decimal(arrow_type):
            return T.DecimalType(arrow_type.precision, arrow_type.scale)
        if pa.types.is_date(arrow_type):
            return T.DateType()
        if pa.types.is_timestamp(arrow_type):
            return T.TimestampType()
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return T.StringType()
        if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return T.BinaryType()

        return T.StringType()

    def _convert_uuid_values(value):
        if isinstance(value, uuid.UUID):
            return str(value)
        if isinstance(value, dict):
            return {
                nested_key: _convert_uuid_values(nested_value)
                for nested_key, nested_value in value.items()
            }
        if isinstance(value, list):
            return [_convert_uuid_values(item) for item in value]
        return value

    normalized_records = [
        {
            column_name: _convert_uuid_values(column_value)
            for column_name, column_value in record.items()
        }
        for record in records
    ]

    spark_schema = T.StructType(
        [
            T.StructField(
                field.name,
                _arrow_type_to_spark_type(field.type),
                nullable=True,
            )
            for field in arrow_table.schema
        ]
    )

    return spark.createDataFrame(normalized_records, schema=spark_schema)


def _is_uuid_parquet_error(exc: Exception) -> bool:
    """Return True when an exception indicates unsupported Parquet UUID typing."""
    return "Illegal Parquet type: FIXED_LEN_BYTE_ARRAY (UUID)" in str(exc)


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
    try:
        base_dataframe = spark.read.parquet(entry.path)
        dataframe = base_dataframe.withColumn(
            "_source_file", F.lit(source_file)
        ).withColumn("_ingested_at", F.current_timestamp())
        row_count = dataframe.count()
    except Exception as exc:
        if not _is_uuid_parquet_error(exc):
            raise

        base_dataframe = _read_parquet_with_uuid_fallback(entry.path, source_file)
        dataframe = base_dataframe.withColumn(
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
