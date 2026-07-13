"""Databricks notebook scaffold for catalog, schema, and raw volume setup."""

# Databricks notebook source

# COMMAND ----------
# Title: 01 Setup Catalog
# Purpose:
# Create the team-scoped schemas and raw volume used by the Databricks workflow.

# MAGIC %run ./_shared_pipeline_config

# COMMAND ----------
register_pipeline_widgets(dbutils)
config = load_pipeline_config(dbutils)

CATALOG = config["catalog"]
RAW_SCHEMA = config["raw_schema"]
BRONZE_SCHEMA = config["bronze_schema"]
SILVER_SCHEMA = config["silver_schema"]
GOLD_SCHEMA = config["gold_schema"]
OPS_SCHEMA = config["ops_schema"]
RAW_VOLUME = config["raw_volume"]

existing_schemas = {
    row.databaseName for row in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()
}

for schema_name in [
    RAW_SCHEMA,
    BRONZE_SCHEMA,
    SILVER_SCHEMA,
    GOLD_SCHEMA,
    OPS_SCHEMA,
]:
    schema_existed = schema_name in existing_schemas
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema_name}")
    if schema_existed:
        print(f"Schema already existed: {CATALOG}.{schema_name}")
    else:
        print(f"Created schema: {CATALOG}.{schema_name}")

existing_volumes = {
    row.volume_name
    for row in spark.sql(f"SHOW VOLUMES IN {CATALOG}.{RAW_SCHEMA}").collect()
}
volume_existed = RAW_VOLUME in existing_volumes
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}.{RAW_VOLUME}")
if volume_existed:
    print(f"Volume already existed: {CATALOG}.{RAW_SCHEMA}.{RAW_VOLUME}")
else:
    print(f"Created volume: {CATALOG}.{RAW_SCHEMA}.{RAW_VOLUME}")

RAW_VOLUME_PATH = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{RAW_VOLUME}"

print("Raw volume path:", RAW_VOLUME_PATH)
spark.sql(f"SHOW SCHEMAS IN {CATALOG}").show(truncate=False)
