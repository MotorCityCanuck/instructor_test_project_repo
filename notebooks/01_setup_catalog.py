"""Databricks notebook scaffold for catalog, schema, and raw volume setup."""

# Databricks notebook source

# COMMAND ----------
# Title: 01 Setup Catalog
# Purpose:
# Create the team-scoped schemas and raw volume used by the Databricks workflow.

CATALOG = "workspace"
TEAM_PREFIX = "team03"  # Change once for your assigned team

RAW_SCHEMA = f"{TEAM_PREFIX}_raw"
BRONZE_SCHEMA = f"{TEAM_PREFIX}_bronze"
SILVER_SCHEMA = f"{TEAM_PREFIX}_silver"
GOLD_SCHEMA = f"{TEAM_PREFIX}_gold"
OPS_SCHEMA = f"{TEAM_PREFIX}_ops"
RAW_VOLUME = "napa_files"

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
