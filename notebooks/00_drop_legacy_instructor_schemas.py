"""Databricks notebook for dropping legacy instructor schemas."""

# Databricks notebook source

# COMMAND ----------
# Title: 00 Drop Legacy Instructor Schemas
# Purpose:
# Remove the legacy shared instructor schemas after the release-specific
# Raw-to-Bronze pipeline has been validated.

LEGACY_SCHEMAS = [
    "workspace.instructor_raw",
    "workspace.instructor_bronze",
    "workspace.instructor_silver",
    "workspace.instructor_gold",
]

for schema_name in LEGACY_SCHEMAS:
    print(f"Dropping schema if it exists: {schema_name}")
    spark.sql(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")

print("Legacy instructor schema cleanup complete.")
