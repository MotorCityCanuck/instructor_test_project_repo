"""Legacy notebook retained only as a redirect to the new Raw-to-Bronze flow."""

# Databricks notebook source

# COMMAND ----------
# Title: 01 Ingest Raw to Bronze
# Purpose:
# This notebook is legacy and is no longer the authoritative Raw-to-Bronze
# implementation path.

message = """
Legacy notebook notice:

`01_ingest_raw_to_bronze.py` is retained only for historical context.
Do not use it for the rebuilt Raw-to-Bronze pipeline.

Use these notebooks instead:
- `notebooks/01_resolve_configuration.py`
- `notebooks/02_validate_release_environment.py`

The rebuilt pipeline targets release-specific schemas such as:
- `workspace.instructor_5k_raw`
- `workspace.instructor_5k_bronze`
- `workspace.instructor_50k_raw`
- `workspace.instructor_50k_bronze`
- `workspace.instructor_250k_raw`
- `workspace.instructor_250k_bronze`
"""

print(message.strip())

raise RuntimeError(
    "Legacy notebook: use the new Raw-to-Bronze task notebooks instead."
)
