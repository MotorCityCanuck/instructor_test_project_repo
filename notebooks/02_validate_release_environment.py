"""Databricks task notebook for validating Raw-to-Bronze release environment."""

# Databricks notebook source

# COMMAND ----------
# Title: 02 Validate Release Environment
# Purpose:
# Resolve Raw-to-Bronze configuration for one release and validate or create the
# required catalog, schemas, and raw volume in Databricks.

NOTEBOOK_VERSION = "2026.07.15.1"

print(f"Notebook version: {NOTEBOOK_VERSION}")

from pathlib import Path


def _load_bootstrap_helper() -> None:
    """Load the shared notebook bootstrap helper."""
    search_roots = []

    if "__file__" in globals():
        search_roots.append(Path(__file__).resolve().parent)

    current_dir = Path.cwd().resolve()
    search_roots.extend([current_dir, *current_dir.parents])

    for root in search_roots:
        for candidate in (
            root / "_bootstrap_napa_pipeline.py",
            root / "notebooks" / "_bootstrap_napa_pipeline.py",
        ):
            if candidate.exists():
                exec(candidate.read_text(), globals())
                return

    raise FileNotFoundError(
        "Could not locate '_bootstrap_napa_pipeline.py'. "
        "Run this notebook from the repository workspace."
    )


_load_bootstrap_helper()
bootstrap_napa_pipeline_imports()

from napa_pipeline.raw_to_bronze.config import load_raw_to_bronze_config
from napa_pipeline.raw_to_bronze.environment import ensure_release_environment
from napa_pipeline.raw_to_bronze.operations import (
    PIPELINE_RUNS_TABLE,
    RUN_MESSAGES_TABLE,
    append_records,
    build_pipeline_run_start_record,
    build_run_message_record,
    create_pipeline_context,
    ensure_operations_tables,
    utc_now,
)

# COMMAND ----------
dbutils.widgets.dropdown("release_name", "napa_5k", ["napa_5k", "napa_50k", "napa_250k"])
dbutils.widgets.text("config_root", "")
dbutils.widgets.dropdown("create_missing", "true", ["true", "false"])
dbutils.widgets.text("pipeline_run_id", "")

release_name = dbutils.widgets.get("release_name").strip()
config_root = dbutils.widgets.get("config_root").strip() or None
create_missing = dbutils.widgets.get("create_missing").strip().lower() == "true"
pipeline_run_id = dbutils.widgets.get("pipeline_run_id").strip() or None

pipeline_started_ts = utc_now()
config = load_raw_to_bronze_config(release_name, config_root=config_root)
status = ensure_release_environment(spark, config, create_missing=create_missing)
context = create_pipeline_context(
    config,
    status.release_environment,
    pipeline_run_id=pipeline_run_id,
)
ensure_operations_tables(spark, context)
append_records(
    spark,
    f"{context.operations_schema_fqn}.{PIPELINE_RUNS_TABLE}",
    [build_pipeline_run_start_record(context, started_ts=pipeline_started_ts)],
)
append_records(
    spark,
    f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
    [
        build_run_message_record(
            context,
            message_level="INFO",
            message_code="CONFIG_LOADED",
            message_text=(
                f"Resolved configuration hash {config.config_hash} for release "
                f"{config.release_name}."
            ),
        ),
        build_run_message_record(
            context,
            message_level="INFO",
            message_code="ENVIRONMENT_VALIDATED",
            message_text="Release environment validation completed successfully.",
        )
    ],
)

# COMMAND ----------
print(f"Release name: {config.release_name}")
print(f"Catalog: {status.release_environment.catalog}")
print(f"Raw schema: {status.release_environment.raw_schema}")
print(f"Bronze schema: {status.release_environment.bronze_schema}")
print(f"Operations schema: {status.release_environment.operations_schema}")
print(f"Raw volume: {status.release_environment.raw_volume_fqn}")
print(f"Raw volume path: {status.release_environment.raw_volume_path}")

for schema_status in status.schema_statuses:
    state = "already existed" if schema_status.existed else "created"
    print(f"Schema {state}: {schema_status.object_name}")

volume_state = "already existed" if status.volume_status.existed else "created"
print(f"Volume {volume_state}: {status.volume_status.object_name}")

try:
    dbutils.jobs.taskValues.set(key="pipeline_run_id", value=context.pipeline_run_id)
    dbutils.jobs.taskValues.set(key="operations_schema_fqn", value=context.operations_schema_fqn)
except Exception:
    pass

display(
    spark.createDataFrame(
        [
            {
                "object_type": schema_status.object_type,
                "object_name": schema_status.object_name,
                "existed": schema_status.existed,
            }
            for schema_status in status.schema_statuses
        ]
        + [
            {
                "object_type": status.volume_status.object_type,
                "object_name": status.volume_status.object_name,
                "existed": status.volume_status.existed,
            }
        ]
    )
)
