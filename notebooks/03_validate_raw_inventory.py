"""Databricks task notebook for validating Raw file inventory."""

# Databricks notebook source

# COMMAND ----------
# Title: 03 Validate Raw Inventory
# Purpose:
# Validate exact Raw file inventory, confirm configured sources are readable
# Parquet, and capture source metadata needed before Bronze publication.

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
from napa_pipeline.raw_to_bronze.environment import resolve_release_environment
from napa_pipeline.raw_to_bronze.inventory import (
    RawInventoryError,
    validate_raw_inventory_and_readiness,
)
from napa_pipeline.raw_to_bronze.operations import (
    RUN_MESSAGES_TABLE,
    SCHEMA_SNAPSHOTS_TABLE,
    append_records,
    build_run_message_record,
    build_schema_snapshot_records,
    create_pipeline_context,
    ensure_operations_tables,
)

# COMMAND ----------
dbutils.widgets.dropdown("release_name", "napa_5k", ["napa_5k", "napa_50k", "napa_250k"])
dbutils.widgets.text("config_root", "")
dbutils.widgets.text("pipeline_run_id", "")

release_name = dbutils.widgets.get("release_name").strip()
config_root = dbutils.widgets.get("config_root").strip() or None
pipeline_run_id = dbutils.widgets.get("pipeline_run_id").strip() or None

config = load_raw_to_bronze_config(release_name, config_root=config_root)
environment = resolve_release_environment(config)
context = create_pipeline_context(
    config,
    environment,
    pipeline_run_id=pipeline_run_id,
)
ensure_operations_tables(spark, context)

try:
    validation_result = validate_raw_inventory_and_readiness(
        spark,
        dbutils,
        config,
        environment,
    )
except RawInventoryError:
    print(f"Release name: {context.release_name}")
    print(f"Raw volume path: {environment.raw_volume_path}")
    print("Direct listing from dbutils.fs.ls:")
    for entry in dbutils.fs.ls(environment.raw_volume_path):
        entry_name = str(getattr(entry, "name", "") or "").rstrip("/")
        print(f"- {entry_name} | {entry.path}")
    raise

inventory_status = validation_result.inventory_status
source_readiness = validation_result.source_readiness

append_records(
    spark,
    f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
    [
        build_run_message_record(
            context,
            message_level="INFO",
            message_code="SOURCE_READ_COMPLETED",
            message_text=(
                "Validated exact Raw inventory and readable Parquet contracts "
                f"for {len(source_readiness)} configured sources."
            ),
        )
    ],
)

schema_snapshot_records = []
for source in source_readiness:
    schema_snapshot_records.extend(
        build_schema_snapshot_records(
            context,
            layer_name="raw",
            object_name=source.file_name,
            schema_fields=list(source.schema_fields),
        )
    )

append_records(
    spark,
    f"{context.operations_schema_fqn}.{SCHEMA_SNAPSHOTS_TABLE}",
    schema_snapshot_records,
)

# COMMAND ----------
print(f"Release name: {context.release_name}")
print(f"Raw volume path: {environment.raw_volume_path}")
print(f"Expected file count: {len(inventory_status.expected_files)}")
print(f"Discovered file count: {len(inventory_status.discovered_files)}")
print(f"Unexpected file policy: {inventory_status.policy}")

try:
    dbutils.jobs.taskValues.set(key="pipeline_run_id", value=context.pipeline_run_id)
except Exception:
    pass

display(
    spark.createDataFrame(
        [
            {
                "source_name": source.source_name,
                "file_name": source.file_name,
                "file_path": source.file_path,
                "file_size": source.file_size,
                "row_count": source.row_count,
                "schema_hash": source.schema_hash,
            }
            for source in source_readiness
        ]
    )
)
