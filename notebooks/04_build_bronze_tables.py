"""Databricks task notebook for building Bronze tables from Raw Parquet files."""

# Databricks notebook source

# COMMAND ----------
# Title: 04 Build Bronze Tables
# Purpose:
# Read validated Raw Parquet sources, append operational metadata, publish
# release-specific Bronze Delta tables, and record table-level execution audit
# evidence.

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

from napa_pipeline.raw_to_bronze.bronze import (
    BronzePublicationError,
    build_bronze_table,
    get_bronze_target_table_fqn,
)
from napa_pipeline.raw_to_bronze.config import load_raw_to_bronze_config
from napa_pipeline.raw_to_bronze.environment import ensure_release_environment
from napa_pipeline.raw_to_bronze.inventory import validate_raw_inventory_and_readiness
from napa_pipeline.raw_to_bronze.operations import (
    RUN_MESSAGES_TABLE,
    SCHEMA_SNAPSHOTS_TABLE,
    TABLE_RUNS_TABLE,
    append_records,
    build_run_message_record,
    build_schema_snapshot_records,
    build_table_run_end_record,
    build_table_run_start_record,
    create_pipeline_context,
    ensure_operations_tables,
    utc_now,
)

# COMMAND ----------
ALLOWED_RELEASES = ["napa_5k", "napa_50k", "napa_250k"]

dbutils.widgets.dropdown("release_name", "napa_5k", ["napa_5k", "napa_50k", "napa_250k"])
dbutils.widgets.text("dataset_release", "")
dbutils.widgets.text("config_root", "")
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.dropdown("create_missing", "false", ["true", "false"])

dataset_release = dbutils.widgets.get("dataset_release").strip()
release_name = dataset_release or dbutils.widgets.get("release_name").strip()
config_root = dbutils.widgets.get("config_root").strip() or None
pipeline_run_id = dbutils.widgets.get("pipeline_run_id").strip() or None
create_missing = dbutils.widgets.get("create_missing").strip().lower() == "true"

if release_name not in ALLOWED_RELEASES:
    raise ValueError(
        "dataset_release or release_name must be one of: "
        f"{', '.join(ALLOWED_RELEASES)}."
    )

config = load_raw_to_bronze_config(release_name, config_root=config_root)
environment_status = ensure_release_environment(
    spark,
    config,
    create_missing=create_missing,
)
environment = environment_status.release_environment
context = create_pipeline_context(
    config,
    environment,
    pipeline_run_id=pipeline_run_id,
)
ensure_operations_tables(spark, context)

validation_result = validate_raw_inventory_and_readiness(
    spark,
    dbutils,
    config,
    environment,
)
source_readiness_by_name = {
    record.source_name: record for record in validation_result.source_readiness
}

append_records(
    spark,
    f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
    [
        build_run_message_record(
            context,
            message_level="INFO",
            message_code="BRONZE_WRITE_STARTED",
            message_text=(
                "Starting Bronze publication for "
                f"{len(source_readiness_by_name)} configured sources."
            ),
        )
    ],
)

# COMMAND ----------
build_results = []

for source_config in config.sources_in_build_order:
    source_name = source_config["source_name"]
    source_readiness = source_readiness_by_name[source_name]
    target_table_fqn = get_bronze_target_table_fqn(environment, source_config)
    table_started_ts = utc_now()

    append_records(
        spark,
        f"{context.operations_schema_fqn}.{TABLE_RUNS_TABLE}",
        [
            build_table_run_start_record(
                context,
                source_file_name=source_readiness.file_name,
                source_table=source_name,
                target_table=target_table_fqn,
                started_ts=table_started_ts,
                source_file_size=source_readiness.file_size,
            )
        ],
    )
    append_records(
        spark,
        f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
        [
            build_run_message_record(
                context,
                message_level="INFO",
                message_code="BRONZE_WRITE_STARTED",
                message_text=(
                    f"Starting Bronze publication for {source_name} "
                    f"from {source_readiness.file_name}."
                ),
                source_name=source_name,
            )
        ],
    )

    try:
        result = build_bronze_table(
            spark,
            config,
            context,
            environment,
            source_config,
            source_readiness,
            ingested_ts=table_started_ts,
        )
        build_results.append(result)

        append_records(
            spark,
            f"{context.operations_schema_fqn}.{TABLE_RUNS_TABLE}",
            [
                build_table_run_end_record(
                    context,
                    source_file_name=result.source_file_name,
                    source_table=source_name,
                    target_table=result.target_table_fqn,
                    started_ts=table_started_ts,
                    status="SUCCEEDED",
                    source_row_count=result.source_row_count,
                    bronze_row_count=result.bronze_row_count,
                    source_schema_hash=result.source_schema_hash,
                    bronze_schema_hash=result.bronze_schema_hash,
                    source_file_size=result.source_file_size,
                )
            ],
        )
        append_records(
            spark,
            f"{context.operations_schema_fqn}.{SCHEMA_SNAPSHOTS_TABLE}",
            build_schema_snapshot_records(
                context,
                layer_name="bronze",
                object_name=source_config["bronze_table"],
                schema_fields=list(result.bronze_schema_fields),
            ),
        )
        append_records(
            spark,
            f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
            [
                build_run_message_record(
                    context,
                    message_level="INFO",
                    message_code="BRONZE_WRITE_COMPLETED",
                    message_text=(
                        f"Published Bronze table {result.target_table_fqn} with "
                        f"{result.bronze_row_count} rows."
                    ),
                    source_name=source_name,
                )
            ],
        )
    except BronzePublicationError as exc:
        append_records(
            spark,
            f"{context.operations_schema_fqn}.{TABLE_RUNS_TABLE}",
            [
                build_table_run_end_record(
                    context,
                    source_file_name=source_readiness.file_name,
                    source_table=source_name,
                    target_table=target_table_fqn,
                    started_ts=table_started_ts,
                    status="FAILED",
                    source_row_count=source_readiness.row_count,
                    source_schema_hash=source_readiness.schema_hash,
                    source_file_size=source_readiness.file_size,
                    error_message=str(exc),
                )
            ],
        )
        append_records(
            spark,
            f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
            [
                build_run_message_record(
                    context,
                    message_level="ERROR",
                    message_code="BRONZE_WRITE_FAILED",
                    message_text=str(exc),
                    source_name=source_name,
                )
            ],
        )
        raise

# COMMAND ----------
print(f"Release name: {context.release_name}")
print(f"Dataset release parameter: {dataset_release or '<not provided>'}")
print(f"Bronze schema: {environment.bronze_schema}")
print(f"Published Bronze table count: {len(build_results)}")

try:
    dbutils.jobs.taskValues.set(key="pipeline_run_id", value=context.pipeline_run_id)
    dbutils.jobs.taskValues.set(key="bronze_table_count", value=len(build_results))
except Exception:
    pass

display(
    spark.createDataFrame(
        [
            {
                "source_name": result.source_name,
                "source_file_name": result.source_file_name,
                "target_table_fqn": result.target_table_fqn,
                "source_row_count": result.source_row_count,
                "bronze_row_count": result.bronze_row_count,
                "source_schema_hash": result.source_schema_hash,
                "bronze_schema_hash": result.bronze_schema_hash,
            }
            for result in build_results
        ]
    )
)
