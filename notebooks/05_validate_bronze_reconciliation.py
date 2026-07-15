"""Databricks task notebook for validating Bronze reconciliation results."""

# Databricks notebook source

# COMMAND ----------
# Title: 05 Validate Bronze Reconciliation
# Purpose:
# Compare Raw counts and business-column contracts to published Bronze Delta
# tables, write reconciliation results, and emit targeted diagnostics for any
# mismatches.

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
from napa_pipeline.raw_to_bronze.inventory import validate_raw_inventory_and_readiness
from napa_pipeline.raw_to_bronze.operations import (
    RECONCILIATION_RESULTS_TABLE,
    RUN_MESSAGES_TABLE,
    append_records,
    build_reconciliation_record,
    build_run_message_record,
    create_pipeline_context,
    ensure_operations_tables,
)
from napa_pipeline.raw_to_bronze.reconciliation import (
    ReconciliationError,
    reconcile_bronze_table,
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

# COMMAND ----------
reconciliation_rows = []
message_rows = []
display_rows = []
mismatch_count = 0

for source_config in config.sources_in_build_order:
    source_name = source_config["source_name"]
    source_readiness = source_readiness_by_name[source_name]
    source_has_mismatch = False

    try:
        result = reconcile_bronze_table(
            spark,
            environment,
            source_config,
            source_readiness,
        )
    except ReconciliationError as exc:
        message_rows.append(
            build_run_message_record(
                context,
                message_level="ERROR",
                message_code="SCHEMA_RECONCILIATION_FAILED",
                message_text=str(exc),
                source_name=source_name,
            )
        )
        append_records(
            spark,
            f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
            message_rows,
        )
        raise

    reconciliation_rows.append(
        build_reconciliation_record(
            context,
            source_file_name=result.source_file_name,
            bronze_table=result.bronze_table,
            raw_row_count=result.raw_row_count,
            bronze_row_count=result.bronze_row_count,
            raw_business_column_count=result.raw_business_column_count,
            bronze_business_column_count=result.bronze_business_column_count,
            metadata_column_count=result.metadata_column_count,
            status=result.status,
        )
    )

    if result.row_count_difference != 0:
        source_has_mismatch = True
        message_rows.append(
            build_run_message_record(
                context,
                message_level="ERROR",
                message_code="ROW_COUNT_MISMATCH",
                message_text=(
                    f"Raw row count {result.raw_row_count} does not match Bronze row "
                    f"count {result.bronze_row_count} for {result.target_table_fqn}."
                ),
                source_name=source_name,
            )
        )

    if (
        result.missing_metadata_columns
        or result.missing_business_columns
        or result.unexpected_business_columns
    ):
        source_has_mismatch = True
        schema_issues = []
        if result.missing_metadata_columns:
            schema_issues.append(
                "missing metadata columns: "
                f"{', '.join(result.missing_metadata_columns)}"
            )
        if result.missing_business_columns:
            schema_issues.append(
                "missing business columns: "
                f"{', '.join(result.missing_business_columns)}"
            )
        if result.unexpected_business_columns:
            schema_issues.append(
                "unexpected business columns: "
                f"{', '.join(result.unexpected_business_columns)}"
            )
        message_rows.append(
            build_run_message_record(
                context,
                message_level="ERROR",
                message_code="SCHEMA_RECONCILIATION_FAILED",
                message_text=(
                    f"Schema reconciliation failed for {result.target_table_fqn}: "
                    f"{'; '.join(schema_issues)}."
                ),
                source_name=source_name,
            )
        )

    if source_has_mismatch:
        mismatch_count += 1

    display_rows.append(
        {
            "source_name": result.source_name,
            "bronze_table": result.bronze_table,
            "target_table_fqn": result.target_table_fqn,
            "raw_row_count": result.raw_row_count,
            "bronze_row_count": result.bronze_row_count,
            "row_count_difference": result.row_count_difference,
            "raw_business_column_count": result.raw_business_column_count,
            "bronze_business_column_count": result.bronze_business_column_count,
            "metadata_column_count": result.metadata_column_count,
            "status": result.status,
        }
    )

append_records(
    spark,
    f"{context.operations_schema_fqn}.{RECONCILIATION_RESULTS_TABLE}",
    reconciliation_rows,
)

summary_code = "PIPELINE_SUCCEEDED" if mismatch_count == 0 else "SCHEMA_RECONCILIATION_FAILED"
summary_text = (
    f"Bronze reconciliation succeeded for {len(reconciliation_rows)} sources."
    if mismatch_count == 0
    else f"Bronze reconciliation found {mismatch_count} mismatches across "
    f"{len(reconciliation_rows)} sources."
)
message_rows.append(
    build_run_message_record(
        context,
        message_level="INFO" if mismatch_count == 0 else "ERROR",
        message_code=summary_code,
        message_text=summary_text,
    )
)
append_records(
    spark,
    f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
    message_rows,
)

if mismatch_count != 0:
    raise RuntimeError(summary_text)

# COMMAND ----------
print(f"Release name: {context.release_name}")
print(f"Dataset release parameter: {dataset_release or '<not provided>'}")
print(f"Bronze schema: {environment.bronze_schema}")
print(f"Reconciled source count: {len(reconciliation_rows)}")
print("Reconciliation mismatches detected: <none>")

try:
    dbutils.jobs.taskValues.set(key="pipeline_run_id", value=context.pipeline_run_id)
    dbutils.jobs.taskValues.set(
        key="reconciled_source_count", value=len(reconciliation_rows)
    )
except Exception:
    pass

display(spark.createDataFrame(display_rows))
