"""Databricks task notebook for finalizing the Raw-to-Bronze pipeline run."""

# Databricks notebook source

# COMMAND ----------
# Title: 06 Finalize Pipeline Run
# Purpose:
# Summarize table-level and reconciliation outcomes, update the durable
# pipeline_runs record, and emit a single final run message for the workflow.

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
from napa_pipeline.raw_to_bronze.finalize import (
    PipelineFinalizationError,
    finalize_pipeline_run,
    summarize_pipeline_run,
)
from napa_pipeline.raw_to_bronze.operations import (
    RUN_MESSAGES_TABLE,
    append_records,
    build_run_message_record,
    create_pipeline_context,
    ensure_operations_tables,
)

# COMMAND ----------
ALLOWED_RELEASES = ["napa_5k", "napa_50k", "napa_250k"]

dbutils.widgets.dropdown("release_name", "napa_5k", ["napa_5k", "napa_50k", "napa_250k"])
dbutils.widgets.text("dataset_release", "")
dbutils.widgets.text("config_root", "")
dbutils.widgets.text("pipeline_run_id", "")

dataset_release = dbutils.widgets.get("dataset_release").strip()
release_name = dataset_release or dbutils.widgets.get("release_name").strip()
config_root = dbutils.widgets.get("config_root").strip() or None
pipeline_run_id = dbutils.widgets.get("pipeline_run_id").strip() or None

if release_name not in ALLOWED_RELEASES:
    raise ValueError(
        "dataset_release or release_name must be one of: "
        f"{', '.join(ALLOWED_RELEASES)}."
    )

config = load_raw_to_bronze_config(release_name, config_root=config_root)
environment = resolve_release_environment(config)
context = create_pipeline_context(
    config,
    environment,
    pipeline_run_id=pipeline_run_id,
)
ensure_operations_tables(spark, context)

expected_source_count = len(config.sources_in_build_order)

summary = summarize_pipeline_run(
    spark,
    context,
    expected_source_count=expected_source_count,
)
finalize_pipeline_run(spark, context, summary)

append_records(
    spark,
    f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
    [
        build_run_message_record(
            context,
            message_level="INFO" if summary.final_status == "SUCCEEDED" else "ERROR",
            message_code=(
                "PIPELINE_SUCCEEDED"
                if summary.final_status == "SUCCEEDED"
                else "PIPELINE_FAILED"
            ),
            message_text=summary.summary_text,
        )
    ],
)

if summary.final_status != "SUCCEEDED":
    raise RuntimeError(summary.summary_text)

# COMMAND ----------
print(f"Release name: {context.release_name}")
print(f"Dataset release parameter: {dataset_release or '<not provided>'}")
print(f"Pipeline run ID: {context.pipeline_run_id}")
print(f"Expected source count: {summary.expected_source_count}")
print(f"Completed table runs: {summary.completed_table_run_count}")
print(f"Failed table runs: {summary.failed_table_run_count}")
print(f"Reconciliation result count: {summary.reconciliation_result_count}")
print(
    "Mismatched reconciliation results: "
    f"{summary.mismatched_reconciliation_count}"
)
print(f"Final pipeline status: {summary.final_status}")
print(summary.summary_text)

try:
    dbutils.jobs.taskValues.set(key="pipeline_run_id", value=context.pipeline_run_id)
    dbutils.jobs.taskValues.set(key="final_status", value=summary.final_status)
except Exception:
    pass

display(
    spark.createDataFrame(
        [
            {
                "pipeline_run_id": context.pipeline_run_id,
                "release_name": context.release_name,
                "expected_source_count": summary.expected_source_count,
                "completed_table_run_count": summary.completed_table_run_count,
                "failed_table_run_count": summary.failed_table_run_count,
                "reconciliation_result_count": summary.reconciliation_result_count,
                "mismatched_reconciliation_count": summary.mismatched_reconciliation_count,
                "final_status": summary.final_status,
                "summary_text": summary.summary_text,
            }
        ]
    )
)
