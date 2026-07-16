"""Finalize the Raw-to-Bronze pipeline run."""

from __future__ import annotations

import argparse

from _bootstrap_napa_pipeline import bootstrap_napa_pipeline_imports

bootstrap_napa_pipeline_imports()

from napa_pipeline.raw_to_bronze.cli import (
    add_config_path_argument,
    add_release_type_argument,
    add_run_id_argument,
    get_databricks_global,
    normalize_config_path,
    release_type_to_release_name,
    set_task_value,
)
from napa_pipeline.raw_to_bronze.config import load_raw_to_bronze_config
from napa_pipeline.raw_to_bronze.environment import resolve_release_environment
from napa_pipeline.raw_to_bronze.finalize import (
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


SCRIPT_VERSION = "2026.07.16.1"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for pipeline finalization."""
    parser = argparse.ArgumentParser(
        description="Finalize the Raw-to-Bronze pipeline run."
    )
    add_release_type_argument(parser)
    add_run_id_argument(parser)
    add_config_path_argument(parser)
    return parser.parse_args()


def main() -> None:
    """Summarize and close the durable pipeline run record."""
    args = parse_args()
    spark = get_databricks_global("spark")
    dbutils = get_databricks_global("dbutils")

    release_name = release_type_to_release_name(args.release_type)
    config = load_raw_to_bronze_config(
        release_name,
        config_root=normalize_config_path(args.config_path),
    )
    environment = resolve_release_environment(config)
    context = create_pipeline_context(config, environment, pipeline_run_id=args.run_id)
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

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Release type: {args.release_type}")
    print(f"Release name: {context.release_name}")
    print(f"Run ID: {context.pipeline_run_id}")
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

    set_task_value(dbutils, "run_id", context.pipeline_run_id)
    set_task_value(dbutils, "final_status", summary.final_status)

    if summary.final_status != "SUCCEEDED":
        raise RuntimeError(summary.summary_text)


if __name__ == "__main__":
    main()
