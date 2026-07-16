"""Finalize the Bronze-to-Silver pipeline run."""

from __future__ import annotations

import argparse

from _bootstrap_napa_pipeline import bootstrap_napa_pipeline_imports

bootstrap_napa_pipeline_imports()

from napa_pipeline.bronze_to_silver.cli import (
    add_config_path_argument,
    add_release_name_argument,
    add_run_id_argument,
    get_databricks_global,
    normalize_config_path,
    set_task_value,
)
from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.environment import resolve_release_environment
from napa_pipeline.bronze_to_silver.finalize import (
    finalize_pipeline_run,
    summarize_pipeline_run,
)
from napa_pipeline.bronze_to_silver.operations import (
    RUN_MESSAGES_TABLE,
    append_records,
    build_run_message_record,
    create_pipeline_context,
    ensure_operations_tables,
)


SCRIPT_VERSION = "2026.07.16.1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Finalize the Bronze-to-Silver pipeline run."
    )
    add_release_name_argument(parser)
    add_run_id_argument(parser)
    add_config_path_argument(parser)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_databricks_global("spark")
    dbutils = get_databricks_global("dbutils")
    config = load_bronze_to_silver_config(
        args.release_name,
        config_root=normalize_config_path(args.config_path),
    )
    environment = resolve_release_environment(config)
    context = create_pipeline_context(config, environment, pipeline_run_id=args.run_id)
    ensure_operations_tables(spark, context)
    expected_table_count = len(config.silver_tables_in_build_order)
    summary = summarize_pipeline_run(
        spark,
        context,
        expected_table_count=expected_table_count,
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
    print(f"Release name: {args.release_name}")
    print(f"Run ID: {args.run_id}")
    print(f"Expected Silver table count: {summary.expected_table_count}")
    print(f"Completed table runs: {summary.completed_table_run_count}")
    print(f"Failed table runs: {summary.failed_table_run_count}")
    print(f"Reconciliation result count: {summary.reconciliation_result_count}")
    print(
        "Mismatched reconciliation results: "
        f"{summary.mismatched_reconciliation_count}"
    )
    print(f"Critical quality failures: {summary.critical_quality_failure_count}")
    print(f"Final pipeline status: {summary.final_status}")
    print(summary.summary_text)

    set_task_value(dbutils, "run_id", args.run_id)
    set_task_value(dbutils, "final_status", summary.final_status)

    if summary.final_status != "SUCCEEDED":
        raise RuntimeError(summary.summary_text)


if __name__ == "__main__":
    main()
