"""Databricks harness for Gold team context-adjustment features."""

from __future__ import annotations

import argparse

from _bootstrap_napa_pipeline import bootstrap_napa_pipeline_imports

bootstrap_napa_pipeline_imports()

from napa_pipeline.silver_to_gold.cli import (
    add_analysis_as_of_date_argument,
    add_config_path_argument,
    add_release_name_argument,
    get_databricks_global,
    normalize_config_path,
    set_task_value,
)
from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.context_adjustments_validation import (
    publish_context_adjustment_tables,
    validate_context_adjustment_source_contract,
)
from napa_pipeline.silver_to_gold.environment import build_runtime_context, ensure_release_environment
from napa_pipeline.silver_to_gold.operations import (
    RECONCILIATION_RESULTS_TABLE,
    TABLE_RUNS_TABLE,
    append_records,
    build_reconciliation_record,
    build_table_run_end_record,
    build_table_run_start_record,
    complete_pipeline_run,
    create_pipeline_context,
    get_operations_table_fqn,
    utc_now,
)
from napa_pipeline.silver_to_gold.workflow import (
    collect_match_rows_for_analysis_date,
    initialize_pipeline_run,
    require_required_silver_source_tables,
    resolve_latest_successful_upstream_run_id,
)


SCRIPT_VERSION = "2026.09.14.1"
TARGET_TABLE = "team_context_adjustment_features"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Gold team context-adjustment features.")
    add_release_name_argument(parser)
    add_config_path_argument(parser)
    add_analysis_as_of_date_argument(parser)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_databricks_global("spark")
    dbutils = get_databricks_global("dbutils")
    pipeline_context = None
    started_ts = None
    try:
        config = load_silver_to_gold_config(args.release_name, normalize_config_path(args.config_path))
        environment = ensure_release_environment(spark, config, create_missing=True).release_environment
        upstream_silver_run_id = resolve_latest_successful_upstream_run_id(spark, config, environment)
        require_required_silver_source_tables(spark, environment)
        runtime_context = build_runtime_context(
            config,
            environment,
            upstream_silver_run_id=upstream_silver_run_id,
            match_rows=collect_match_rows_for_analysis_date(spark, environment),
            analysis_as_of_date=args.analysis_as_of_date,
        )
        pipeline_context = create_pipeline_context(runtime_context)
        initialize_pipeline_run(spark, pipeline_context)
        validate_context_adjustment_source_contract(spark, environment)
        started_ts = utc_now()
        table_runs_fqn = get_operations_table_fqn(pipeline_context, TABLE_RUNS_TABLE)
        append_records(
            spark,
            table_runs_fqn,
            [build_table_run_start_record(
                pipeline_context,
                target_gold_table=TARGET_TABLE,
                build_stage="context_adjustments",
                build_order=165,
                started_ts=started_ts,
            )],
        )
        summary = publish_context_adjustment_tables(
            spark,
            environment,
            analysis_as_of_date=pipeline_context.analysis_as_of_date,
            context_config=config.data["context_adjustments"],
        ).team_context_adjustment_features
        source_count = int(spark.table(f"{environment.catalog}.{environment.silver_schema}.teams").count())
        if source_count != summary.output_row_count:
            raise ValueError(
                f"{TARGET_TABLE} row count did not reconcile: source_rows={source_count}, "
                f"output_rows={summary.output_row_count}."
            )
        append_records(
            spark,
            table_runs_fqn,
            [build_table_run_end_record(
                pipeline_context,
                target_gold_table=TARGET_TABLE,
                build_stage="context_adjustments",
                build_order=165,
                started_ts=started_ts,
                status="SUCCEEDED",
                input_row_count=source_count,
                output_row_count=summary.output_row_count,
                excluded_row_count=0,
            )],
        )
        append_records(
            spark,
            get_operations_table_fqn(pipeline_context, RECONCILIATION_RESULTS_TABLE),
            [build_reconciliation_record(
                pipeline_context,
                reconciliation_name="team_context_adjustment_features_row_balance",
                source_count=source_count,
                accepted_count=summary.output_row_count,
                excluded_count=0,
            )],
        )
        print(f"Script version: {SCRIPT_VERSION}")
        print(f"Release name: {pipeline_context.release_name}")
        print(f"Analysis as-of date: {pipeline_context.analysis_as_of_date}")
        print(f"Published: {summary.target_table_fqn} (rows={summary.output_row_count})")
        set_task_value(dbutils, "pipeline_run_id", pipeline_context.pipeline_run_id)
        set_task_value(dbutils, "phase_context_features_row_count", summary.output_row_count)
        complete_pipeline_run(spark, pipeline_context, status="SUCCEEDED")
    except Exception as exc:
        if pipeline_context is not None and started_ts is not None:
            append_records(
                spark,
                get_operations_table_fqn(pipeline_context, TABLE_RUNS_TABLE),
                [build_table_run_end_record(
                    pipeline_context,
                    target_gold_table=TARGET_TABLE,
                    build_stage="context_adjustments",
                    build_order=165,
                    started_ts=started_ts,
                    status="FAILED",
                    error_message=str(exc),
                )],
            )
        if pipeline_context is not None:
            complete_pipeline_run(
                spark,
                pipeline_context,
                status="FAILED",
                error_class=type(exc).__name__,
                error_message=str(exc),
            )
        raise


if __name__ == "__main__":
    main()
