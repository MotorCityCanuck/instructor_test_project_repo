"""Databricks harness for standalone Gold-layer audit and anomaly validation."""

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
from napa_pipeline.silver_to_gold.environment import (
    build_runtime_context,
    ensure_release_environment,
)
from napa_pipeline.silver_to_gold.gold_audit import (
    GoldAuditValidationError,
    publish_gold_layer_audit,
)
from napa_pipeline.silver_to_gold.operations import (
    COLUMN_PROFILE_RESULTS_TABLE,
    QUALITY_RESULTS_TABLE,
    RECONCILIATION_RESULTS_TABLE,
    TABLE_PROFILE_RESULTS_TABLE,
    TABLE_RUNS_TABLE,
    append_records,
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
    resolve_latest_successful_upstream_run_id,
)


SCRIPT_VERSION = "2026.07.26.1"
AUDIT_OUTPUT_CONFIG = {
    TABLE_PROFILE_RESULTS_TABLE: {
        "build_order": 300,
        "reconciliation_name": "gold_table_profile_results_row_balance",
    },
    COLUMN_PROFILE_RESULTS_TABLE: {
        "build_order": 310,
        "reconciliation_name": "gold_column_profile_results_row_balance",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Describe and validate published Silver-to-Gold tables in Databricks."
    )
    add_release_name_argument(parser)
    add_config_path_argument(parser)
    add_analysis_as_of_date_argument(parser)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_databricks_global("spark")
    dbutils = get_databricks_global("dbutils")
    pipeline_context = None
    table_run_started_ts_by_target: dict[str, object] = {}

    try:
        config = load_silver_to_gold_config(
            args.release_name,
            config_root=normalize_config_path(args.config_path),
        )
        environment_status = ensure_release_environment(spark, config, create_missing=True)
        environment = environment_status.release_environment
        upstream_silver_run_id = resolve_latest_successful_upstream_run_id(
            spark,
            config,
            environment,
        )
        match_rows = (
            []
            if args.analysis_as_of_date
            else collect_match_rows_for_analysis_date(spark, environment)
        )
        runtime_context = build_runtime_context(
            config,
            environment,
            upstream_silver_run_id=upstream_silver_run_id,
            match_rows=match_rows,
            analysis_as_of_date=args.analysis_as_of_date,
        )
        pipeline_context = create_pipeline_context(runtime_context)
        initialize_pipeline_run(spark, pipeline_context)
        table_runs_fqn = get_operations_table_fqn(pipeline_context, TABLE_RUNS_TABLE)

        for table_name in AUDIT_OUTPUT_CONFIG:
            started_ts = utc_now()
            table_run_started_ts_by_target[table_name] = started_ts
            append_records(
                spark,
                table_runs_fqn,
                [
                    build_table_run_start_record(
                        pipeline_context,
                        target_gold_table=table_name,
                        build_stage="gold_audit",
                        build_order=AUDIT_OUTPUT_CONFIG[table_name]["build_order"],
                        started_ts=started_ts,
                    )
                ],
            )

        audit_summary = publish_gold_layer_audit(
            spark,
            pipeline_context,
            config,
            environment,
        )

        append_records(
            spark,
            table_runs_fqn,
            [
                build_table_run_end_record(
                    pipeline_context,
                    target_gold_table=TABLE_PROFILE_RESULTS_TABLE,
                    build_stage="gold_audit",
                    build_order=AUDIT_OUTPUT_CONFIG[TABLE_PROFILE_RESULTS_TABLE]["build_order"],
                    started_ts=table_run_started_ts_by_target[TABLE_PROFILE_RESULTS_TABLE],
                    status="SUCCEEDED",
                    input_row_count=audit_summary.table_profile_row_count,
                    output_row_count=audit_summary.table_profile_row_count,
                    excluded_row_count=0,
                    warning_count=audit_summary.warning_count,
                ),
                build_table_run_end_record(
                    pipeline_context,
                    target_gold_table=COLUMN_PROFILE_RESULTS_TABLE,
                    build_stage="gold_audit",
                    build_order=AUDIT_OUTPUT_CONFIG[COLUMN_PROFILE_RESULTS_TABLE]["build_order"],
                    started_ts=table_run_started_ts_by_target[COLUMN_PROFILE_RESULTS_TABLE],
                    status="SUCCEEDED",
                    input_row_count=audit_summary.column_profile_row_count,
                    output_row_count=audit_summary.column_profile_row_count,
                    excluded_row_count=0,
                    warning_count=audit_summary.warning_count,
                ),
            ],
        )

        print(f"Script version: {SCRIPT_VERSION}")
        print(f"Pipeline name: {pipeline_context.pipeline_name}")
        print(f"Pipeline version: {pipeline_context.pipeline_version}")
        print(f"Release name: {pipeline_context.release_name}")
        print(f"Release role: {pipeline_context.release_role}")
        print(f"Gold audit pipeline run ID: {pipeline_context.pipeline_run_id}")
        print(f"Upstream Silver run ID: {pipeline_context.upstream_pipeline_run_id}")
        print(f"Config root: {config.config_root}")
        print(f"Config hash: {pipeline_context.configuration_hash}")
        print(f"Analysis as-of date: {pipeline_context.analysis_as_of_date}")
        print(f"Scoring scenario: {pipeline_context.scoring_scenario}")
        print(
            "Authoritative recommendation flag: "
            f"{pipeline_context.authoritative_recommendation_flag}"
        )
        print(f"Catalog: {environment.catalog}")
        print(f"Gold schema: {environment.gold_schema}")
        print(f"Operations schema: {environment.operations_schema}")
        print("Published Gold audit outputs:")
        print(
            f"  - {TABLE_PROFILE_RESULTS_TABLE}: {audit_summary.table_profile_results_fqn} "
            f"(rows={audit_summary.table_profile_row_count})"
        )
        print(
            f"  - {COLUMN_PROFILE_RESULTS_TABLE}: {audit_summary.column_profile_results_fqn} "
            f"(rows={audit_summary.column_profile_row_count})"
        )
        print(
            f"  - {QUALITY_RESULTS_TABLE}: {audit_summary.quality_results_fqn} "
            f"(rows={audit_summary.quality_record_count})"
        )
        print(
            f"  - {RECONCILIATION_RESULTS_TABLE}: {audit_summary.reconciliation_results_fqn} "
            f"(rows={audit_summary.reconciliation_record_count})"
        )
        print(
            "Audit summary: "
            f"audited_tables={audit_summary.audited_table_count}, "
            f"warnings={audit_summary.warning_count}, "
            f"critical_failures={audit_summary.critical_failure_count}"
        )

        set_task_value(dbutils, "run_id", pipeline_context.pipeline_run_id)
        set_task_value(dbutils, "pipeline_run_id", pipeline_context.pipeline_run_id)
        set_task_value(dbutils, "release_name", pipeline_context.release_name)
        set_task_value(dbutils, "config_hash", pipeline_context.configuration_hash)
        set_task_value(dbutils, "analysis_as_of_date", str(pipeline_context.analysis_as_of_date))
        set_task_value(dbutils, "upstream_silver_run_id", pipeline_context.upstream_pipeline_run_id)
        set_task_value(dbutils, "gold_audit_table_profile_row_count", audit_summary.table_profile_row_count)
        set_task_value(dbutils, "gold_audit_column_profile_row_count", audit_summary.column_profile_row_count)
        set_task_value(dbutils, "gold_audit_quality_record_count", audit_summary.quality_record_count)
        set_task_value(
            dbutils,
            "gold_audit_reconciliation_record_count",
            audit_summary.reconciliation_record_count,
        )
        complete_pipeline_run(spark, pipeline_context, status="SUCCEEDED")
    except Exception as exc:
        if pipeline_context is not None:
            failed_records = []
            for table_name in AUDIT_OUTPUT_CONFIG:
                started_ts = table_run_started_ts_by_target.get(table_name)
                if started_ts is None:
                    continue
                failed_records.append(
                    build_table_run_end_record(
                        pipeline_context,
                        target_gold_table=table_name,
                        build_stage="gold_audit",
                        build_order=AUDIT_OUTPUT_CONFIG[table_name]["build_order"],
                        started_ts=started_ts,
                        status="FAILED",
                        error_message=str(exc),
                    )
                )
            if failed_records:
                append_records(
                    spark,
                    get_operations_table_fqn(pipeline_context, TABLE_RUNS_TABLE),
                    failed_records,
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
