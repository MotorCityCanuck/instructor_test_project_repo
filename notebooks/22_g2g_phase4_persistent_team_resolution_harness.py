"""Databricks harness for Silver-to-Gold Phase 4 persistent-team resolution testing."""

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
from napa_pipeline.silver_to_gold.operations import (
    TABLE_RUNS_TABLE,
    RECONCILIATION_RESULTS_TABLE,
    append_records,
    build_reconciliation_record,
    build_table_run_end_record,
    build_table_run_start_record,
    complete_pipeline_run,
    create_pipeline_context,
    get_operations_table_fqn,
    utc_now,
)
from napa_pipeline.silver_to_gold.team_resolution import publish_resolved_match_teams
from napa_pipeline.silver_to_gold.workflow import (
    PHASE4_TARGET_TABLES,
    collect_match_rows_for_analysis_date,
    initialize_pipeline_run,
    require_required_silver_source_tables,
    resolve_latest_successful_upstream_run_id,
)


SCRIPT_VERSION = "2026.07.22.1"

PHASE4_BUILD_ORDER = 30
PHASE4_TARGET_TABLE = "resolved_match_teams"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Silver-to-Gold Phase 4 persistent-team resolution in Databricks."
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
    table_run_started_ts = None

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
        existing_silver_tables = require_required_silver_source_tables(spark, environment)
        match_rows = collect_match_rows_for_analysis_date(spark, environment)
        runtime_context = build_runtime_context(
            config,
            environment,
            upstream_silver_run_id=upstream_silver_run_id,
            match_rows=match_rows,
            analysis_as_of_date=args.analysis_as_of_date,
        )
        pipeline_context = create_pipeline_context(runtime_context)
        initialize_pipeline_run(spark, pipeline_context)
        table_run_started_ts = utc_now()
        append_records(
            spark,
            get_operations_table_fqn(pipeline_context, TABLE_RUNS_TABLE),
            [
                build_table_run_start_record(
                    pipeline_context,
                    target_gold_table=PHASE4_TARGET_TABLE,
                    build_stage="gold",
                    build_order=PHASE4_BUILD_ORDER,
                    started_ts=table_run_started_ts,
                )
            ],
        )
        resolution_summary = publish_resolved_match_teams(spark, environment)
        if resolution_summary.output_row_count != resolution_summary.input_row_count:
            raise ValueError(
                "Resolved match-side row count does not reconcile with match_teams: "
                f"resolved_rows={resolution_summary.output_row_count}, "
                f"match_teams_rows={resolution_summary.input_row_count}."
            )
        resolution_total = (
            resolution_summary.direct_resolution_count
            + resolution_summary.active_pair_resolution_count
            + resolution_summary.historical_pair_resolution_count
            + resolution_summary.ambiguous_count
            + resolution_summary.unresolved_count
        )
        if resolution_total != resolution_summary.output_row_count:
            raise ValueError(
                "Resolution summary counts do not reconcile with resolved rows: "
                f"summary_total={resolution_total}, resolved_rows={resolution_summary.output_row_count}."
            )
        append_records(
            spark,
            get_operations_table_fqn(pipeline_context, TABLE_RUNS_TABLE),
            [
                build_table_run_end_record(
                    pipeline_context,
                    target_gold_table=PHASE4_TARGET_TABLE,
                    build_stage="gold",
                    build_order=PHASE4_BUILD_ORDER,
                    started_ts=table_run_started_ts,
                    status="SUCCEEDED",
                    input_row_count=resolution_summary.input_row_count,
                    output_row_count=resolution_summary.output_row_count,
                    excluded_row_count=0,
                    warning_count=(
                        resolution_summary.ambiguous_count
                        + resolution_summary.unresolved_count
                    ),
                )
            ],
        )
        append_records(
            spark,
            get_operations_table_fqn(pipeline_context, RECONCILIATION_RESULTS_TABLE),
            [
                build_reconciliation_record(
                    pipeline_context,
                    reconciliation_name="resolved_match_teams_row_balance",
                    source_count=resolution_summary.input_row_count,
                    accepted_count=resolution_summary.output_row_count,
                    excluded_count=0,
                )
            ],
        )

        print(f"Script version: {SCRIPT_VERSION}")
        print(f"Pipeline name: {pipeline_context.pipeline_name}")
        print(f"Pipeline version: {pipeline_context.pipeline_version}")
        print(f"Release name: {pipeline_context.release_name}")
        print(f"Release role: {pipeline_context.release_role}")
        print(f"Gold pipeline run ID: {pipeline_context.pipeline_run_id}")
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
        print(f"Silver schema: {environment.silver_schema}")
        print(f"Gold schema: {environment.gold_schema}")
        print(f"Gold stage schema: {environment.gold_stage_schema}")
        print(f"Operations schema: {environment.operations_schema}")
        print(f"Required Silver source table count: {len(existing_silver_tables)}")
        print("Planned Phase 4 target tables:")
        for table_name in PHASE4_TARGET_TABLES:
            print(f"  - {environment.catalog}.{environment.gold_schema}.{table_name}")
        print(f"Published stage table: {resolution_summary.stage_table_fqn}")
        print(f"Published target table: {resolution_summary.target_table_fqn}")
        print(f"match_teams input row count: {resolution_summary.input_row_count}")
        print(f"resolved_match_teams output row count: {resolution_summary.output_row_count}")
        print("Resolution counts:")
        print(f"  - direct_resolution_count: {resolution_summary.direct_resolution_count}")
        print(
            f"  - active_pair_resolution_count: "
            f"{resolution_summary.active_pair_resolution_count}"
        )
        print(
            f"  - historical_pair_resolution_count: "
            f"{resolution_summary.historical_pair_resolution_count}"
        )
        print(f"  - ambiguous_count: {resolution_summary.ambiguous_count}")
        print(f"  - unresolved_count: {resolution_summary.unresolved_count}")
        print(
            f"  - persistent_team_resolution_pct: "
            f"{resolution_summary.persistent_team_resolution_pct:.2f}"
        )

        set_task_value(dbutils, "run_id", pipeline_context.pipeline_run_id)
        set_task_value(dbutils, "pipeline_run_id", pipeline_context.pipeline_run_id)
        set_task_value(dbutils, "release_name", pipeline_context.release_name)
        set_task_value(dbutils, "config_hash", pipeline_context.configuration_hash)
        set_task_value(dbutils, "analysis_as_of_date", str(pipeline_context.analysis_as_of_date))
        set_task_value(dbutils, "upstream_silver_run_id", pipeline_context.upstream_pipeline_run_id)
        set_task_value(dbutils, "validated_silver_table_count", len(existing_silver_tables))
        set_task_value(
            dbutils,
            "phase4_resolved_row_count",
            resolution_summary.output_row_count,
        )
        set_task_value(
            dbutils,
            "phase4_direct_resolution_count",
            resolution_summary.direct_resolution_count,
        )
        set_task_value(
            dbutils,
            "phase4_active_pair_resolution_count",
            resolution_summary.active_pair_resolution_count,
        )
        set_task_value(
            dbutils,
            "phase4_historical_pair_resolution_count",
            resolution_summary.historical_pair_resolution_count,
        )
        set_task_value(
            dbutils,
            "phase4_ambiguous_count",
            resolution_summary.ambiguous_count,
        )
        set_task_value(
            dbutils,
            "phase4_unresolved_count",
            resolution_summary.unresolved_count,
        )
        set_task_value(
            dbutils,
            "phase4_persistent_team_resolution_pct",
            round(resolution_summary.persistent_team_resolution_pct, 4),
        )
        complete_pipeline_run(spark, pipeline_context, status="SUCCEEDED")
    except Exception as exc:
        if pipeline_context is not None and table_run_started_ts is not None:
            append_records(
                spark,
                get_operations_table_fqn(pipeline_context, TABLE_RUNS_TABLE),
                [
                    build_table_run_end_record(
                        pipeline_context,
                        target_gold_table=PHASE4_TARGET_TABLE,
                        build_stage="gold",
                        build_order=PHASE4_BUILD_ORDER,
                        started_ts=table_run_started_ts,
                        status="FAILED",
                        error_message=str(exc),
                    )
                ],
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
