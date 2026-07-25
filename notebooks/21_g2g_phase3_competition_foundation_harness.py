"""Databricks harness for Silver-to-Gold Phase 3 competition-foundation testing."""

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
from napa_pipeline.silver_to_gold.competition_validation import (
    PHASE3_REQUIRED_SOURCE_COLUMNS,
    publish_phase3_competition_foundation,
    validate_phase3_source_contract,
)
from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import (
    build_runtime_context,
    ensure_release_environment,
)
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
    PHASE3_TARGET_TABLES,
    collect_match_rows_for_analysis_date,
    initialize_pipeline_run,
    require_required_silver_source_tables,
    resolve_latest_successful_upstream_run_id,
)


SCRIPT_VERSION = "2026.07.22.1"
PHASE3_TARGET_CONFIG = {
    "competition_match_sides": {
        "build_order": 10,
        "source_table": "match_teams",
        "reconciliation_name": "competition_match_sides_row_balance",
    },
    "competition_player_matches": {
        "build_order": 20,
        "source_table": "match_team_players",
        "reconciliation_name": "competition_player_matches_row_balance",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Silver-to-Gold Phase 3 prerequisites in Databricks."
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
        validated_columns = validate_phase3_source_contract(spark, environment)

        for table_name in PHASE3_TARGET_TABLES:
            started_ts = utc_now()
            table_run_started_ts_by_target[table_name] = started_ts
            append_records(
                spark,
                get_operations_table_fqn(pipeline_context, TABLE_RUNS_TABLE),
                [
                    build_table_run_start_record(
                        pipeline_context,
                        target_gold_table=table_name,
                        build_stage="foundation",
                        build_order=PHASE3_TARGET_CONFIG[table_name]["build_order"],
                        started_ts=started_ts,
                    )
                ],
            )

        publication_summary = publish_phase3_competition_foundation(
            spark,
            environment,
            analysis_as_of_date=pipeline_context.analysis_as_of_date,
        )

        match_sides_excluded_count = (
            publication_summary.competition_match_sides.input_row_count
            - publication_summary.competition_match_sides.output_row_count
        )
        if match_sides_excluded_count < 0:
            raise ValueError(
                "competition_match_sides output exceeded source match_teams rows: "
                f"output_rows={publication_summary.competition_match_sides.output_row_count}, "
                f"source_rows={publication_summary.competition_match_sides.input_row_count}."
            )

        player_matches_excluded_count = (
            publication_summary.competition_player_matches.input_row_count * 2
            - publication_summary.competition_player_matches.output_row_count
        )
        if player_matches_excluded_count < 0:
            raise ValueError(
                "competition_player_matches output exceeded expected two-player expansion "
                "from competition_match_sides: "
                f"output_rows={publication_summary.competition_player_matches.output_row_count}, "
                f"expected_max_rows={publication_summary.competition_player_matches.input_row_count * 2}."
            )

        append_records(
            spark,
            get_operations_table_fqn(pipeline_context, TABLE_RUNS_TABLE),
            [
                build_table_run_end_record(
                    pipeline_context,
                    target_gold_table="competition_match_sides",
                    build_stage="foundation",
                    build_order=PHASE3_TARGET_CONFIG["competition_match_sides"]["build_order"],
                    started_ts=table_run_started_ts_by_target["competition_match_sides"],
                    status="SUCCEEDED",
                    input_row_count=publication_summary.competition_match_sides.input_row_count,
                    output_row_count=publication_summary.competition_match_sides.output_row_count,
                    excluded_row_count=match_sides_excluded_count,
                ),
                build_table_run_end_record(
                    pipeline_context,
                    target_gold_table="competition_player_matches",
                    build_stage="foundation",
                    build_order=PHASE3_TARGET_CONFIG["competition_player_matches"]["build_order"],
                    started_ts=table_run_started_ts_by_target["competition_player_matches"],
                    status="SUCCEEDED",
                    input_row_count=publication_summary.competition_player_matches.input_row_count * 2,
                    output_row_count=publication_summary.competition_player_matches.output_row_count,
                    excluded_row_count=player_matches_excluded_count,
                ),
            ],
        )
        append_records(
            spark,
            get_operations_table_fqn(pipeline_context, RECONCILIATION_RESULTS_TABLE),
            [
                build_reconciliation_record(
                    pipeline_context,
                    reconciliation_name=PHASE3_TARGET_CONFIG["competition_match_sides"]["reconciliation_name"],
                    source_count=publication_summary.competition_match_sides.input_row_count,
                    accepted_count=publication_summary.competition_match_sides.output_row_count,
                    excluded_count=match_sides_excluded_count,
                ),
                build_reconciliation_record(
                    pipeline_context,
                    reconciliation_name=PHASE3_TARGET_CONFIG["competition_player_matches"]["reconciliation_name"],
                    source_count=publication_summary.competition_player_matches.input_row_count * 2,
                    accepted_count=publication_summary.competition_player_matches.output_row_count,
                    excluded_count=player_matches_excluded_count,
                ),
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
        print("Validated Silver source tables:")
        for table_fqn in existing_silver_tables:
            print(f"  - {table_fqn}")
        print("Validated Phase 3 source columns:")
        for table_name, columns in validated_columns.items():
            print(f"  - {environment.catalog}.{environment.silver_schema}.{table_name}")
            for column_name in columns:
                print(f"      * {column_name}")
        print("Planned Phase 3 target tables:")
        for table_name in PHASE3_TARGET_TABLES:
            print(f"  - {environment.catalog}.{environment.gold_schema}.{table_name}")
        print("Published Phase 3 target tables:")
        print(
            f"  - competition_match_sides: "
            f"{publication_summary.competition_match_sides.target_table_fqn} "
            f"(input={publication_summary.competition_match_sides.input_row_count}, "
            f"output={publication_summary.competition_match_sides.output_row_count}, "
            f"excluded={match_sides_excluded_count})"
        )
        print(
            f"  - competition_player_matches: "
            f"{publication_summary.competition_player_matches.target_table_fqn} "
            f"(input_max={publication_summary.competition_player_matches.input_row_count * 2}, "
            f"output={publication_summary.competition_player_matches.output_row_count}, "
            f"excluded={player_matches_excluded_count})"
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
            "phase3_match_sides_output_row_count",
            publication_summary.competition_match_sides.output_row_count,
        )
        set_task_value(
            dbutils,
            "phase3_match_sides_excluded_row_count",
            match_sides_excluded_count,
        )
        set_task_value(
            dbutils,
            "phase3_player_matches_output_row_count",
            publication_summary.competition_player_matches.output_row_count,
        )
        set_task_value(
            dbutils,
            "phase3_player_matches_excluded_row_count",
            player_matches_excluded_count,
        )
        complete_pipeline_run(spark, pipeline_context, status="SUCCEEDED")
    except Exception as exc:
        if pipeline_context is not None:
            failed_records = []
            for table_name in PHASE3_TARGET_TABLES:
                started_ts = table_run_started_ts_by_target.get(table_name)
                if started_ts is None:
                    continue
                failed_records.append(
                    build_table_run_end_record(
                        pipeline_context,
                        target_gold_table=table_name,
                        build_stage="foundation",
                        build_order=PHASE3_TARGET_CONFIG[table_name]["build_order"],
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
