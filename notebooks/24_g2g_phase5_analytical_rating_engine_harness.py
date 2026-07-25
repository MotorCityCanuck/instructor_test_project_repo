"""Databricks harness for Silver-to-Gold Phase 5 analytical rating-engine testing."""

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
from napa_pipeline.silver_to_gold.io import (
    get_gold_target_table_fqn,
    get_operations_table_fqn,
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
    utc_now,
)
from napa_pipeline.silver_to_gold.ratings_validation import (
    PHASE5_REQUIRED_SOURCE_COLUMNS,
    publish_phase5_rating_tables,
    validate_phase5_source_contract,
)
from napa_pipeline.silver_to_gold.workflow import (
    PHASE5_TARGET_TABLES,
    collect_match_rows_for_analysis_date,
    initialize_pipeline_run,
    require_required_silver_source_tables,
    resolve_latest_successful_upstream_run_id,
)


SCRIPT_VERSION = "2026.07.25.1"
PHASE5_TARGET_CONFIG = {
    "player_rating_events": {
        "build_order": 40,
        "source_table_fqn_resolver": lambda environment: get_gold_target_table_fqn(
            environment,
            "competition_player_matches",
        ),
        "reconciliation_name": "player_rating_events_row_balance",
    },
    "player_rating_history": {
        "build_order": 50,
        "source_table_fqn_resolver": lambda environment: get_gold_target_table_fqn(
            environment,
            "player_rating_events",
        ),
        "reconciliation_name": "player_rating_history_row_balance",
    },
    "player_current_ratings": {
        "build_order": 60,
        "source_table_fqn_resolver": lambda environment: f"{environment.catalog}.{environment.silver_schema}.players",
        "reconciliation_name": "player_current_ratings_row_balance",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Silver-to-Gold Phase 5 analytical ratings in Databricks."
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
        validated_columns = validate_phase5_source_contract(spark, environment)

        for table_name in PHASE5_TARGET_TABLES:
            started_ts = utc_now()
            table_run_started_ts_by_target[table_name] = started_ts
            append_records(
                spark,
                get_operations_table_fqn(pipeline_context, TABLE_RUNS_TABLE),
                [
                    build_table_run_start_record(
                        pipeline_context,
                        target_gold_table=table_name,
                        build_stage="ratings",
                        build_order=PHASE5_TARGET_CONFIG[table_name]["build_order"],
                        started_ts=started_ts,
                    )
                ],
            )

        publication_summary = publish_phase5_rating_tables(
            spark,
            environment,
            analysis_as_of_date=pipeline_context.analysis_as_of_date,
            ratings_config=config.data["ratings"],
        )

        source_counts = {
            table_name: int(
                spark.table(PHASE5_TARGET_CONFIG[table_name]["source_table_fqn_resolver"](environment)).count()
            )
            for table_name in PHASE5_TARGET_TABLES
        }
        output_counts = {
            "player_rating_events": publication_summary.player_rating_events.output_row_count,
            "player_rating_history": publication_summary.player_rating_history.output_row_count,
            "player_current_ratings": publication_summary.player_current_ratings.output_row_count,
        }
        excluded_counts = {
            "player_rating_events": source_counts["player_rating_events"]
            - output_counts["player_rating_events"],
            "player_rating_history": 0,
            "player_current_ratings": 0,
        }
        if excluded_counts["player_rating_events"] < 0:
            raise ValueError(
                "player_rating_events output exceeded competition_player_matches rows: "
                f"output_rows={output_counts['player_rating_events']}, "
                f"source_rows={source_counts['player_rating_events']}."
            )

        append_records(
            spark,
            get_operations_table_fqn(pipeline_context, TABLE_RUNS_TABLE),
            [
                build_table_run_end_record(
                    pipeline_context,
                    target_gold_table=table_name,
                    build_stage="ratings",
                    build_order=PHASE5_TARGET_CONFIG[table_name]["build_order"],
                    started_ts=table_run_started_ts_by_target[table_name],
                    status="SUCCEEDED",
                    input_row_count=source_counts[table_name],
                    output_row_count=output_counts[table_name],
                    excluded_row_count=excluded_counts[table_name],
                )
                for table_name in PHASE5_TARGET_TABLES
            ],
        )
        append_records(
            spark,
            get_operations_table_fqn(pipeline_context, RECONCILIATION_RESULTS_TABLE),
            [
                build_reconciliation_record(
                    pipeline_context,
                    reconciliation_name=PHASE5_TARGET_CONFIG[table_name]["reconciliation_name"],
                    source_count=source_counts[table_name],
                    accepted_count=output_counts[table_name],
                    excluded_count=excluded_counts[table_name],
                )
                for table_name in PHASE5_TARGET_TABLES
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
        print("Validated Phase 5 source columns:")
        for logical_name, columns in validated_columns.items():
            layer, table_name, _required_columns = PHASE5_REQUIRED_SOURCE_COLUMNS[logical_name]
            schema_name = environment.gold_schema if layer == "gold" else environment.silver_schema
            print(f"  - {environment.catalog}.{schema_name}.{table_name}")
            for column_name in columns:
                print(f"      * {column_name}")
        print("Planned Phase 5 target tables:")
        for table_name in PHASE5_TARGET_TABLES:
            print(f"  - {environment.catalog}.{environment.gold_schema}.{table_name}")
        print("Published Phase 5 target tables:")
        print(
            f"  - player_rating_events: {publication_summary.player_rating_events.target_table_fqn} "
            f"(input={source_counts['player_rating_events']}, "
            f"output={output_counts['player_rating_events']}, "
            f"excluded={excluded_counts['player_rating_events']})"
        )
        print(
            f"  - player_rating_history: {publication_summary.player_rating_history.target_table_fqn} "
            f"(input={source_counts['player_rating_history']}, "
            f"output={output_counts['player_rating_history']}, "
            f"excluded={excluded_counts['player_rating_history']})"
        )
        print(
            f"  - player_current_ratings: {publication_summary.player_current_ratings.target_table_fqn} "
            f"(input={source_counts['player_current_ratings']}, "
            f"output={output_counts['player_current_ratings']}, "
            f"excluded={excluded_counts['player_current_ratings']})"
        )

        set_task_value(dbutils, "run_id", pipeline_context.pipeline_run_id)
        set_task_value(dbutils, "pipeline_run_id", pipeline_context.pipeline_run_id)
        set_task_value(dbutils, "release_name", pipeline_context.release_name)
        set_task_value(dbutils, "config_hash", pipeline_context.configuration_hash)
        set_task_value(dbutils, "analysis_as_of_date", str(pipeline_context.analysis_as_of_date))
        set_task_value(dbutils, "upstream_silver_run_id", pipeline_context.upstream_pipeline_run_id)
        set_task_value(dbutils, "validated_silver_table_count", len(existing_silver_tables))
        set_task_value(dbutils, "phase5_player_rating_events_row_count", output_counts["player_rating_events"])
        set_task_value(dbutils, "phase5_player_rating_history_row_count", output_counts["player_rating_history"])
        set_task_value(dbutils, "phase5_player_current_ratings_row_count", output_counts["player_current_ratings"])
        complete_pipeline_run(spark, pipeline_context, status="SUCCEEDED")
    except Exception as exc:
        if pipeline_context is not None:
            failed_records = []
            for table_name in PHASE5_TARGET_TABLES:
                started_ts = table_run_started_ts_by_target.get(table_name)
                if started_ts is None:
                    continue
                failed_records.append(
                    build_table_run_end_record(
                        pipeline_context,
                        target_gold_table=table_name,
                        build_stage="ratings",
                        build_order=PHASE5_TARGET_CONFIG[table_name]["build_order"],
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
