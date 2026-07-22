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
from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import (
    build_runtime_context,
    ensure_release_environment,
)
from napa_pipeline.silver_to_gold.operations import create_pipeline_context
from napa_pipeline.silver_to_gold.workflow import (
    PHASE3_TARGET_TABLES,
    collect_match_rows_for_analysis_date,
    initialize_pipeline_run,
    require_required_silver_source_tables,
    resolve_latest_successful_upstream_run_id,
)


SCRIPT_VERSION = "2026.07.22.1"


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
    print("Planned Phase 3 target tables:")
    for table_name in PHASE3_TARGET_TABLES:
        print(f"  - {environment.catalog}.{environment.gold_schema}.{table_name}")

    set_task_value(dbutils, "run_id", pipeline_context.pipeline_run_id)
    set_task_value(dbutils, "pipeline_run_id", pipeline_context.pipeline_run_id)
    set_task_value(dbutils, "release_name", pipeline_context.release_name)
    set_task_value(dbutils, "config_hash", pipeline_context.configuration_hash)
    set_task_value(dbutils, "analysis_as_of_date", str(pipeline_context.analysis_as_of_date))
    set_task_value(dbutils, "upstream_silver_run_id", pipeline_context.upstream_pipeline_run_id)
    set_task_value(dbutils, "validated_silver_table_count", len(existing_silver_tables))


if __name__ == "__main__":
    main()
