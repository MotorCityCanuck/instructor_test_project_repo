"""Execute the Bronze-to-Silver athlete-stage tables."""

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
from napa_pipeline.bronze_to_silver.execute import execute_stage
from napa_pipeline.bronze_to_silver.operations import create_pipeline_context


SCRIPT_VERSION = "2026.07.16.1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Execute the Bronze-to-Silver athlete-stage tables."
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
    table_runs = execute_stage(
        spark,
        config,
        environment,
        context,
        stage_name="athlete",
    )

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Release name: {args.release_name}")
    print(f"Run ID: {args.run_id}")
    print("Athlete tables completed:")
    for table_run in table_runs:
        print(f"- {table_run['target_table']} status={table_run['status']}")

    set_task_value(dbutils, "run_id", args.run_id)
    set_task_value(dbutils, "athlete_table_count", len(table_runs))


if __name__ == "__main__":
    main()
