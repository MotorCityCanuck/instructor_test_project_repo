"""Validate the Bronze-to-Silver release environment and start the pipeline run."""

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
from napa_pipeline.bronze_to_silver.environment import ensure_release_environment
from napa_pipeline.bronze_to_silver.execute import initialize_pipeline_run
from napa_pipeline.bronze_to_silver.operations import create_pipeline_context


SCRIPT_VERSION = "2026.07.16.1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the Bronze-to-Silver release environment."
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
    environment_status = ensure_release_environment(spark, config, create_missing=True)
    context = create_pipeline_context(
        config,
        environment_status.release_environment,
        pipeline_run_id=args.run_id,
    )
    initialize_pipeline_run(spark, context)

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Release name: {args.release_name}")
    print(f"Run ID: {args.run_id}")
    for schema_status in environment_status.schema_statuses:
        print(
            f"{schema_status.object_name} existed={schema_status.existed}"
        )

    set_task_value(dbutils, "run_id", args.run_id)
    set_task_value(dbutils, "validated_schema_count", len(environment_status.schema_statuses))


if __name__ == "__main__":
    main()
