"""Resolve Bronze-to-Silver configuration and generate the workflow run ID."""

from __future__ import annotations

import argparse

from _bootstrap_napa_pipeline import bootstrap_napa_pipeline_imports

bootstrap_napa_pipeline_imports()

from napa_pipeline.bronze_to_silver.cli import (
    add_config_path_argument,
    add_release_name_argument,
    get_databricks_global,
    normalize_config_path,
    set_task_value,
)
from napa_pipeline.bronze_to_silver.config import load_bronze_to_silver_config
from napa_pipeline.bronze_to_silver.environment import resolve_release_environment
from napa_pipeline.bronze_to_silver.operations import create_pipeline_context


SCRIPT_VERSION = "2026.07.16.1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Resolve Bronze-to-Silver configuration for a release."
    )
    add_release_name_argument(parser)
    add_config_path_argument(parser)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_bronze_to_silver_config(
        args.release_name,
        config_root=normalize_config_path(args.config_path),
    )
    environment = resolve_release_environment(config)
    context = create_pipeline_context(config, environment)

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Pipeline name: {context.pipeline_name}")
    print(f"Pipeline version: {context.pipeline_version}")
    print(f"Release name: {context.release_name}")
    print(f"Config root: {config.config_root}")
    print(f"Config hash: {context.configuration_hash}")
    print(f"Run ID: {context.pipeline_run_id}")
    print(f"Catalog: {environment.catalog}")
    print(f"Bronze schema: {environment.bronze_schema}")
    print(f"Silver schema: {environment.silver_schema}")
    print(f"Silver reject schema: {environment.silver_reject_schema}")
    print(f"Operations schema: {environment.operations_schema}")
    print(f"Configured source count: {len(config.enabled_sources)}")
    print(f"Configured Silver table count: {len(config.silver_tables_in_build_order)}")

    dbutils = get_databricks_global("dbutils")
    set_task_value(dbutils, "run_id", context.pipeline_run_id)
    set_task_value(dbutils, "pipeline_run_id", context.pipeline_run_id)
    set_task_value(dbutils, "release_name", context.release_name)
    set_task_value(dbutils, "config_hash", context.configuration_hash)
    set_task_value(dbutils, "catalog", environment.catalog)
    set_task_value(dbutils, "bronze_schema", environment.bronze_schema)
    set_task_value(dbutils, "silver_schema", environment.silver_schema)
    set_task_value(dbutils, "silver_reject_schema", environment.silver_reject_schema)


if __name__ == "__main__":
    main()
