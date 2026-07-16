"""Resolve Raw-to-Bronze configuration and generate the workflow run ID."""

from __future__ import annotations

import argparse

from _bootstrap_napa_pipeline import bootstrap_napa_pipeline_imports

bootstrap_napa_pipeline_imports()

from napa_pipeline.raw_to_bronze.cli import (
    add_config_path_argument,
    add_release_type_argument,
    get_databricks_global,
    normalize_config_path,
    release_type_to_release_name,
    set_task_value,
)
from napa_pipeline.raw_to_bronze.config import load_raw_to_bronze_config
from napa_pipeline.raw_to_bronze.environment import resolve_release_environment
from napa_pipeline.raw_to_bronze.operations import create_pipeline_context


SCRIPT_VERSION = "2026.07.16.1"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the configuration task."""
    parser = argparse.ArgumentParser(
        description="Resolve Raw-to-Bronze configuration for a release type."
    )
    add_release_type_argument(parser)
    add_config_path_argument(parser)
    return parser.parse_args()


def main() -> None:
    """Resolve configuration and set workflow task values."""
    args = parse_args()
    release_name = release_type_to_release_name(args.release_type)
    config = load_raw_to_bronze_config(
        release_name,
        config_root=normalize_config_path(args.config_path),
    )
    environment = resolve_release_environment(config)
    context = create_pipeline_context(config, environment)

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Pipeline name: {context.pipeline_name}")
    print(f"Pipeline version: {context.pipeline_version}")
    print(f"Release type: {args.release_type}")
    print(f"Release name: {context.release_name}")
    print(f"Config root: {config.config_root}")
    print(f"Config hash: {context.configuration_hash}")
    print(f"Run ID: {context.pipeline_run_id}")
    print(f"Catalog: {environment.catalog}")
    print(f"Raw schema: {environment.raw_schema}")
    print(f"Bronze schema: {environment.bronze_schema}")
    print(f"Operations schema: {environment.operations_schema}")
    print(f"Raw volume path: {environment.raw_volume_path}")
    print(f"Enabled source count: {len(config.sources_in_build_order)}")

    dbutils = get_databricks_global("dbutils")
    set_task_value(dbutils, "run_id", context.pipeline_run_id)
    set_task_value(dbutils, "pipeline_run_id", context.pipeline_run_id)
    set_task_value(dbutils, "release_name", context.release_name)
    set_task_value(dbutils, "config_hash", context.configuration_hash)
    set_task_value(dbutils, "catalog", environment.catalog)
    set_task_value(dbutils, "raw_schema", environment.raw_schema)
    set_task_value(dbutils, "bronze_schema", environment.bronze_schema)
    set_task_value(dbutils, "source_path", environment.raw_volume_path)


if __name__ == "__main__":
    main()
