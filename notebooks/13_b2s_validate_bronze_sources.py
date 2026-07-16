"""Validate configured Bronze source tables for Bronze-to-Silver."""

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
from napa_pipeline.bronze_to_silver.workflow import validate_bronze_source_tables


SCRIPT_VERSION = "2026.07.16.1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate configured Bronze source tables for Bronze-to-Silver."
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
    existing_tables, missing_tables = validate_bronze_source_tables(
        spark,
        config,
        environment,
    )

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Release name: {args.release_name}")
    print(f"Run ID: {args.run_id}")
    print(f"Existing Bronze table count: {len(existing_tables)}")
    for table_name in existing_tables:
        print(f"FOUND {table_name}")

    set_task_value(dbutils, "run_id", args.run_id)
    set_task_value(dbutils, "bronze_source_count", len(existing_tables))

    if missing_tables:
        for missing in missing_tables:
            print(f"MISSING {missing}")
        raise RuntimeError(
            "Configured Bronze source tables are missing: "
            + ", ".join(missing_tables)
        )


if __name__ == "__main__":
    main()
